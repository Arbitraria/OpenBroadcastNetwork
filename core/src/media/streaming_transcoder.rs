use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tempfile::Builder;
use tracing::{info, debug, error, warn};

use crate::media::mp4_parser::{Mp4Parser, MseSegment};
use super::transcoder::TranscoderError;

/// Result type for streaming transcoder operations
pub type StreamingResult<T> = Result<T, TranscoderError>;

/// A segment that has been transcoded and is ready for streaming
#[derive(Debug, Clone)]
pub struct TranscodedSegment {
    pub segment_type: String,
    pub data: Vec<u8>,
    pub timestamp: Option<i64>,
    pub duration: Option<u32>,
    pub is_keyframe: bool,
}

/// Handles progressive transcoding of AC-3 to AAC while streaming
pub struct StreamingTranscoder {
    /// Channel for sending transcoded segments
    segment_tx: mpsc::Sender<TranscodedSegment>,
    /// Handle to the transcoding task
    transcode_handle: Option<JoinHandle<StreamingResult<()>>>,
    /// Temporary file path for transcoded output
    temp_path: Option<PathBuf>,
    /// Shared MP4 parser instance wrapped for thread safety
    mp4_parser: Arc<RwLock<Mp4Parser>>,
}

impl StreamingTranscoder {
    /// Create a new streaming transcoder
    pub fn new() -> (Self, mpsc::Receiver<TranscodedSegment>) {
        let (segment_tx, segment_rx) = mpsc::channel(100); // Buffer up to 100 segments
        
        (
            StreamingTranscoder {
                segment_tx,
                transcode_handle: None,
                temp_path: None,
                mp4_parser: Arc::new(RwLock::new(Mp4Parser::new())),
            },
            segment_rx
        )
    }
    
    /// Start transcoding a file progressively
    pub async fn start_transcoding<P: AsRef<Path>>(&mut self, input_path: P) -> StreamingResult<()> {
        let input_path = input_path.as_ref().to_path_buf();
        let segment_tx = self.segment_tx.clone();
        
        // Create temp file for transcoded output
        let temp_file = Builder::new()
            .suffix(".mp4")
            .tempfile()
            .map_err(TranscoderError::Io)?;
        
        let temp_path = temp_file.into_temp_path().keep()
            .map_err(|e| TranscoderError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
        
        self.temp_path = Some(temp_path.to_path_buf());
        let temp_path_clone = temp_path.to_path_buf();
        
        info!("Starting progressive transcoding: {} -> {:?}", input_path.display(), temp_path);
        
        let mp4_parser = Arc::clone(&self.mp4_parser);
        
        // Spawn transcoding task
        let handle = tokio::spawn(async move {
            Self::transcode_and_stream(input_path, temp_path_clone, segment_tx, mp4_parser).await
        });
        
        self.transcode_handle = Some(handle);
        Ok(())
    }
    
    /// Internal method to handle transcoding and progressive parsing
    async fn transcode_and_stream(
        input_path: PathBuf,
        output_path: PathBuf,
        segment_tx: mpsc::Sender<TranscodedSegment>,
        mp4_parser: Arc<RwLock<Mp4Parser>>,
    ) -> StreamingResult<()> {
        // Use FFmpeg with progressive writing to single output file
        // This approach writes fragments progressively to the output file
        let mut ffmpeg = Command::new("ffmpeg")
            .args(&[
                "-i", input_path.to_str().unwrap(),
                "-c:v", "copy",              // Copy video as-is
                "-c:a", "aac",               // Convert audio to AAC
                "-b:a", "128k",              // Audio bitrate
                // Progressive fMP4 output
                "-movflags", "frag_keyframe+empty_moov+faststart+dash",
                "-frag_duration", "2000000", // 2 second fragments (in microseconds)
                "-min_frag_duration", "2000000",
                "-y",                        // Overwrite output
                output_path.to_str().unwrap()
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(TranscoderError::Io)?;
        
        info!("FFmpeg process started for progressive transcoding");
        
        // Clone variables for the monitor task
        let monitor_output_path = output_path.clone();
        let monitor_segment_tx = segment_tx.clone();
        let monitor_mp4_parser = mp4_parser.clone();
        
        // Monitor the output file for progressive fragments
        let monitor_handle = tokio::spawn(async move {
            info!("Monitor task starting for file: {:?}", monitor_output_path);
            match Self::monitor_progressive_output(monitor_output_path, monitor_segment_tx, monitor_mp4_parser).await {
                Ok(()) => info!("Monitor task completed successfully"),
                Err(e) => error!("Monitor task failed: {:?}", e),
            }
        });
        
        // Wait for FFmpeg to complete (but don't block the monitor)
        let status = ffmpeg.wait().map_err(TranscoderError::Io)?;
        
        if !status.success() {
            error!("FFmpeg transcoding failed with status: {:?}", status);
            monitor_handle.abort();
            return Err(TranscoderError::ConversionFailed("FFmpeg failed".to_string()));
        }
        
        info!("FFmpeg transcoding completed successfully");
        
        // Wait a bit more for monitor to finish processing
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        
        // Now we can finish monitor
        if let Err(e) = monitor_handle.await {
            warn!("Monitor task ended with error: {:?}", e);
        }
        
        Ok(())
    }
    
    /// Monitor progressive output file and parse it into proper MP4 segments
    async fn monitor_progressive_output(
        output_path: PathBuf,
        segment_tx: mpsc::Sender<TranscodedSegment>,
        mp4_parser: Arc<RwLock<Mp4Parser>>,
    ) -> StreamingResult<()> {
        use tokio::time::{sleep, Duration};
        
        // Wait for file to be created
        for _ in 0..50 { // Wait up to 5 seconds
            if output_path.exists() {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        
        if !output_path.exists() {
            return Err(TranscoderError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Output file not created by FFmpeg"
            )));
        }
        
        info!("Monitoring progressive transcoding output: {:?}", output_path);
        
        // Track the last processed file size to detect new data
        let mut last_size = 0u64;
        let mut segments_sent = 0;
        let mut init_segment_sent = false;
        
        // Poll the file for changes
        for attempt in 0..120 { // Monitor for up to 60 seconds
            debug!("Monitor attempt {}: checking file", attempt);
            // Get current file size
            let metadata = match tokio::fs::metadata(&output_path).await {
                Ok(m) => m,
                Err(e) => {
                    warn!("Failed to get file metadata: {}", e);
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };
            
            let current_size = metadata.len();
            debug!("File size: {} bytes (last: {})", current_size, last_size);
            
            // Check if file has grown
            if current_size > last_size && current_size > 10000 { // At least 10KB
                info!("File grown from {} to {} bytes", last_size, current_size);
                
                // Read the entire file (we'll parse it properly)
                let file_data = match tokio::fs::read(&output_path).await {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("Failed to read file: {}", e);
                        sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                };
                
                // Parse the file with Mp4Parser
                let segments = {
                    let mut parser = mp4_parser.write().await;
                    
                    // Parse the MP4 data
                    if let Err(e) = parser.parse(&file_data) {
                        warn!("Failed to parse MP4 data: {}", e);
                        sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    
                    // Generate MSE segments
                    match parser.generate_mse_segments() {
                        Ok(segs) => segs,
                        Err(e) => {
                            warn!("Failed to generate MSE segments: {}", e);
                            sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    }
                };
                
                info!("Generated {} MSE segments from transcoded file", segments.len());
                
                // Send segments we haven't sent yet
                for (idx, segment) in segments.iter().enumerate() {
                    // Skip segments we've already sent
                    if idx < segments_sent {
                        continue;
                    }
                    
                    // Special handling for initialization segment
                    if segment.segment_type == "initialization" && !init_segment_sent {
                        info!("Sending initialization segment ({} bytes)", segment.data.len());
                        
                        let transcoded = TranscodedSegment {
                            segment_type: segment.segment_type.clone(),
                            data: segment.data.clone(),
                            timestamp: segment.timestamp.map(|t| t as i64),
                            duration: segment.duration.map(|d| d as u32),
                            is_keyframe: segment.is_keyframe,
                        };
                        
                        if segment_tx.send(transcoded).await.is_err() {
                            error!("Receiver dropped while sending initialization segment");
                            return Ok(());
                        }
                        
                        init_segment_sent = true;
                        segments_sent = idx + 1;
                    } else if segment.segment_type != "initialization" {
                        // Send media segment
                        debug!("Sending media segment {} ({} bytes, timestamp: {:?})", 
                               idx, segment.data.len(), segment.timestamp);
                        
                        let transcoded = TranscodedSegment {
                            segment_type: segment.segment_type.clone(),
                            data: segment.data.clone(),
                            timestamp: segment.timestamp.map(|t| t as i64),
                            duration: segment.duration.map(|d| d as u32),
                            is_keyframe: segment.is_keyframe,
                        };
                        
                        if segment_tx.send(transcoded).await.is_err() {
                            error!("Receiver dropped while sending media segment");
                            return Ok(());
                        }
                        
                        segments_sent = idx + 1;
                        
                        // Small delay between segments to not overwhelm receiver
                        sleep(Duration::from_millis(50)).await;
                    }
                }
                
                last_size = current_size;
            }
            
            // Check if FFmpeg process might still be running
            // If file hasn't changed in a while, assume it's done
            if attempt > 10 && current_size == last_size {
                info!("File size stable at {} bytes, assuming transcoding complete", current_size);
                break;
            }
            
            sleep(Duration::from_millis(500)).await;
        }
        
        info!("Progressive transcoding completed - sent {} segments", segments_sent);
        Ok(())
    }
    
    /// Stop transcoding and clean up
    pub async fn stop(&mut self) -> StreamingResult<()> {
        if let Some(handle) = self.transcode_handle.take() {
            handle.abort();
        }
        
        // Clean up temp file
        if let Some(temp_path) = self.temp_path.take() {
            if temp_path.exists() {
                tokio::fs::remove_file(&temp_path).await
                    .map_err(TranscoderError::Io)?;
            }
        }
        
        Ok(())
    }
}

impl Drop for StreamingTranscoder {
    fn drop(&mut self) {
        // Clean up temp file if still exists
        if let Some(temp_path) = self.temp_path.take() {
            if temp_path.exists() {
                let _ = std::fs::remove_file(&temp_path);
            }
        }
    }
}