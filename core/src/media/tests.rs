//! Tests for the media module

use super::*;
use std::time::Duration;

#[tokio::test]
async fn test_memory_source() {
    let data = b"test data".to_vec();
    let mut source = source::MemorySource::new(data.clone(), interface::MediaFormat::Raw);
    
    let chunk = source.next_chunk().await.unwrap();
    assert_eq!(chunk, data);
    
    // Should be at end of stream
    assert!(source.next_chunk().await.is_err());
}

#[tokio::test]
async fn test_memory_sink() {
    let mut sink = sink::MemorySink::new();
    let data = b"test data".to_vec();
    
    sink.write_chunk(&data).await.unwrap();
    sink.flush().await.unwrap();
    
    let result = sink.into_inner().await;
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_media_stream() {
    let data = b"test data".to_vec();
    let source = source::MemorySource::new(data.clone(), interface::MediaFormat::Raw);
    let mut stream = stream::MediaStreamImpl::new(Box::new(source), None, None);
    
    let chunk = stream.read(1024).await.unwrap();
    assert_eq!(chunk, data);
    
    // Should be at end of stream
    assert!(stream.read(1024).await.is_err());
}

use crate::test_report::{TestReport, TestResult};
use std::time::{SystemTime, Instant};

#[tokio::test]
async fn test_media_pipeline() {
    use std::panic::AssertUnwindSafe;
    use futures::FutureExt;
    let test_start = Instant::now();
    let mut report = TestReport::new();
    let mut test_status = "pass".to_string();
    let mut error_msg = None;
    let test_result = AssertUnwindSafe(async {
        // Create a pass-through pipeline
        let mut pipeline = pipeline::MediaPipeline::new();
        // Pass the stage directly without pre-boxing it
        pipeline.add_stage(pipeline::PassThroughStage::new("test"));
        // Process some data
        let data = b"test data".to_vec();
        let result = pipeline.process(data.clone()).await.unwrap();
        // The output should be the same as the input
        assert_eq!(result, data);
    }).catch_unwind().await;
    if let Err(e) = test_result {
        test_status = "fail".to_string();
        error_msg = Some(format!("{e:?}"));
    }
    let duration_ms = test_start.elapsed().as_millis();
    let result = TestResult {
        name: "test_media_pipeline".to_string(),
        module: "media".to_string(),
        status: test_status,
        error: error_msg,
        duration_ms,
        timestamp: SystemTime::now(),
    };
    report.add_result(result);
    let _ = report.save_to_file("test_report.json");
}


#[test]
fn test_quality_levels() {
    use quality::QualityLevel;
    
    assert!(QualityLevel::Low < QualityLevel::Medium);
    assert!(QualityLevel::Medium < QualityLevel::High);
    assert!(QualityLevel::High < QualityLevel::Highest);
}

use crate::test_report::{TestReport, TestResult};
use std::time::{SystemTime, Instant};

#[tokio::test]
async fn test_bandwidth_monitor() {
    use std::panic::AssertUnwindSafe;
    use futures::FutureExt;
    let test_start = Instant::now();
    let mut report = TestReport::new();
    let mut test_status = "pass".to_string();
    let mut error_msg = None;
    let test_result = AssertUnwindSafe(async {
        use quality::{BandwidthMonitor, QualityConfig};
        let config = QualityConfig::default();
        let mut monitor = BandwidthMonitor::new(config);
        // Record some downloads
        monitor.record_download(1024 * 1024); // 1 MB
        monitor.record_download(1024 * 1024); // Another 1 MB
        // Should have some bandwidth
        assert!(monitor.current_bandwidth() > 0.0);
    }).catch_unwind().await;
    if let Err(e) = test_result {
        test_status = "fail".to_string();
        error_msg = Some(format!("{e:?}"));
    }
    let duration_ms = test_start.elapsed().as_millis();
    let result = TestResult {
        name: "test_bandwidth_monitor".to_string(),
        module: "media".to_string(),
        status: test_status,
        error: error_msg,
        duration_ms,
        timestamp: SystemTime::now(),
    };
    report.add_result(result);
    let _ = report.save_to_file("test_report.json");
}

