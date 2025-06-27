/**
 * Main entry point for OpenBroadcastNetwork TypeScript viewer
 */

import { MediaManager } from './core/MediaManager';

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  console.log('OpenBroadcastNetwork Viewer initializing...');
  
  // Get video element
  const videoElement = document.getElementById('video') as HTMLVideoElement;
  if (!videoElement) {
    console.error('Video element not found');
    return;
  }

  // Initialize MediaManager
  const mediaManager = new MediaManager();
  
  // Basic initialization for testing
  mediaManager.initialize()
    .then(url => {
      console.log('MediaSource initialized:', url);
      videoElement.src = url;
    })
    .catch(error => {
      console.error('Failed to initialize MediaSource:', error);
    });

  // Debug info
  const debugToggle = document.getElementById('debug-toggle');
  const debugPanel = document.getElementById('debug-panel');
  
  debugToggle?.addEventListener('click', () => {
    debugPanel?.classList.toggle('visible');
  });

  // Status updates
  const statusElement = document.getElementById('status');
  const statusText = document.getElementById('status-text');
  
  function updateStatus(state: 'connecting' | 'connected' | 'error', message: string): void {
    if (statusElement) {
      statusElement.className = state;
    }
    if (statusText) {
      statusText.textContent = message;
    }
  }

  updateStatus('connecting', 'Initializing...');
  
  // Placeholder for WebSocket connection
  console.log('Ready to connect to WebSocket...');
});