//! Connection health monitoring for overlay networks
//!
//! This module provides utilities for tracking and analyzing peer connection health.

use std::collections::VecDeque;
use std::time::Instant;

/// Connection health metrics for a peer
#[derive(Debug, Clone)]
pub struct ConnectionHealth {
    /// Overall connection quality (0.0-1.0)
    pub quality: f32,
    /// Success rate of recent connections (0.0-1.0)
    pub success_rate: f32,
    /// Timestamp of last successful connection
    pub last_success: Instant,
    /// Count of consecutive connection failures
    pub consecutive_failures: usize,
    /// History of recent connection quality measurements
    pub history: VecDeque<(Instant, f32)>,
    /// Last time this health record was checked
    pub last_checked: Instant,
    /// Whether the connection is currently degraded
    pub is_degraded: bool,
}

impl Default for ConnectionHealth {
    fn default() -> Self {
        Self {
            quality: 1.0, // Start with perfect quality
            success_rate: 1.0,
            last_success: Instant::now(),
            consecutive_failures: 0,
            history: VecDeque::with_capacity(10),
            last_checked: Instant::now(),
            is_degraded: false,
        }
    }
}

impl ConnectionHealth {
    /// Create a new connection health tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful connection
    pub fn record_success(&mut self) {
        self.last_success = Instant::now();
        self.consecutive_failures = 0;
        self.success_rate = (self.success_rate * 0.9) + 0.1; // Weighted average

        // Add to history
        self.add_to_history(1.0);

        // Recalculate quality
        self.recalculate_quality();
    }

    /// Record a failed connection
    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.success_rate *= 0.9; // Weighted average, no success contribution

        // Add to history
        self.add_to_history(0.0);

        // Recalculate quality
        self.recalculate_quality();
    }

    /// Add a quality measurement to history
    fn add_to_history(&mut self, quality: f32) {
        self.history.push_back((Instant::now(), quality));
        if self.history.len() > 10 {
            self.history.pop_front();
        }
    }

    /// Recalculate the overall quality based on history
    fn recalculate_quality(&mut self) {
        if self.history.is_empty() {
            self.quality = self.success_rate;
            return;
        }

        self.quality = self.history.iter().map(|(_, q)| q).sum::<f32>() / self.history.len() as f32;
    }

    /// Update the degraded status based on thresholds
    pub fn update_degraded_status(
        &mut self,
        quality_threshold: f32,
        max_failures: usize,
        min_success_rate: f32,
    ) {
        self.last_checked = Instant::now();

        self.is_degraded = self.consecutive_failures > max_failures
            || self.quality < quality_threshold
            || self.success_rate < min_success_rate;
    }

    /// Check if this health record is expired
    pub fn is_expired(&self, max_age: std::time::Duration) -> bool {
        Instant::now().duration_since(self.last_checked) > max_age
    }
}
