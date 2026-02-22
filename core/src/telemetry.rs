//! Telemetry and metrics for the decentralized streaming system
//!
//! This module provides comprehensive telemetry and metrics collection for monitoring
//! the health, performance, and behavior of the decentralized streaming network. The
//! telemetry system collects, aggregates, and reports various metrics from all components
//! of the system to enable observability and debugging.
//!
//! The telemetry system supports several types of metrics:
//! - **Counters**: For incrementing values like packets sent, errors, or events
//! - **Gauges**: For values that can go up and down, like memory usage or connection count
//! - **Histograms**: For distributions of values, like latency or request duration
//!
//! Metrics can be exported to various backends for visualization and alerting, and
//! the collection process is designed to have minimal overhead on the main application.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Configuration for the telemetry and metrics system
///
/// Controls how metrics are collected, stored, and reported by the telemetry system.
/// These settings affect the granularity, frequency, and storage requirements of
/// the metrics collection process.
///
/// The configuration can be adjusted based on the deployment environment, with
/// development environments typically using more verbose telemetry and production
/// environments being more selective to reduce overhead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Enable telemetry collection
    pub enabled: bool,

    /// Reporting interval in seconds
    pub reporting_interval_secs: u64,

    /// Maximum history to keep (in entries)
    pub history_size: usize,
}

/// A counter metric for tracking incrementing values
///
/// Counters are used to track values that only increase over time, such as
/// the number of packets sent, connections established, errors encountered,
/// or bytes transferred. They provide insights into the cumulative activity
/// of the system.
///
/// Counters can be incremented by one or by arbitrary amounts, but cannot
/// be decremented. They are typically used for rate calculations over time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Counter {
    /// Current value
    value: u64,
}

impl Counter {
    /// Create a new counter
    pub fn new() -> Self {
        Self { value: 0 }
    }

    /// Increment the counter by one
    pub fn increment(&mut self) {
        self.value += 1;
    }

    /// Increment the counter by the given value
    pub fn increment_by(&mut self, value: u64) {
        self.value += value;
    }

    /// Get the current value
    pub fn value(&self) -> u64 {
        self.value
    }
}

/// A gauge metric for tracking values that can increase or decrease
///
/// Gauges represent a single numerical value that can arbitrarily go up and down
/// over time. They are ideal for measuring current states such as memory usage,
/// connection count, queue depth, or temperature.
///
/// Unlike counters, gauges can be both increased and decreased, and they represent
/// a snapshot of the current state rather than a cumulative value.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Gauge {
    /// Current value
    value: f64,
}

impl Gauge {
    /// Create a new gauge
    pub fn new() -> Self {
        Self { value: 0.0 }
    }

    /// Set the gauge to the given value
    pub fn set(&mut self, value: f64) {
        self.value = value;
    }

    /// Get the current value
    pub fn value(&self) -> f64 {
        self.value
    }
}

/// A histogram metric
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Histogram {
    /// Values in the histogram
    values: Vec<f64>,

    /// Sum of all values
    sum: f64,

    /// Count of values
    count: usize,
}

impl Histogram {
    /// Create a new histogram
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            sum: 0.0,
            count: 0,
        }
    }

    /// Add a value to the histogram
    pub fn observe(&mut self, value: f64) {
        self.values.push(value);
        self.sum += value;
        self.count += 1;
    }

    /// Get the average value
    pub fn average(&self) -> Option<f64> {
        if self.count > 0 {
            Some(self.sum / self.count as f64)
        } else {
            None
        }
    }
}

/// Telemetry manager for collecting and reporting metrics
///
/// The central component responsible for managing all metrics collection in the
/// decentralized streaming system. It maintains collections of different metric
/// types (counters, gauges, histograms) and handles the periodic reporting of
/// these metrics.
///
/// The TelemetryManager provides a thread-safe interface for components to
/// register and update metrics from anywhere in the application, with minimal
/// impact on performance.
pub struct TelemetryManager {
    /// Configuration
    config: TelemetryConfig,

    /// Counters
    counters: Arc<RwLock<HashMap<String, Counter>>>,

    /// Gauges
    gauges: Arc<RwLock<HashMap<String, Gauge>>>,

    /// Histograms
    histograms: Arc<RwLock<HashMap<String, Histogram>>>,
}

impl TelemetryManager {
    /// Create a new telemetry manager with the given configuration
    pub fn new(config: TelemetryConfig) -> Self {
        Self {
            config,
            counters: Arc::new(RwLock::new(HashMap::new())),
            gauges: Arc::new(RwLock::new(HashMap::new())),
            histograms: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the telemetry manager
    pub async fn start(&self) {
        if !self.config.enabled {
            return;
        }

        // Start the reporting task
        let _counters = self.counters.clone();
        let _gauges = self.gauges.clone();
        let _histograms = self.histograms.clone();
        let interval = Duration::from_secs(self.config.reporting_interval_secs);

        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);

            loop {
                timer.tick().await;

                // Report metrics here
                // In a real implementation, this would send metrics to a collection service
            }
        });
    }

    /// Get or create a counter with the given name
    pub async fn counter(&self, name: &str) -> Counter {
        let mut counters = self.counters.write().await;

        if !counters.contains_key(name) {
            counters.insert(name.to_string(), Counter::new());
        }

        counters.get(name).unwrap().clone()
    }

    /// Get or create a gauge with the given name
    pub async fn gauge(&self, name: &str) -> Gauge {
        let mut gauges = self.gauges.write().await;

        if !gauges.contains_key(name) {
            gauges.insert(name.to_string(), Gauge::new());
        }

        gauges.get(name).unwrap().clone()
    }

    /// Get or create a histogram with the given name
    pub async fn histogram(&self, name: &str) -> Histogram {
        let mut histograms = self.histograms.write().await;

        if !histograms.contains_key(name) {
            histograms.insert(name.to_string(), Histogram::new());
        }

        histograms.get(name).unwrap().clone()
    }
}
