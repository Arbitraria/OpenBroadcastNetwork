//! Metrics for the overlay network
//!
//! This module provides metrics collection and reporting for the overlay network.

use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex};
use tokio::time;

/// A simple counter metric
#[derive(Debug, Clone, Default)]
pub struct Counter {
    /// Current value
    value: u64,
}

impl Counter {
    /// Create a new counter
    pub fn new() -> Self {
        Self { value: 0 }
    }
    
    /// Increment the counter by 1
    pub fn increment(&mut self) {
        self.value += 1;
    }
    
    /// Add the specified amount to the counter
    pub fn add(&mut self, amount: u64) {
        self.value += amount;
    }
    
    /// Get the current value
    pub fn value(&self) -> u64 {
        self.value
    }
    
    /// Reset the counter to zero
    pub fn reset(&mut self) {
        self.value = 0;
    }
}

/// A gauge metric that tracks a value over time
#[derive(Debug, Clone, Default)]
pub struct Gauge {
    /// Current value
    value: f64,
}

impl Gauge {
    /// Create a new gauge
    pub fn new() -> Self {
        Self { value: 0.0 }
    }
    
    /// Set the gauge to the specified value
    pub fn set(&mut self, value: f64) {
        self.value = value;
    }
    
    /// Increment the gauge by the specified amount
    pub fn increment(&mut self, amount: f64) {
        self.value += amount;
    }
    
    /// Decrement the gauge by the specified amount
    pub fn decrement(&mut self, amount: f64) {
        self.value -= amount;
    }
    
    /// Get the current value
    pub fn value(&self) -> f64 {
        self.value
    }
}

/// A meter that tracks the rate of events over time
#[derive(Debug, Clone)]
pub struct Meter {
    /// Total count
    count: u64,
    /// Start time
    start_time: Instant,
    /// Last update time
    last_update: Instant,
    /// 1-minute rate
    one_min_rate: f64,
    /// 5-minute rate
    five_min_rate: f64,
    /// 15-minute rate
    fifteen_min_rate: f64,
    /// Time period for calculating rates (in seconds)
    tick_interval: u64,
}

impl Meter {
    /// Create a new meter
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            count: 0,
            start_time: now,
            last_update: now,
            one_min_rate: 0.0,
            five_min_rate: 0.0,
            fifteen_min_rate: 0.0,
            tick_interval: 5, // Update rates every 5 seconds
        }
    }
    
    /// Mark that an event occurred
    pub fn mark(&mut self) {
        self.mark_n(1);
    }
    
    /// Mark that n events occurred
    pub fn mark_n(&mut self, n: u64) {
        self.count += n;
        self.update_rates();
    }
    
    /// Update rates based on current time
    fn update_rates(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs();
        
        if elapsed >= self.tick_interval {
            let rate = self.count as f64 / elapsed as f64;
            
            // Update exponentially weighted moving averages
            let alpha_1 = 1.0 - (-5.0 / 60.0).exp();
            let alpha_5 = 1.0 - (-5.0 / 300.0).exp();
            let alpha_15 = 1.0 - (-5.0 / 900.0).exp();
            
            self.one_min_rate = self.one_min_rate + alpha_1 * (rate - self.one_min_rate);
            self.five_min_rate = self.five_min_rate + alpha_5 * (rate - self.five_min_rate);
            self.fifteen_min_rate = self.fifteen_min_rate + alpha_15 * (rate - self.fifteen_min_rate);
            
            self.last_update = now;
            self.count = 0;
        }
    }
    
    /// Get the mean rate since the meter was created
    pub fn mean_rate(&self) -> f64 {
        let uptime = self.start_time.elapsed().as_secs();
        if uptime == 0 {
            0.0
        } else {
            self.count as f64 / uptime as f64
        }
    }
    
    /// Get the 1-minute rate
    pub fn one_minute_rate(&self) -> f64 {
        self.one_min_rate
    }
    
    /// Get the 5-minute rate
    pub fn five_minute_rate(&self) -> f64 {
        self.five_min_rate
    }
    
    /// Get the 15-minute rate
    pub fn fifteen_minute_rate(&self) -> f64 {
        self.fifteen_min_rate
    }
}

impl Default for Meter {
    fn default() -> Self {
        Self::new()
    }
}

/// A histogram that tracks the distribution of values
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Count of samples
    count: u64,
    /// Sum of samples
    sum: f64,
    /// Minimum value
    min: f64,
    /// Maximum value
    max: f64,
    /// Mean value
    mean: f64,
    /// Variance
    variance: f64,
    /// Standard deviation
    std_dev: f64,
    /// Recent samples for calculating percentiles
    samples: Vec<f64>,
    /// Maximum number of samples to keep
    max_samples: usize,
}

impl Histogram {
    /// Create a new histogram
    pub fn new(max_samples: usize) -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            mean: 0.0,
            variance: 0.0,
            std_dev: 0.0,
            samples: Vec::with_capacity(max_samples),
            max_samples,
        }
    }
    
    /// Update the histogram with a new value
    pub fn update(&mut self, value: f64) {
        // Update count and sum
        self.count += 1;
        self.sum += value;
        
        // Update min and max
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        
        // Update mean and variance using Welford's algorithm
        let old_mean = self.mean;
        self.mean += (value - self.mean) / self.count as f64;
        let s = (value - old_mean) * (value - self.mean);
        self.variance = (self.variance * (self.count - 1) as f64 + s) / self.count as f64;
        self.std_dev = self.variance.sqrt();
        
        // Add to samples for percentiles
        if self.samples.len() >= self.max_samples {
            self.samples.remove(0);
        }
        self.samples.push(value);
    }
    
    /// Get the count of samples
    pub fn count(&self) -> u64 {
        self.count
    }
    
    /// Get the minimum value
    pub fn min(&self) -> f64 {
        self.min
    }
    
    /// Get the maximum value
    pub fn max(&self) -> f64 {
        self.max
    }
    
    /// Get the mean value
    pub fn mean(&self) -> f64 {
        self.mean
    }
    
    /// Get the standard deviation
    pub fn std_dev(&self) -> f64 {
        self.std_dev
    }
    
    /// Get value at the given percentile (0.0 to 1.0)
    pub fn percentile(&self, p: f64) -> Option<f64> {
        if self.samples.is_empty() {
            return None;
        }
        
        // Sort a copy of the samples
        let mut sorted = self.samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        // Calculate the index for the percentile
        let index = (p * (sorted.len() - 1) as f64).round() as usize;
        
        // Return the value at the calculated index
        Some(sorted[index.min(sorted.len() - 1)])
    }
    
    /// Get median (50th percentile)
    pub fn median(&self) -> Option<f64> {
        self.percentile(0.5)
    }
    
    /// Get 75th percentile
    pub fn p75(&self) -> Option<f64> {
        self.percentile(0.75)
    }
    
    /// Get 95th percentile
    pub fn p95(&self) -> Option<f64> {
        self.percentile(0.95)
    }
    
    /// Get 99th percentile
    pub fn p99(&self) -> Option<f64> {
        self.percentile(0.99)
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new(1000) // Default to 1000 samples
    }
}

/// Metric types that can be registered
#[derive(Debug, Clone)]
pub enum Metric {
    /// A counter metric
    Counter(Counter),
    /// A gauge metric
    Gauge(Gauge),
    /// A meter metric
    Meter(Meter),
    /// A histogram metric
    Histogram(Histogram),
}

/// A registry for all metrics
#[derive(Debug, Default)]
pub struct MetricRegistry {
    /// All registered metrics
    metrics: RwLock<HashMap<String, Metric>>,
}

impl MetricRegistry {
    /// Create a new metrics registry
    pub fn new() -> Self {
        Self {
            metrics: RwLock::new(HashMap::new()),
        }
    }
    
    /// Register a counter metric
    pub async fn register_counter(&self, name: &str) -> Counter {
        let counter = Counter::new();
        let mut metrics = self.metrics.write().await;
        metrics.insert(name.to_string(), Metric::Counter(counter.clone()));
        counter
    }
    
    /// Register a gauge metric
    pub async fn register_gauge(&self, name: &str) -> Gauge {
        let gauge = Gauge::new();
        let mut metrics = self.metrics.write().await;
        metrics.insert(name.to_string(), Metric::Gauge(gauge.clone()));
        gauge
    }
    
    /// Register a meter metric
    pub async fn register_meter(&self, name: &str) -> Meter {
        let meter = Meter::new();
        let mut metrics = self.metrics.write().await;
        metrics.insert(name.to_string(), Metric::Meter(meter.clone()));
        meter
    }
    
    /// Register a histogram metric
    pub async fn register_histogram(&self, name: &str, max_samples: usize) -> Histogram {
        let histogram = Histogram::new(max_samples);
        let mut metrics = self.metrics.write().await;
        metrics.insert(name.to_string(), Metric::Histogram(histogram.clone()));
        histogram
    }
    
    /// Get a metric by name
    pub async fn get(&self, name: &str) -> Option<Metric> {
        let metrics = self.metrics.read().await;
        metrics.get(name).cloned()
    }
    
    /// Get all metrics
    pub async fn get_all(&self) -> HashMap<String, Metric> {
        let metrics = self.metrics.read().await;
        metrics.clone()
    }
    
    /// Remove a metric
    pub async fn remove(&self, name: &str) -> bool {
        let mut metrics = self.metrics.write().await;
        metrics.remove(name).is_some()
    }
}

/// Overlay network metrics
#[derive(Debug, Clone)]
pub struct OverlayMetrics {
    /// Registry for all metrics
    registry: Arc<MetricRegistry>,
    
    /// Connections counter
    pub connection_counter: Counter,
    /// Messages counter
    pub message_counter: Counter,
    /// Stream chunks counter
    pub chunk_counter: Counter,
    /// Peers gauge
    pub peers_gauge: Gauge,
    /// Streams gauge
    pub streams_gauge: Gauge,
    /// Connection failures counter
    pub connection_failures: Counter,
    /// Message rate meter
    pub message_rate: Meter,
    /// Chunk rate meter
    pub chunk_rate: Meter,
    /// Relay latency histogram
    pub relay_latency: Histogram,
    /// Chunk size histogram
    pub chunk_size: Histogram,
}

impl OverlayMetrics {
    /// Create a new set of overlay metrics
    pub async fn new() -> Self {
        let registry = Arc::new(MetricRegistry::new());
        
        let connection_counter = registry.register_counter("overlay.connections").await;
        let message_counter = registry.register_counter("overlay.messages").await;
        let chunk_counter = registry.register_counter("overlay.chunks").await;
        let peers_gauge = registry.register_gauge("overlay.peers").await;
        let streams_gauge = registry.register_gauge("overlay.streams").await;
        let connection_failures = registry.register_counter("overlay.connection_failures").await;
        let message_rate = registry.register_meter("overlay.message_rate").await;
        let chunk_rate = registry.register_meter("overlay.chunk_rate").await;
        let relay_latency = registry.register_histogram("overlay.relay_latency", 1000).await;
        let chunk_size = registry.register_histogram("overlay.chunk_size", 1000).await;
        
        Self {
            registry,
            connection_counter,
            message_counter,
            chunk_counter,
            peers_gauge,
            streams_gauge,
            connection_failures,
            message_rate,
            chunk_rate,
            relay_latency,
            chunk_size,
        }
    }
    
    /// Get all metrics as a formatted string
    pub async fn report(&self) -> String {
        let metrics = self.registry.get_all().await;
        let mut result = String::new();
        
        for (name, metric) in metrics {
            match metric {
                Metric::Counter(counter) => {
                    result.push_str(&format!("{} = {}\n", name, counter.value()));
                },
                Metric::Gauge(gauge) => {
                    result.push_str(&format!("{} = {:.2}\n", name, gauge.value()));
                },
                Metric::Meter(meter) => {
                    result.push_str(&format!("{}.rate_1m = {:.2}\n", name, meter.one_minute_rate()));
                    result.push_str(&format!("{}.rate_5m = {:.2}\n", name, meter.five_minute_rate()));
                    result.push_str(&format!("{}.rate_15m = {:.2}\n", name, meter.fifteen_minute_rate()));
                },
                Metric::Histogram(hist) => {
                    if hist.count() > 0 {
                        result.push_str(&format!("{}.count = {}\n", name, hist.count()));
                        result.push_str(&format!("{}.min = {:.2}\n", name, hist.min()));
                        result.push_str(&format!("{}.mean = {:.2}\n", name, hist.mean()));
                        result.push_str(&format!("{}.max = {:.2}\n", name, hist.max()));
                        
                        if let Some(p50) = hist.median() {
                            result.push_str(&format!("{}.p50 = {:.2}\n", name, p50));
                        }
                        
                        if let Some(p95) = hist.p95() {
                            result.push_str(&format!("{}.p95 = {:.2}\n", name, p95));
                        }
                        
                        if let Some(p99) = hist.p99() {
                            result.push_str(&format!("{}.p99 = {:.2}\n", name, p99));
                        }
                    }
                },
            }
        }
        
        result
    }
    
    /// Start a metrics reporter task
    pub async fn start_reporter(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let metrics = self.clone();
        
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                let report = metrics.report().await;
                tracing::debug!("Metrics report:\n{}", report);
            }
        })
    }
} 