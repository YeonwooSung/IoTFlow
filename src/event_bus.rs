use crate::types::*;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use uuid::Uuid;

#[async_trait]
pub trait EventHandler: Send + Sync {
    async fn handle(&self, event: &Event) -> Result<()>;
    fn name(&self) -> &str;
    fn can_handle(&self, event_type: &str) -> bool;
}

pub struct EventBus {
    sender: broadcast::Sender<Event>,
    handlers: Arc<RwLock<HashMap<String, Vec<Arc<dyn EventHandler>>>>>,
    metrics: Arc<RwLock<EventMetrics>>,
}

#[derive(Debug, Default, Clone)]
pub struct EventMetrics {
    pub total_events_published: u64,
    pub total_events_processed: u64,
    pub events_by_type: HashMap<String, u64>,
    pub handler_errors: HashMap<String, u64>,
    pub processing_times: HashMap<String, Vec<u64>>, // microseconds
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(EventMetrics::default())),
        }
    }

    pub async fn publish(&self, event: Event) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        metrics.total_events_published += 1;
        *metrics
            .events_by_type
            .entry(event.event_type.clone())
            .or_insert(0) += 1;
        drop(metrics);

        println!(
            "Publishing IoT event: {} for device: {} at {}",
            event.event_type,
            event
                .metadata
                .device_id
                .as_ref()
                .unwrap_or(&"unknown".to_string()),
            event.timestamp
        );

        match self.sender.send(event) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("Failed to publish event: {}", e)),
        }
    }

    pub async fn subscribe(&self, event_type: String, handler: Arc<dyn EventHandler>) {
        let mut handlers = self.handlers.write().await;
        handlers
            .entry(event_type)
            .or_insert_with(Vec::new)
            .push(handler);
    }

    pub async fn start_processing(&self) -> Result<()> {
        let receiver = self.sender.subscribe();
        let mut stream = BroadcastStream::new(receiver);
        let handlers = Arc::clone(&self.handlers);
        let metrics = Arc::clone(&self.metrics);

        while let Some(event_result) = stream.next().await {
            match event_result {
                Ok(event) => {
                    let start_time = std::time::Instant::now();
                    let handlers_lock = handlers.read().await;

                    // Find all handlers that can process this event
                    let mut applicable_handlers = Vec::<Arc<dyn EventHandler>>::new();
                    for (_, handler_list) in handlers_lock.iter() {
                        for handler in handler_list {
                            if handler.can_handle(&event.event_type) {
                                applicable_handlers.push(Arc::clone(handler));
                            }
                        }
                    }
                    drop(handlers_lock);

                    // Process with all applicable handlers
                    for handler in applicable_handlers {
                        let handler_name = handler.name().to_string();
                        match handler.handle(&event).await {
                            Ok(()) => {
                                let processing_time = start_time.elapsed().as_micros() as u64;
                                let mut metrics_lock = metrics.write().await;
                                metrics_lock.total_events_processed += 1;
                                metrics_lock
                                    .processing_times
                                    .entry(handler_name)
                                    .or_insert_with(Vec::new)
                                    .push(processing_time);
                            }
                            Err(e) => {
                                eprintln!("Handler '{}' error: {}", handler_name, e);
                                let mut metrics_lock = metrics.write().await;
                                *metrics_lock.handler_errors.entry(handler_name).or_insert(0) += 1;
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Event stream error: {}", e),
            }
        }
        Ok(())
    }

    pub async fn get_metrics(&self) -> EventMetrics {
        self.metrics.read().await.clone()
    }
}

// Specialized IoT Event Handlers

pub struct SensorDataValidator {
    valid_ranges: HashMap<SensorType, (f64, f64)>,
}

impl SensorDataValidator {
    pub fn new() -> Self {
        let mut valid_ranges = HashMap::new();
        valid_ranges.insert(SensorType::Temperature, (-50.0, 100.0));
        valid_ranges.insert(SensorType::Humidity, (0.0, 100.0));
        valid_ranges.insert(SensorType::Pressure, (800.0, 1200.0));

        Self { valid_ranges }
    }

    fn validate_sensor_data(&self, sensor_type: &SensorType, value: f64) -> ValidationResult {
        if let Some((min, max)) = self.valid_ranges.get(sensor_type) {
            if value < *min || value > *max {
                return ValidationResult::OutOfRange {
                    expected_range: (*min, *max),
                    actual: value,
                };
            }
        }
        ValidationResult::Valid
    }
}

#[async_trait]
impl EventHandler for SensorDataValidator {
    async fn handle(&self, event: &Event) -> Result<()> {
        if let Ok(domain_event) = serde_json::from_value::<DomainEvent>(event.data.clone()) {
            match domain_event {
                DomainEvent::SensorDataReceived {
                    device_id,
                    sensor_type,
                    value,
                    ..
                } => {
                    let validation_result = self.validate_sensor_data(&sensor_type, value);

                    if !matches!(validation_result, ValidationResult::Valid) {
                        println!(
                            "Validation failed for device {}: {:?}",
                            device_id, validation_result
                        );
                        // In a real system, you'd publish a DataValidated event here
                    } else {
                        println!("Data validated successfully for device {}", device_id);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "SensorDataValidator"
    }

    fn can_handle(&self, event_type: &str) -> bool {
        event_type == "SensorDataReceived"
    }
}

pub struct TimeSeriesAggregator {
    aggregation_windows: Vec<Duration>,
    device_data: Arc<RwLock<HashMap<String, Vec<(DateTime<Utc>, f64)>>>>,
}

impl TimeSeriesAggregator {
    pub fn new() -> Self {
        Self {
            aggregation_windows: vec![
                Duration::minutes(1),
                Duration::minutes(5),
                Duration::minutes(15),
                Duration::hours(1),
            ],
            device_data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn add_data_point(&self, device_id: String, timestamp: DateTime<Utc>, value: f64) {
        let mut data = self.device_data.write().await;
        let device_series = data.entry(device_id).or_insert_with(Vec::new);
        device_series.push((timestamp, value));

        // Keep only recent data (last 24 hours)
        let cutoff = Utc::now() - Duration::hours(24);
        device_series.retain(|(ts, _)| *ts > cutoff);
    }

    async fn calculate_aggregations(&self, device_id: &str) -> Vec<AggregationResult> {
        let data = self.device_data.read().await;
        let mut results = Vec::new();

        if let Some(device_series) = data.get(device_id) {
            let now = Utc::now();

            for window in &self.aggregation_windows {
                let window_start = now - *window;
                let window_data: Vec<f64> = device_series
                    .iter()
                    .filter(|(ts, _)| *ts > window_start)
                    .map(|(_, value)| *value)
                    .collect();

                if !window_data.is_empty() {
                    let avg = window_data.iter().sum::<f64>() / window_data.len() as f64;
                    results.push(AggregationResult {
                        value: avg,
                        sample_count: window_data.len() as u32,
                        sensor_type: SensorType::Custom("aggregate".to_string()),
                        unit: "average".to_string(),
                    });
                }
            }
        }

        results
    }
}

#[async_trait]
impl EventHandler for TimeSeriesAggregator {
    async fn handle(&self, event: &Event) -> Result<()> {
        if let Ok(domain_event) = serde_json::from_value::<DomainEvent>(event.data.clone()) {
            match domain_event {
                DomainEvent::SensorDataReceived {
                    device_id, value, ..
                } => {
                    self.add_data_point(device_id.clone(), event.timestamp, value)
                        .await;
                    let aggregations = self.calculate_aggregations(&device_id).await;

                    if !aggregations.is_empty() {
                        println!(
                            "Generated {} aggregations for device {}",
                            aggregations.len(),
                            device_id
                        );
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "TimeSeriesAggregator"
    }

    fn can_handle(&self, event_type: &str) -> bool {
        event_type == "SensorDataReceived"
    }
}

pub struct AlertManager {
    active_alerts: Arc<RwLock<HashMap<String, Vec<ActiveAlert>>>>,
}

#[derive(Debug, Clone)]
struct ActiveAlert {
    id: Uuid,
    device_id: String,
    alert_type: AlertType,
    triggered_at: DateTime<Utc>,
    severity: AlertSeverity,
    acknowledged: bool,
}

impl AlertManager {
    pub fn new() -> Self {
        Self {
            active_alerts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn add_alert(&self, device_id: String, alert_type: AlertType, severity: AlertSeverity) {
        let mut alerts = self.active_alerts.write().await;
        let device_alerts = alerts.entry(device_id.clone()).or_insert_with(Vec::new);

        let alert = ActiveAlert {
            id: Uuid::new_v4(),
            device_id,
            alert_type,
            triggered_at: Utc::now(),
            severity,
            acknowledged: false,
        };

        device_alerts.push(alert);
    }

    async fn resolve_alert(&self, device_id: String, alert_id: Uuid) {
        let mut alerts = self.active_alerts.write().await;
        if let Some(device_alerts) = alerts.get_mut(&device_id) {
            device_alerts.retain(|alert| alert.id != alert_id);
        }
    }

    pub async fn get_active_alerts(&self) -> HashMap<String, Vec<ActiveAlert>> {
        self.active_alerts.read().await.clone()
    }
}

#[async_trait]
impl EventHandler for AlertManager {
    async fn handle(&self, event: &Event) -> Result<()> {
        if let Ok(domain_event) = serde_json::from_value::<DomainEvent>(event.data.clone()) {
            match domain_event {
                DomainEvent::AlertTriggered {
                    device_id,
                    alert_type,
                    severity,
                    ..
                } => {
                    self.add_alert(device_id.clone(), alert_type, severity)
                        .await;
                    println!("Alert triggered for device: {}", device_id);
                }
                DomainEvent::AlertResolved {
                    device_id,
                    alert_id,
                    ..
                } => {
                    self.resolve_alert(device_id.clone(), alert_id).await;
                    println!("Alert resolved for device: {}", device_id);
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "AlertManager"
    }

    fn can_handle(&self, event_type: &str) -> bool {
        matches!(event_type, "AlertTriggered" | "AlertResolved")
    }
}

pub struct DeviceConnectionMonitor {
    connection_timeouts: HashMap<String, Duration>,
}

impl DeviceConnectionMonitor {
    pub fn new() -> Self {
        Self {
            connection_timeouts: HashMap::new(),
        }
    }

    fn check_device_timeout(&self, device_id: &str, last_seen: DateTime<Utc>) -> bool {
        let default_timeout = Duration::minutes(5);
        let timeout = self
            .connection_timeouts
            .get(device_id)
            .unwrap_or(&default_timeout);

        Utc::now() - last_seen > *timeout
    }
}

#[async_trait]
impl EventHandler for DeviceConnectionMonitor {
    async fn handle(&self, event: &Event) -> Result<()> {
        if let Ok(domain_event) = serde_json::from_value::<DomainEvent>(event.data.clone()) {
            match domain_event {
                DomainEvent::DeviceConnected { device_id, .. } => {
                    println!("Device connected: {}", device_id);
                }
                DomainEvent::DeviceDisconnected {
                    device_id, reason, ..
                } => {
                    println!("Device disconnected: {} (reason: {:?})", device_id, reason);
                }
                DomainEvent::SensorDataReceived { device_id, .. } => {
                    // Update last seen timestamp for the device
                    if self.check_device_timeout(&device_id, event.timestamp) {
                        println!(
                            "Warning: Device {} may be experiencing connectivity issues",
                            device_id
                        );
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "DeviceConnectionMonitor"
    }

    fn can_handle(&self, event_type: &str) -> bool {
        matches!(
            event_type,
            "DeviceConnected" | "DeviceDisconnected" | "SensorDataReceived"
        )
    }
}

// Data Persistence Handler for storing processed data
pub struct DataPersistenceHandler {
    // In a real implementation, this would connect to a time-series database
    // like InfluxDB, TimescaleDB, or similar
}

impl DataPersistenceHandler {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl EventHandler for DataPersistenceHandler {
    async fn handle(&self, event: &Event) -> Result<()> {
        // In production, you'd store validated sensor data, aggregations, etc.
        println!(
            "Persisting event: {} to time-series database",
            event.event_type
        );
        Ok(())
    }

    fn name(&self) -> &str {
        "DataPersistenceHandler"
    }

    fn can_handle(&self, event_type: &str) -> bool {
        matches!(
            event_type,
            "SensorDataReceived" | "DataValidated" | "DataAggregated"
        )
    }
}

// Legacy EventBus implementation (commented out)
/*
pub struct EventBus {
    sender: broadcast::Sender<Event>,
    handlers: HashMap<String, Vec<Box<dyn EventHandler>>>,
}
impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            handlers: HashMap::new(),
        }
    }
    pub async fn publish(&self, event: Event) -> Result<(), broadcast::error::SendError<Event>> {
        println!(
            "Publishing event: {} for aggregate: {}",
            event.event_type, event.aggregate_id
        );
        self.sender.send(event)
    }
    pub fn subscribe(&mut self, event_type: String, handler: Box<dyn EventHandler>) {
        self.handlers
            .entry(event_type)
            .or_insert_with(Vec::new)
            .push(handler);
    }
    pub async fn start_processing(&self) {
        let receiver = self.sender.subscribe();
        let mut stream = BroadcastStream::new(receiver);
        while let Some(event_result) = stream.next().await {
            match event_result {
                Ok(event) => {
                    if let Some(handlers) = self.handlers.get(&event.event_type) {
                        for handler in handlers {
                            if let Err(e) = handler.handle(&event).await {
                                eprintln!("Handler error: {}", e);
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Event stream error: {}", e),
            }
        }
    }
}
*/
