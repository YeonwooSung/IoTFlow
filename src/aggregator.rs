use crate::event_bus::EventBus;
use crate::event_store::PostgresEventStore;
use crate::types::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct DeviceAggregate {
    pub id: String, // Device ID (not UUID)
    pub device_type: Option<DeviceType>,
    pub location: Option<String>,
    pub capabilities: Vec<SensorCapability>,
    pub configuration: Option<DeviceConfiguration>,
    pub is_connected: bool,
    pub is_registered: bool,
    pub last_seen: Option<DateTime<Utc>>,
    pub alert_thresholds: HashMap<SensorType, AlertThreshold>,
    pub version: u64,
}

impl DeviceAggregate {
    pub fn new(device_id: String) -> Self {
        Self {
            id: device_id,
            device_type: None,
            location: None,
            capabilities: Vec::new(),
            configuration: None,
            is_connected: false,
            is_registered: false,
            last_seen: None,
            alert_thresholds: HashMap::new(),
            version: 0,
        }
    }

    pub fn register_device(
        &mut self,
        device_type: DeviceType,
        location: String,
        capabilities: Vec<SensorCapability>,
    ) -> Result<DomainEvent> {
        if self.is_registered {
            return Err(anyhow::anyhow!("Device already registered"));
        }

        Ok(DomainEvent::DeviceRegistered {
            device_id: self.id.clone(),
            device_type,
            location,
            capabilities,
        })
    }

    pub fn connect_device(&mut self, ip_address: Option<String>) -> Result<DomainEvent> {
        if !self.is_registered {
            return Err(anyhow::anyhow!(
                "Device must be registered before connecting"
            ));
        }
        if self.is_connected {
            return Err(anyhow::anyhow!("Device already connected"));
        }

        Ok(DomainEvent::DeviceConnected {
            device_id: self.id.clone(),
            connection_timestamp: Utc::now(),
            ip_address,
        })
    }

    pub fn disconnect_device(&mut self, reason: DisconnectionReason) -> Result<DomainEvent> {
        if !self.is_connected {
            return Err(anyhow::anyhow!("Device not connected"));
        }

        Ok(DomainEvent::DeviceDisconnected {
            device_id: self.id.clone(),
            disconnection_timestamp: Utc::now(),
            reason,
        })
    }

    pub fn process_sensor_data(
        &mut self,
        sensor_type: SensorType,
        value: f64,
        unit: String,
        quality: DataQuality,
    ) -> Result<Vec<DomainEvent>> {
        if !self.is_connected {
            return Err(anyhow::anyhow!("Device not connected"));
        }

        let mut events = vec![DomainEvent::SensorDataReceived {
            device_id: self.id.clone(),
            sensor_type: sensor_type.clone(),
            value,
            unit: unit.clone(),
            quality: quality.clone(),
            location: self.location.clone(),
        }];

        // Check for alert conditions
        if let Some(threshold) = self.alert_thresholds.get(&sensor_type) {
            if let Some(alert_event) =
                self.check_alert_conditions(&sensor_type, value, threshold)?
            {
                events.push(alert_event);
            }
        }

        Ok(events)
    }

    pub fn update_configuration(&mut self, new_config: DeviceConfiguration) -> Result<DomainEvent> {
        if !self.is_registered {
            return Err(anyhow::anyhow!("Device must be registered first"));
        }

        let old_config = self
            .configuration
            .clone()
            .unwrap_or_else(|| DeviceConfiguration {
                sampling_interval: 60,
                reporting_interval: 300,
                data_compression: false,
                alert_thresholds: HashMap::new(),
                custom_settings: HashMap::new(),
            });

        Ok(DomainEvent::DeviceConfigurationUpdated {
            device_id: self.id.clone(),
            old_config,
            new_config,
        })
    }

    fn check_alert_conditions(
        &self,
        _sensor_type: &SensorType,
        value: f64,
        threshold: &AlertThreshold,
    ) -> Result<Option<DomainEvent>> {
        // Check min threshold
        if let Some(min_val) = threshold.min_value {
            if value < min_val {
                return Ok(Some(DomainEvent::AlertTriggered {
                    device_id: self.id.clone(),
                    alert_type: AlertType::ThresholdExceeded,
                    threshold_value: min_val,
                    actual_value: value,
                    severity: AlertSeverity::Medium,
                    message: format!("Value {} below minimum threshold {}", value, min_val),
                }));
            }
        }

        // Check max threshold
        if let Some(max_val) = threshold.max_value {
            if value > max_val {
                return Ok(Some(DomainEvent::AlertTriggered {
                    device_id: self.id.clone(),
                    alert_type: AlertType::ThresholdExceeded,
                    threshold_value: max_val,
                    actual_value: value,
                    severity: AlertSeverity::High,
                    message: format!("Value {} exceeds maximum threshold {}", value, max_val),
                }));
            }
        }

        Ok(None)
    }

    pub fn apply_event(&mut self, event: &DomainEvent) {
        match event {
            DomainEvent::DeviceRegistered {
                device_id: _,
                device_type,
                location,
                capabilities,
            } => {
                self.device_type = Some(device_type.clone());
                self.location = Some(location.clone());
                self.capabilities = capabilities.clone();
                self.is_registered = true;
            }
            DomainEvent::DeviceConnected {
                device_id: _,
                connection_timestamp,
                ip_address: _,
            } => {
                self.is_connected = true;
                self.last_seen = Some(*connection_timestamp);
            }
            DomainEvent::DeviceDisconnected {
                device_id: _,
                disconnection_timestamp,
                reason: _,
            } => {
                self.is_connected = false;
                self.last_seen = Some(*disconnection_timestamp);
            }
            DomainEvent::DeviceConfigurationUpdated {
                device_id: _,
                old_config: _,
                new_config,
            } => {
                self.configuration = Some(new_config.clone());
                self.alert_thresholds = new_config.alert_thresholds.clone();
            }
            DomainEvent::SensorDataReceived {
                device_id: _,
                sensor_type: _,
                value: _,
                unit: _,
                quality: _,
                location: _,
            } => {
                self.last_seen = Some(Utc::now());
            }
            _ => {
                // Other events don't affect device aggregate state
            }
        }
        self.version += 1;
    }
}

pub struct IoTCommandHandler {
    event_store: Arc<PostgresEventStore>,
    event_bus: Arc<EventBus>,
}

impl IoTCommandHandler {
    pub fn new(event_store: Arc<PostgresEventStore>, event_bus: Arc<EventBus>) -> Self {
        Self {
            event_store,
            event_bus,
        }
    }

    pub async fn handle_register_device(
        &self,
        device_id: String,
        device_type: DeviceType,
        location: String,
        capabilities: Vec<SensorCapability>,
    ) -> Result<()> {
        let aggregate_uuid = self.device_id_to_uuid(&device_id);
        let events = self.event_store.load_events(aggregate_uuid).await?;
        let mut aggregate = DeviceAggregate::new(device_id);

        // Apply existing events
        for event in &events {
            if let Ok(domain_event) = serde_json::from_value::<DomainEvent>(event.data.clone()) {
                aggregate.apply_event(&domain_event);
            }
        }

        // Execute command
        let domain_event = aggregate.register_device(device_type, location, capabilities)?;

        // Create and save event
        let event = Event {
            id: Uuid::new_v4(),
            aggregate_id: aggregate_uuid,
            event_type: "DeviceRegistered".to_string(),
            data: serde_json::to_value(&domain_event)?,
            metadata: EventMetadata {
                version: aggregate.version + 1,
                causation_id: None,
                correlation_id: None,
                device_id: Some(aggregate.id.clone()),
                location: aggregate.location.clone(),
                batch_id: None,
            },
            timestamp: Utc::now(),
        };

        self.save_and_publish_event(aggregate_uuid, event, aggregate.version)
            .await
    }

    pub async fn handle_sensor_data(
        &self,
        device_id: String,
        sensor_type: SensorType,
        value: f64,
        unit: String,
        quality: DataQuality,
    ) -> Result<()> {
        let aggregate_uuid = self.device_id_to_uuid(&device_id);
        let events = self.event_store.load_events(aggregate_uuid).await?;
        let mut aggregate = DeviceAggregate::new(device_id);

        // Apply existing events
        for event in &events {
            if let Ok(domain_event) = serde_json::from_value::<DomainEvent>(event.data.clone()) {
                aggregate.apply_event(&domain_event);
            }
        }

        // Process sensor data (may generate multiple events)
        let domain_events = aggregate.process_sensor_data(sensor_type, value, unit, quality)?;

        // Create and save events
        let mut events_to_save = Vec::new();
        for (i, domain_event) in domain_events.iter().enumerate() {
            let event = Event {
                id: Uuid::new_v4(),
                aggregate_id: aggregate_uuid,
                event_type: match domain_event {
                    DomainEvent::SensorDataReceived { .. } => "SensorDataReceived",
                    DomainEvent::AlertTriggered { .. } => "AlertTriggered",
                    _ => "Unknown",
                }
                .to_string(),
                data: serde_json::to_value(&domain_event)?,
                metadata: EventMetadata {
                    version: aggregate.version + i as u64 + 1,
                    causation_id: None,
                    correlation_id: None,
                    device_id: Some(aggregate.id.clone()),
                    location: aggregate.location.clone(),
                    batch_id: None,
                },
                timestamp: Utc::now(),
            };
            events_to_save.push(event);
        }

        self.save_and_publish_events(aggregate_uuid, events_to_save, aggregate.version)
            .await
    }

    pub async fn handle_device_connection(
        &self,
        device_id: String,
        ip_address: Option<String>,
    ) -> Result<()> {
        let aggregate_uuid = self.device_id_to_uuid(&device_id);
        let events = self.event_store.load_events(aggregate_uuid).await?;
        let mut aggregate = DeviceAggregate::new(device_id);

        for event in &events {
            if let Ok(domain_event) = serde_json::from_value::<DomainEvent>(event.data.clone()) {
                aggregate.apply_event(&domain_event);
            }
        }

        let domain_event = aggregate.connect_device(ip_address)?;

        let event = Event {
            id: Uuid::new_v4(),
            aggregate_id: aggregate_uuid,
            event_type: "DeviceConnected".to_string(),
            data: serde_json::to_value(&domain_event)?,
            metadata: EventMetadata {
                version: aggregate.version + 1,
                causation_id: None,
                correlation_id: None,
                device_id: Some(aggregate.id.clone()),
                location: aggregate.location.clone(),
                batch_id: None,
            },
            timestamp: Utc::now(),
        };

        self.save_and_publish_event(aggregate_uuid, event, aggregate.version)
            .await
    }

    // Helper methods
    fn device_id_to_uuid(&self, device_id: &str) -> Uuid {
        // Create deterministic UUID from device_id using namespace
        use uuid::Uuid;
        let namespace = Uuid::NAMESPACE_OID; // Use standard namespace
        Uuid::new_v5(&namespace, device_id.as_bytes())
    }

    async fn save_and_publish_event(
        &self,
        aggregate_uuid: Uuid,
        event: Event,
        expected_version: u64,
    ) -> Result<()> {
        self.event_store
            .save_events(aggregate_uuid, vec![event.clone()], expected_version)
            .await?;
        self.event_bus.publish(event).await?;
        Ok(())
    }

    async fn save_and_publish_events(
        &self,
        aggregate_uuid: Uuid,
        events: Vec<Event>,
        expected_version: u64,
    ) -> Result<()> {
        self.event_store
            .save_events(aggregate_uuid, events.clone(), expected_version)
            .await?;

        for event in events {
            self.event_bus.publish(event).await?;
        }
        Ok(())
    }
}
