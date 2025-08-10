use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub data: serde_json::Value,
    pub metadata: EventMetadata,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub version: u64,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub device_id: Option<String>,
    pub location: Option<String>,
    pub batch_id: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum DomainEvent {
    // Device lifecycle events
    DeviceRegistered {
        device_id: String,
        device_type: DeviceType,
        location: String,
        capabilities: Vec<SensorCapability>,
    },
    DeviceConnected {
        device_id: String,
        connection_timestamp: DateTime<Utc>,
        ip_address: Option<String>,
    },
    DeviceDisconnected {
        device_id: String,
        disconnection_timestamp: DateTime<Utc>,
        reason: DisconnectionReason,
    },
    DeviceConfigurationUpdated {
        device_id: String,
        old_config: DeviceConfiguration,
        new_config: DeviceConfiguration,
    },

    // Sensor data events
    SensorDataReceived {
        device_id: String,
        sensor_type: SensorType,
        value: f64,
        unit: String,
        quality: DataQuality,
        location: Option<String>,
    },
    SensorDataBatch {
        device_id: String,
        batch_id: Uuid,
        measurements: Vec<SensorMeasurement>,
        batch_timestamp: DateTime<Utc>,
    },

    // Data processing events
    DataValidated {
        original_event_id: Uuid,
        device_id: String,
        validation_result: ValidationResult,
    },
    DataAggregated {
        device_ids: Vec<String>,
        aggregation_type: AggregationType,
        time_window: TimeWindow,
        result: AggregationResult,
    },

    // Alert events
    AlertTriggered {
        device_id: String,
        alert_type: AlertType,
        threshold_value: f64,
        actual_value: f64,
        severity: AlertSeverity,
        message: String,
    },
    AlertResolved {
        device_id: String,
        alert_id: Uuid,
        resolution_timestamp: DateTime<Utc>,
    },

    // System events
    SystemMaintenanceScheduled {
        device_ids: Vec<String>,
        maintenance_window: TimeWindow,
        maintenance_type: MaintenanceType,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeviceType {
    TemperatureSensor,
    HumiditySensor,
    PressureSensor,
    MotionDetector,
    SmartMeter,
    Gateway,
    Camera,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SensorType {
    Temperature,
    Humidity,
    Pressure,
    Motion,
    Light,
    Sound,
    Power,
    Voltage,
    Current,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorCapability {
    pub sensor_type: SensorType,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
    pub precision: Option<u8>,
    pub sampling_rate: Option<u32>, // Hz
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorMeasurement {
    pub sensor_type: SensorType,
    pub value: f64,
    pub unit: String,
    pub timestamp: DateTime<Utc>,
    pub quality: DataQuality,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataQuality {
    Good,
    Uncertain,
    Bad,
    Maintenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DisconnectionReason {
    NetworkTimeout,
    PowerFailure,
    Maintenance,
    UserRequested,
    SystemShutdown,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceConfiguration {
    pub sampling_interval: u32,  // seconds
    pub reporting_interval: u32, // seconds
    pub data_compression: bool,
    pub alert_thresholds: HashMap<SensorType, AlertThreshold>,
    pub custom_settings: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThreshold {
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
    pub rate_of_change: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationResult {
    Valid,
    OutOfRange {
        expected_range: (f64, f64),
        actual: f64,
    },
    RateOfChangeExceeded {
        max_rate: f64,
        actual_rate: f64,
    },
    DuplicateData {
        previous_timestamp: DateTime<Utc>,
    },
    InvalidFormat {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationType {
    Average,
    Sum,
    Min,
    Max,
    Count,
    StandardDeviation,
    Percentile(u8),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub duration_minutes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationResult {
    pub value: f64,
    pub sample_count: u32,
    pub sensor_type: SensorType,
    pub unit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertType {
    ThresholdExceeded,
    DeviceOffline,
    DataQualityDegraded,
    RateOfChangeExceeded,
    MaintenanceRequired,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaintenanceType {
    Scheduled,
    Emergency,
    Firmware,
    Calibration,
}

// User-related events (commented out as they're not needed for IoT pipeline)
/*
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum UserDomainEvent {
    UserRegistered {
        user_id: Uuid,
        email: String,
        username: String,
    },
    UserEmailChanged {
        user_id: Uuid,
        old_email: String,
        new_email: String,
    },
    UserDeactivated {
        user_id: Uuid,
        reason: String,
    },
}
*/
