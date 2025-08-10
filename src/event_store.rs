use crate::types::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

pub struct PostgresEventStore {
    pool: PgPool,
}

impl PostgresEventStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Initialize database schema for IoT event store
    pub async fn initialize_schema(&self) -> Result<()> {
        // Main events table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                id UUID PRIMARY KEY,
                aggregate_id UUID NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                data JSONB NOT NULL,
                metadata JSONB NOT NULL,
                version BIGINT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                device_id VARCHAR(100),
                location VARCHAR(200),
                
                CONSTRAINT unique_aggregate_version UNIQUE (aggregate_id, version)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create indexes for events table
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_aggregate_id ON events (aggregate_id)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_device_id ON events (device_id)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_location ON events (location)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_device_timestamp ON events (device_id, timestamp)")
            .execute(&self.pool)
            .await?;

        // Specialized table for sensor data for faster queries
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sensor_data (
                id UUID PRIMARY KEY,
                event_id UUID REFERENCES events(id),
                device_id VARCHAR(100) NOT NULL,
                sensor_type VARCHAR(50) NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                unit VARCHAR(20) NOT NULL,
                quality VARCHAR(20) NOT NULL,
                location VARCHAR(200),
                timestamp TIMESTAMPTZ NOT NULL,
                
                CONSTRAINT unique_device_sensor_timestamp UNIQUE (device_id, sensor_type, timestamp)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create indexes for sensor_data table
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sensor_data_device_type ON sensor_data (device_id, sensor_type)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_sensor_data_timestamp ON sensor_data (timestamp)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sensor_data_device_timestamp ON sensor_data (device_id, timestamp)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sensor_data_type_timestamp ON sensor_data (sensor_type, timestamp)")
            .execute(&self.pool)
            .await?;

        // Device registry table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS devices (
                device_id VARCHAR(100) PRIMARY KEY,
                aggregate_id UUID UNIQUE NOT NULL,
                device_type VARCHAR(50),
                location VARCHAR(200),
                is_registered BOOLEAN DEFAULT FALSE,
                is_connected BOOLEAN DEFAULT FALSE,
                last_seen TIMESTAMPTZ,
                configuration JSONB,
                capabilities JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create indexes for devices table
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_devices_location ON devices (location)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_devices_type ON devices (device_type)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_devices_status ON devices (is_connected, is_registered)")
            .execute(&self.pool)
            .await?;

        // Alerts table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS alerts (
                id UUID PRIMARY KEY,
                device_id VARCHAR(100) NOT NULL,
                alert_type VARCHAR(50) NOT NULL,
                severity VARCHAR(20) NOT NULL,
                message TEXT,
                threshold_value DOUBLE PRECISION,
                actual_value DOUBLE PRECISION,
                triggered_at TIMESTAMPTZ NOT NULL,
                resolved_at TIMESTAMPTZ,
                acknowledged_at TIMESTAMPTZ,
                is_active BOOLEAN DEFAULT TRUE
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create indexes for alerts table
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_alerts_device_id ON alerts (device_id)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts (is_active, triggered_at)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts (severity, triggered_at)",
        )
        .execute(&self.pool)
        .await?;

        // Aggregations table for pre-computed analytics
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS aggregations (
                id UUID PRIMARY KEY,
                device_id VARCHAR(100) NOT NULL,
                sensor_type VARCHAR(50) NOT NULL,
                aggregation_type VARCHAR(20) NOT NULL,
                time_window_minutes INTEGER NOT NULL,
                window_start TIMESTAMPTZ NOT NULL,
                window_end TIMESTAMPTZ NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                sample_count INTEGER NOT NULL,
                unit VARCHAR(20),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                
                CONSTRAINT unique_aggregation UNIQUE (device_id, sensor_type, aggregation_type, time_window_minutes, window_start)
            )
            "#
        )
        .execute(&self.pool)
        .await?;

        // Create indexes for aggregations table
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_aggregations_device_sensor ON aggregations (device_id, sensor_type)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_aggregations_window ON aggregations (window_start, window_end)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_aggregations_type ON aggregations (aggregation_type, time_window_minutes)")
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn save_events(
        &self,
        aggregate_id: Uuid,
        events: Vec<Event>,
        expected_version: u64,
    ) -> Result<(), EventStoreError> {
        let mut tx = self.pool.begin().await?;

        // Check current version for optimistic concurrency control
        let current_version: Option<i64> =
            sqlx::query("SELECT MAX(version) FROM events WHERE aggregate_id = $1")
                .bind(aggregate_id)
                .fetch_optional(&mut *tx)
                .await?
                .and_then(|row| row.get(0));

        let current_version = current_version.unwrap_or(0) as u64;
        if current_version != expected_version {
            return Err(EventStoreError::ConcurrencyConflict {
                expected: expected_version,
                actual: current_version,
            });
        }

        // Insert events and related data
        for (i, event) in events.iter().enumerate() {
            let version = expected_version + i as u64 + 1;

            // Insert main event
            sqlx::query(
                r#"
                INSERT INTO events (id, aggregate_id, event_type, data, metadata, version, timestamp, device_id, location)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#
            )
            .bind(event.id)
            .bind(event.aggregate_id)
            .bind(&event.event_type)
            .bind(&event.data)
            .bind(serde_json::to_value(&event.metadata)?)
            .bind(version as i64)
            .bind(event.timestamp)
            .bind(&event.metadata.device_id)
            .bind(&event.metadata.location)
            .execute(&mut *tx)
            .await?;

            // Insert specialized data based on event type
            if let Ok(domain_event) = serde_json::from_value::<DomainEvent>(event.data.clone()) {
                self.save_specialized_data(&mut tx, event, &domain_event)
                    .await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    async fn save_specialized_data(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        event: &Event,
        domain_event: &DomainEvent,
    ) -> Result<(), EventStoreError> {
        match domain_event {
            DomainEvent::SensorDataReceived {
                device_id,
                sensor_type,
                value,
                unit,
                quality,
                location,
            } => {
                sqlx::query(
                    r#"
                    INSERT INTO sensor_data (id, event_id, device_id, sensor_type, value, unit, quality, location, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (device_id, sensor_type, timestamp) DO NOTHING
                    "#
                )
                .bind(Uuid::new_v4())
                .bind(event.id)
                .bind(device_id)
                .bind(serde_json::to_string(sensor_type)?)
                .bind(value)
                .bind(unit)
                .bind(serde_json::to_string(quality)?)
                .bind(location)
                .bind(event.timestamp)
                .execute(&mut **tx)
                .await?;
            }
            DomainEvent::DeviceRegistered {
                device_id,
                device_type,
                location,
                capabilities,
            } => {
                sqlx::query(
                    r#"
                    INSERT INTO devices (device_id, aggregate_id, device_type, location, is_registered, capabilities, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, TRUE, $5, NOW(), NOW())
                    ON CONFLICT (device_id) 
                    DO UPDATE SET 
                        device_type = EXCLUDED.device_type,
                        location = EXCLUDED.location,
                        is_registered = TRUE,
                        capabilities = EXCLUDED.capabilities,
                        updated_at = NOW()
                    "#
                )
                .bind(device_id)
                .bind(event.aggregate_id)
                .bind(serde_json::to_string(device_type)?)
                .bind(location)
                .bind(serde_json::to_value(capabilities)?)
                .execute(&mut **tx)
                .await?;
            }
            DomainEvent::DeviceConnected {
                device_id,
                connection_timestamp,
                ..
            } => {
                sqlx::query(
                    r#"
                    UPDATE devices 
                    SET is_connected = TRUE, last_seen = $2, updated_at = NOW()
                    WHERE device_id = $1
                    "#,
                )
                .bind(device_id)
                .bind(connection_timestamp)
                .execute(&mut **tx)
                .await?;
            }
            DomainEvent::DeviceDisconnected {
                device_id,
                disconnection_timestamp,
                ..
            } => {
                sqlx::query(
                    r#"
                    UPDATE devices 
                    SET is_connected = FALSE, last_seen = $2, updated_at = NOW()
                    WHERE device_id = $1
                    "#,
                )
                .bind(device_id)
                .bind(disconnection_timestamp)
                .execute(&mut **tx)
                .await?;
            }
            DomainEvent::AlertTriggered {
                device_id,
                alert_type,
                severity,
                message,
                threshold_value,
                actual_value,
                ..
            } => {
                sqlx::query(
                    r#"
                    INSERT INTO alerts (id, device_id, alert_type, severity, message, threshold_value, actual_value, triggered_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                    "#
                )
                .bind(Uuid::new_v4())
                .bind(device_id)
                .bind(serde_json::to_string(alert_type)?)
                .bind(serde_json::to_string(severity)?)
                .bind(message)
                .bind(threshold_value)
                .bind(actual_value)
                .execute(&mut **tx)
                .await?;
            }
            _ => {
                // Other events don't need specialized storage
            }
        }
        Ok(())
    }

    pub async fn load_events(&self, aggregate_id: Uuid) -> Result<Vec<Event>, EventStoreError> {
        let rows = sqlx::query("SELECT * FROM events WHERE aggregate_id = $1 ORDER BY version ASC")
            .bind(aggregate_id)
            .fetch_all(&self.pool)
            .await?;

        let mut events = Vec::new();
        for row in rows {
            let metadata_json: Value = row.get("metadata");
            let event = Event {
                id: row.get("id"),
                aggregate_id: row.get("aggregate_id"),
                event_type: row.get("event_type"),
                data: row.get("data"),
                metadata: serde_json::from_value(metadata_json)?,
                timestamp: row.get("timestamp"),
            };
            events.push(event);
        }
        Ok(events)
    }

    // IoT-specific query methods

    pub async fn get_sensor_data(
        &self,
        device_id: &str,
        sensor_type: Option<&SensorType>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: Option<i32>,
    ) -> Result<Vec<SensorDataPoint>> {
        let mut query = String::from(
            r#"
            SELECT device_id, sensor_type, value, unit, quality, location, timestamp
            FROM sensor_data
            WHERE device_id = $1 AND timestamp BETWEEN $2 AND $3
            "#,
        );

        let start_time_s = start_time.to_rfc3339();
        let end_time_s = end_time.to_rfc3339();
        let mut params = vec![device_id, &start_time_s, &end_time_s];

        if let Some(sensor_type) = sensor_type {
            query.push_str(" AND sensor_type = $4");
            params.push(&serde_json::to_string(sensor_type).unwrap());
        }

        query.push_str(" ORDER BY timestamp DESC");

        if let Some(limit) = limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }

        let rows = sqlx::query(&query)
            .bind(device_id)
            .bind(start_time)
            .bind(end_time)
            .fetch_all(&self.pool)
            .await?;

        let mut data_points = Vec::new();
        for row in rows {
            let sensor_type_str: String = row.get("sensor_type");
            let quality_str: String = row.get("quality");

            data_points.push(SensorDataPoint {
                device_id: row.get("device_id"),
                sensor_type: serde_json::from_str(&sensor_type_str)?,
                value: row.get("value"),
                unit: row.get("unit"),
                quality: serde_json::from_str(&quality_str)?,
                location: row.get("location"),
                timestamp: row.get("timestamp"),
            });
        }

        Ok(data_points)
    }

    pub async fn get_device_status(&self, device_id: &str) -> Result<Option<DeviceStatus>> {
        let row = sqlx::query(
            r#"
            SELECT device_id, device_type, location, is_registered, is_connected, 
                   last_seen, configuration, capabilities
            FROM devices
            WHERE device_id = $1
            "#,
        )
        .bind(device_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let device_type_str: Option<String> = row.get("device_type");
            let device_type = if let Some(type_str) = device_type_str {
                Some(serde_json::from_str(&type_str)?)
            } else {
                None
            };

            Ok(Some(DeviceStatus {
                device_id: row.get("device_id"),
                device_type,
                location: row.get("location"),
                is_registered: row.get("is_registered"),
                is_connected: row.get("is_connected"),
                last_seen: row.get("last_seen"),
                configuration: row.get("configuration"),
                capabilities: row.get("capabilities"),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn get_active_alerts(&self, device_id: Option<&str>) -> Result<Vec<AlertInfo>> {
        let (query, bind_device) = if let Some(device_id) = device_id {
            (
                r#"
                SELECT id, device_id, alert_type, severity, message, threshold_value, 
                       actual_value, triggered_at, acknowledged_at
                FROM alerts
                WHERE device_id = $1 AND is_active = TRUE
                ORDER BY triggered_at DESC
                "#,
                Some(device_id),
            )
        } else {
            (
                r#"
                SELECT id, device_id, alert_type, severity, message, threshold_value, 
                       actual_value, triggered_at, acknowledged_at
                FROM alerts
                WHERE is_active = TRUE
                ORDER BY triggered_at DESC
                "#,
                None,
            )
        };

        let rows = if let Some(device_id) = bind_device {
            sqlx::query(query)
                .bind(device_id)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query(query).fetch_all(&self.pool).await?
        };

        let mut alerts = Vec::new();
        for row in rows {
            let alert_type_str: String = row.get("alert_type");
            let severity_str: String = row.get("severity");

            alerts.push(AlertInfo {
                id: row.get("id"),
                device_id: row.get("device_id"),
                alert_type: serde_json::from_str(&alert_type_str)?,
                severity: serde_json::from_str(&severity_str)?,
                message: row.get("message"),
                threshold_value: row.get("threshold_value"),
                actual_value: row.get("actual_value"),
                triggered_at: row.get("triggered_at"),
                acknowledged_at: row.get("acknowledged_at"),
            });
        }

        Ok(alerts)
    }

    pub async fn get_connected_devices(&self, location: Option<&str>) -> Result<Vec<String>> {
        let (query, bind_location) = if let Some(location) = location {
            (
                "SELECT device_id FROM devices WHERE is_connected = TRUE AND location = $1",
                Some(location),
            )
        } else {
            (
                "SELECT device_id FROM devices WHERE is_connected = TRUE",
                None,
            )
        };

        let rows = if let Some(location) = bind_location {
            sqlx::query(query)
                .bind(location)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query(query).fetch_all(&self.pool).await?
        };

        Ok(rows.into_iter().map(|row| row.get("device_id")).collect())
    }
}

// Additional types for IoT queries

#[derive(Debug, Clone)]
pub struct SensorDataPoint {
    pub device_id: String,
    pub sensor_type: SensorType,
    pub value: f64,
    pub unit: String,
    pub quality: DataQuality,
    pub location: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DeviceStatus {
    pub device_id: String,
    pub device_type: Option<DeviceType>,
    pub location: Option<String>,
    pub is_registered: bool,
    pub is_connected: bool,
    pub last_seen: Option<DateTime<Utc>>,
    pub configuration: Option<Value>,
    pub capabilities: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct AlertInfo {
    pub id: Uuid,
    pub device_id: String,
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub message: Option<String>,
    pub threshold_value: Option<f64>,
    pub actual_value: Option<f64>,
    pub triggered_at: DateTime<Utc>,
    pub acknowledged_at: Option<DateTime<Utc>>,
}

#[derive(Debug, thiserror::Error)]
pub enum EventStoreError {
    #[error("Concurrency conflict: expected version {expected}, got {actual}")]
    ConcurrencyConflict { expected: u64, actual: u64 },
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Invalid device ID format: {0}")]
    InvalidDeviceId(String),
}
