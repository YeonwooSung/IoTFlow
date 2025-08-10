mod aggregator;
mod event_bus;
mod event_store;
mod types;

use crate::aggregator::IoTCommandHandler;
use crate::event_bus::{
    AlertManager, DataPersistenceHandler, DeviceConnectionMonitor, EventBus, EventHandler,
    SensorDataValidator, TimeSeriesAggregator,
};
use crate::event_store::PostgresEventStore;
use crate::types::*;

use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::info;

#[derive(Clone)]
pub struct IoTDataPipeline {
    command_handler: Arc<IoTCommandHandler>,
    event_bus: Arc<EventBus>,
    event_store: Arc<PostgresEventStore>,
}

impl IoTDataPipeline {
    pub async fn new(database_url: &str) -> Result<Self> {
        // Initialize database connection
        let pool = sqlx::postgres::PgPool::connect(database_url).await?;
        let event_store = Arc::new(PostgresEventStore::new(pool));

        // Initialize database schema
        event_store.initialize_schema().await?;

        // Create event bus with capacity for high-throughput IoT data
        let event_bus = Arc::new(EventBus::new(10000));

        // Create command handler
        let command_handler = Arc::new(IoTCommandHandler::new(
            Arc::clone(&event_store),
            Arc::clone(&event_bus),
        ));

        // Register event handlers
        Self::register_event_handlers(&event_bus).await;

        Ok(Self {
            command_handler,
            event_bus,
            event_store,
        })
    }

    async fn register_event_handlers(event_bus: &Arc<EventBus>) {
        info!("Registering IoT event handlers...");

        // Data validation handler
        let validator = Arc::new(SensorDataValidator::new());
        event_bus
            .subscribe("SensorDataReceived".to_string(), validator)
            .await;

        // Time-series aggregation handler
        let aggregator = Arc::new(TimeSeriesAggregator::new());
        event_bus
            .subscribe("SensorDataReceived".to_string(), aggregator)
            .await;

        // Alert management handler
        let alert_manager = Arc::new(AlertManager::new()) as Arc<dyn EventHandler>;
        event_bus
            .subscribe("AlertTriggered".to_string(), Arc::clone(&alert_manager))
            .await;
        event_bus
            .subscribe("AlertResolved".to_string(), alert_manager)
            .await;

        // Device connection monitoring
        let connection_monitor = Arc::new(DeviceConnectionMonitor::new()) as Arc<dyn EventHandler>;
        event_bus
            .subscribe(
                "DeviceConnected".to_string(),
                Arc::clone(&connection_monitor),
            )
            .await;
        event_bus
            .subscribe(
                "DeviceDisconnected".to_string(),
                Arc::clone(&connection_monitor),
            )
            .await;
        event_bus
            .subscribe("SensorDataReceived".to_string(), connection_monitor)
            .await;

        // Data persistence handler
        let persistence_handler = Arc::new(DataPersistenceHandler::new()) as Arc<dyn EventHandler>;
        event_bus
            .subscribe(
                "SensorDataReceived".to_string(),
                Arc::clone(&persistence_handler),
            )
            .await;
        event_bus
            .subscribe(
                "DataValidated".to_string(),
                Arc::clone(&persistence_handler),
            )
            .await;
        event_bus
            .subscribe("DataAggregated".to_string(), persistence_handler)
            .await;

        info!("All event handlers registered successfully");
    }

    pub async fn start_processing(&self) -> Result<()> {
        println!("Starting IoT data pipeline processing...");

        // Start event processing in background
        let event_bus = Arc::clone(&self.event_bus);
        tokio::spawn(async move {
            if let Err(e) = event_bus.start_processing().await {
                eprintln!("Event processing failed: {}", e);
            }
        });

        println!("IoT data pipeline is now processing events");
        Ok(())
    }

    // Device management methods
    pub async fn register_device(
        &self,
        device_id: String,
        device_type: DeviceType,
        location: String,
        capabilities: Vec<SensorCapability>,
    ) -> Result<()> {
        println!(
            "Registering device: {} at location: {}",
            device_id, location
        );

        self.command_handler
            .handle_register_device(device_id, device_type, location, capabilities)
            .await
    }

    pub async fn connect_device(
        &self,
        device_id: String,
        ip_address: Option<String>,
    ) -> Result<()> {
        println!("Connecting device: {}", device_id);

        self.command_handler
            .handle_device_connection(device_id, ip_address)
            .await
    }

    pub async fn process_sensor_data(
        &self,
        device_id: String,
        sensor_type: SensorType,
        value: f64,
        unit: String,
        quality: DataQuality,
    ) -> Result<()> {
        println!(
            "Processing sensor data from device: {} - {}:{} {}",
            device_id,
            format!("{:?}", sensor_type),
            value,
            unit
        );

        self.command_handler
            .handle_sensor_data(device_id, sensor_type, value, unit, quality)
            .await
    }

    // Query methods
    pub async fn get_device_status(
        &self,
        device_id: &str,
    ) -> Result<Option<crate::event_store::DeviceStatus>> {
        self.event_store
            .get_device_status(device_id)
            .await
            .map_err(|e| e.into())
    }

    pub async fn get_sensor_data(
        &self,
        device_id: &str,
        sensor_type: Option<&SensorType>,
        hours_back: u32,
        limit: Option<i32>,
    ) -> Result<Vec<crate::event_store::SensorDataPoint>> {
        let end_time = Utc::now();
        let start_time = end_time - chrono::Duration::hours(hours_back as i64);

        self.event_store
            .get_sensor_data(device_id, sensor_type, start_time, end_time, limit)
            .await
            .map_err(|e| e.into())
    }

    pub async fn get_active_alerts(
        &self,
        device_id: Option<&str>,
    ) -> Result<Vec<crate::event_store::AlertInfo>> {
        self.event_store
            .get_active_alerts(device_id)
            .await
            .map_err(|e| e.into())
    }

    pub async fn get_connected_devices(&self, location: Option<&str>) -> Result<Vec<String>> {
        self.event_store
            .get_connected_devices(location)
            .await
            .map_err(|e| e.into())
    }

    pub async fn get_metrics(&self) -> crate::event_bus::EventMetrics {
        self.event_bus.get_metrics().await
    }
}

// IoT Device Simulator for testing
pub struct IoTDeviceSimulator {
    pipeline: IoTDataPipeline,
    devices: Vec<SimulatedDevice>,
}

#[derive(Debug, Clone)]
struct SimulatedDevice {
    id: String,
    device_type: DeviceType,
    location: String,
    sensors: Vec<SensorCapability>,
    is_connected: bool,
}

impl IoTDeviceSimulator {
    pub fn new(pipeline: IoTDataPipeline) -> Self {
        Self {
            pipeline,
            devices: Vec::new(),
        }
    }

    pub async fn add_device(&mut self, device_id: String, location: String) -> Result<()> {
        let device_type = DeviceType::TemperatureSensor;
        let sensors = vec![
            SensorCapability {
                sensor_type: SensorType::Temperature,
                min_value: Some(-20.0),
                max_value: Some(50.0),
                precision: Some(2),
                sampling_rate: Some(1), // 1 Hz
            },
            SensorCapability {
                sensor_type: SensorType::Humidity,
                min_value: Some(0.0),
                max_value: Some(100.0),
                precision: Some(1),
                sampling_rate: Some(1),
            },
        ];

        // Register device
        self.pipeline
            .register_device(
                device_id.clone(),
                device_type.clone(),
                location.clone(),
                sensors.clone(),
            )
            .await?;

        // Connect device
        self.pipeline
            .connect_device(device_id.clone(), Some("192.168.1.100".to_string()))
            .await?;

        let device = SimulatedDevice {
            id: device_id,
            device_type,
            location,
            sensors,
            is_connected: true,
        };

        self.devices.push(device);
        Ok(())
    }

    pub async fn simulate_sensor_data(&self) -> Result<()> {
        #[cfg(feature = "simulation")]
        {
            use rand::rngs::StdRng;
            use rand::{Rng, SeedableRng};

            let mut rng = StdRng::from_entropy();

            for device in &self.devices {
                if !device.is_connected {
                    continue;
                }

                // Simulate temperature reading
                let temperature = rng.gen_range(18.0..25.0) + rng.gen_range(-2.0..2.0);
                self.pipeline
                    .process_sensor_data(
                        device.id.clone(),
                        SensorType::Temperature,
                        temperature,
                        "°C".to_string(),
                        DataQuality::Good,
                    )
                    .await?;

                // Simulate humidity reading
                let humidity = rng.gen_range(40.0..70.0) + rng.gen_range(-5.0..5.0);
                self.pipeline
                    .process_sensor_data(
                        device.id.clone(),
                        SensorType::Humidity,
                        humidity,
                        "%".to_string(),
                        DataQuality::Good,
                    )
                    .await?;

                // Occasionally simulate out-of-range values to trigger alerts
                if rng.gen_range(0..100) < 5 {
                    let high_temp = rng.gen_range(60.0..80.0);
                    self.pipeline
                        .process_sensor_data(
                            device.id.clone(),
                            SensorType::Temperature,
                            high_temp,
                            "°C".to_string(),
                            DataQuality::Good,
                        )
                        .await?;
                }
            }
        }

        #[cfg(not(feature = "simulation"))]
        {
            eprintln!("Simulation feature not enabled. Enable with --features simulation");
        }

        Ok(())
    }

    pub async fn run_simulation(&self, duration_seconds: u64, interval_seconds: u64) -> Result<()> {
        println!(
            "Starting IoT device simulation for {} seconds",
            duration_seconds
        );

        let end_time = std::time::Instant::now() + std::time::Duration::from_secs(duration_seconds);

        while std::time::Instant::now() < end_time {
            if let Err(e) = self.simulate_sensor_data().await {
                eprintln!("Error during simulation: {}", e);
            }

            sleep(Duration::from_secs(interval_seconds)).await;
        }

        println!("IoT device simulation completed");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting IoT Data Pipeline...");

    // Database URL (should be from environment variable in production)
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:password@localhost/iotflow".to_string());

    // Initialize the IoT data pipeline
    let pipeline = IoTDataPipeline::new(&database_url).await?;

    // Start event processing
    pipeline.start_processing().await?;

    // Create device simulator
    let mut simulator = IoTDeviceSimulator::new(pipeline.clone());

    // Add some simulated devices
    simulator
        .add_device(
            "temp-sensor-01".to_string(),
            "Building A - Floor 1".to_string(),
        )
        .await?;
    simulator
        .add_device(
            "temp-sensor-02".to_string(),
            "Building A - Floor 2".to_string(),
        )
        .await?;
    simulator
        .add_device(
            "temp-sensor-03".to_string(),
            "Building B - Floor 1".to_string(),
        )
        .await?;

    println!("Added {} simulated devices", simulator.devices.len());

    // Start simulation in background
    let simulation_pipeline = pipeline.clone();
    let simulation_handle = tokio::spawn(async move {
        let simulator = IoTDeviceSimulator::new(simulation_pipeline);
        if let Err(e) = simulator.run_simulation(300, 5).await {
            // 5 minutes, every 5 seconds
            eprintln!("Simulation error: {}", e);
        }
    });

    // Monitor pipeline metrics
    let metrics_pipeline = pipeline.clone();
    let metrics_handle = tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(30)).await;
            let metrics = metrics_pipeline.get_metrics().await;
            println!(
                "Pipeline Metrics - Events Published: {}, Events Processed: {}",
                metrics.total_events_published, metrics.total_events_processed
            );

            if !metrics.events_by_type.is_empty() {
                for (event_type, count) in &metrics.events_by_type {
                    println!("  {}: {} events", event_type, count);
                }
            }
        }
    });

    // Wait for simulation to complete
    if let Err(e) = simulation_handle.await {
        eprintln!("Simulation task failed: {}", e);
    }

    // Demo: Query some data
    println!("Querying device data...");

    let connected_devices = pipeline.get_connected_devices(None).await?;
    println!("Connected devices: {:?}", connected_devices);

    if !connected_devices.is_empty() {
        let device_id = &connected_devices[0];
        let sensor_data = pipeline
            .get_sensor_data(device_id, None, 1, Some(10))
            .await?;
        println!(
            "Recent sensor data for {}: {} readings",
            device_id,
            sensor_data.len()
        );

        let device_status = pipeline.get_device_status(device_id).await?;
        println!("Device status: {:?}", device_status);

        let alerts = pipeline.get_active_alerts(Some(device_id)).await?;
        println!("Active alerts for {}: {} alerts", device_id, alerts.len());
    }

    // Keep metrics monitoring running for a bit longer
    tokio::select! {
        _ = sleep(Duration::from_secs(60)) => {
            println!("Shutting down IoT data pipeline");
        }
        _ = metrics_handle => {
            println!("Metrics monitoring completed");
        }
    }

    let final_metrics = pipeline.get_metrics().await;
    println!("Final metrics: {:?}", final_metrics);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;

    #[tokio::test]
    async fn test_device_registration() {
        // This would require a test database
        // let pipeline = IoTDataPipeline::new("postgresql://test:test@localhost/test_iotflow").await.unwrap();
        // ... test implementation
    }

    #[tokio::test]
    async fn test_sensor_data_processing() {
        // Test sensor data validation and processing
        // ... test implementation
    }

    #[tokio::test]
    async fn test_alert_generation() {
        // Test alert generation for out-of-range values
        // ... test implementation
    }
}
