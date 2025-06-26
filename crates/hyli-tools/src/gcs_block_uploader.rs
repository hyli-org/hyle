use std::path::PathBuf;

use anyhow::Result;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest};
use hyle_model::DataEvent;
use hyle_modules::{
    bus::SharedMessageBus,
    modules::{Module, module_bus_client},
};
use hyle_modules::{log_error, module_handle_messages};
use serde::{Deserialize, Serialize};

module_bus_client! {
    #[derive(Debug)]
    struct GcsUploaderBusClient {
        receiver(DataEvent),
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Conf {
    /// The log format to use - "json", "node" or "full" (default)
    pub log_format: String,

    /// Directory name to store node state.
    pub data_directory: PathBuf,

    /// URL to connect to.
    pub da_read_from: String,

    // GCS uploader options
    pub gcs_bucket: String,
    pub gcs_prefix: String,
}

impl Conf {
    pub fn new(config_files: Vec<String>) -> Result<Self, anyhow::Error> {
        let mut s = config::Config::builder().add_source(config::File::from_str(
            include_str!("gcs_conf_defaults.toml"),
            config::FileFormat::Toml,
        ));
        // Priority order: config file, then environment variables, then CLI
        for config_file in config_files {
            s = s.add_source(config::File::with_name(&config_file).required(false));
        }
        let conf: Self = s
            .add_source(
                config::Environment::with_prefix("hyle")
                    .separator("__")
                    .prefix_separator("_"),
            )
            .build()?
            .try_deserialize()?;
        Ok(conf)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GcsBlockUploaderCtx {
    pub config: Conf,
}

pub struct GcsBlockUploader {
    config: Conf,
    bus: GcsUploaderBusClient,
    gcs_client: Client,
    testnet_genesis_timestamp: Option<u128>,
}

impl Module for GcsBlockUploader {
    type Context = GcsBlockUploaderCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = GcsUploaderBusClient::new_from_bus(bus.new_handle()).await;
        let config = ClientConfig::default().with_auth().await.unwrap();
        let gcs_client = Client::new(config);
        let testnet_genesis_timestamp: Option<u128> =
            Self::load_from_disk_or_default(&ctx.config.data_directory.join("gcs_uploader.bin"));
        Ok(GcsBlockUploader {
            config: ctx.config,
            bus,
            gcs_client,
            testnet_genesis_timestamp,
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await?;
        Ok(())
    }

    async fn persist(&mut self) -> Result<()> {
        log_error!(
            Self::save_on_disk(
                &self.config.data_directory.join("gcs_uploader.bin"),
                &self.testnet_genesis_timestamp,
            ),
            "Persisting GCS uploader state"
        )
    }
}

impl GcsBlockUploader {
    pub async fn start(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<DataEvent> event => {
                self.handle_data_availability_event(event).await?;
            }
        };
        Ok(())
    }

    async fn handle_data_availability_event(&mut self, event: DataEvent) -> Result<()> {
        let DataEvent::OrderedSignedBlock(block) = event;
        let block_height = block.height().0;
        let block_timestamp = block.consensus_proposal.timestamp.0;
        if block_height == 0 {
            self.testnet_genesis_timestamp = Some(block_timestamp);
            tracing::info!("Testnet genesis timestamp set to {}", block_timestamp);
        }

        let prefix = &self.config.gcs_prefix;
        let object_name = format!(
            "{}/{}/block_{}.bin",
            prefix,
            self.testnet_genesis_timestamp.expect("must be set"),
            block_height
        );
        let data = borsh::to_vec(&block)?;
        let req = UploadObjectRequest {
            bucket: self.config.gcs_bucket.clone(),
            generation: Some(0), // 0 means - don't overwrite existing objects
            ..Default::default()
        };
        let media = Media::new(object_name.clone());
        let upload_type = google_cloud_storage::http::objects::upload::UploadType::Simple(media);
        // Log, but ignore errors - could be that we already dumped this, or some other thing - we'll do our best to store everything.
        match self
            .gcs_client
            .upload_object(&req, data, &upload_type)
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "Successfully uploaded block {} to GCS bucket {}",
                    block_height,
                    self.config.gcs_bucket
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to upload block {} to GCS bucket {}: {}",
                    block_height,
                    self.config.gcs_bucket,
                    e
                );
            }
        }
        Ok(())
    }
}
