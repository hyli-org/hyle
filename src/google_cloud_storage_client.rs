// Module that send requests to Google Cloud Storage

use anyhow::{Context, Result};
use google_cloud_storage::client::Client;
use hyle_modules::{
    bus::SharedMessageBus, log_error, module_bus_client, module_handle_messages, modules::Module,
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::utils::conf::SharedConf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GCSRequest {
    ProofUpload {
        proof: Vec<u8>,
        tx_hash: hyle_model::TxHash,
        parent_data_proposal_hash: hyle_model::DataProposalHash,
    },
}

module_bus_client! {
struct GoogleCloudStorageBusClient {
    receiver(GCSRequest),
}
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GCSBucketConf {
    pub bucket: String,
    pub prefix: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GCSConf {
    pub proof: GCSBucketConf,
}

pub struct GoogleCloudStorageClient {
    config: SharedConf,
    bus: GoogleCloudStorageBusClient,
    google_cloud_storage_client: google_cloud_storage::client::Client,
}

impl Module for GoogleCloudStorageClient {
    type Context = SharedConf;
    async fn build(bus: SharedMessageBus, shared_config: Self::Context) -> Result<Self> {
        let bus = GoogleCloudStorageBusClient::new_from_bus(bus.new_handle()).await;

        let google_cloud_storage_client = Client::new(
            google_cloud_storage::client::ClientConfig::default()
                .with_auth()
                .await
                .context("Failed to create GCS client")?,
        );

        Ok(GoogleCloudStorageClient {
            config: shared_config.clone(),
            bus,
            google_cloud_storage_client,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<GCSRequest> request => {
                debug!("Received GCS request: {:?}", request);
                match request {
                    GCSRequest::ProofUpload { proof, tx_hash, parent_data_proposal_hash } => {
                        debug!("Processing proof upload of size: {}", proof.len());
                        _ = log_error!(
                            self.upload_proof(proof, tx_hash, parent_data_proposal_hash).await,
                            "Uploading proof to GCS"
                        );
                    }
                }
            }
        };

        Ok(())
    }
}

impl GoogleCloudStorageClient {
    pub async fn upload_proof(
        &self,
        proof: Vec<u8>,
        tx_hash: hyle_model::TxHash,
        parent_data_proposal_hash: hyle_model::DataProposalHash,
    ) -> Result<()> {
        // Upload to GCS if client is configured
        let object_name = format!(
            "{}/proofs/{}/{}.bin",
            self.config.gcs.proof.prefix, parent_data_proposal_hash.0, tx_hash.0
        );

        let upload_request = google_cloud_storage::http::objects::upload::UploadObjectRequest {
            bucket: self.config.gcs.proof.bucket.clone(),
            generation: Some(0), // 0 means - don't overwrite existing objects
            ..Default::default()
        };

        let media = google_cloud_storage::http::objects::upload::Media::new(object_name.clone());
        let upload_type = google_cloud_storage::http::objects::upload::UploadType::Simple(media);

        self.google_cloud_storage_client
            .upload_object(&upload_request, proof, &upload_type)
            .await
            .context(format!("Failed to upload proof {tx_hash} to GCS"))?;

        Ok(())
    }
}
