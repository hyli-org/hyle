use anyhow::{Context, Result};
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
use hyle_model::{
    BlobTransaction, ContractAction, HyleOutput, Identity, NodeStateEvent, TxHash,
    verifiers::Secp256k1Blob,
};
use hyle_modules::{
    bus::SharedMessageBus, module_bus_client, modules::Module, node_state::NukeTxAction,
};
use secp256k1::{Message, PublicKey, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::{collections::BTreeMap, path::PathBuf};

module_bus_client! {
    #[derive(Debug)]
    struct NukeTxBusClient {
        receiver(NodeStateEvent),
    }
}
pub struct NukeTxModuleCtx {
    pub config: Conf,
    pub txs: BTreeMap<TxHash, Vec<HyleOutput>>,
}

pub struct NukeTxModule {
    _bus: NukeTxBusClient,
    remaining_txs: BTreeMap<TxHash, Vec<HyleOutput>>,
    node_client: NodeApiHttpClient,
    secret_key: SecretKey,
}

impl Module for NukeTxModule {
    type Context = NukeTxModuleCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = NukeTxBusClient::new_from_bus(bus.new_handle()).await;
        let secret_key_bytes =
            hex::decode(&ctx.config.secret_key).context("Failed to decode secret key hex")?;
        let secret_key = SecretKey::from_byte_array(secret_key_bytes.try_into().expect("32 bytes"))
            .expect("32 bytes, within curve order");

        Ok(NukeTxModule {
            _bus: bus,
            node_client: NodeApiHttpClient::new(ctx.config.node_url)?,
            secret_key,
            remaining_txs: ctx.txs.clone(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await?;
        Ok(())
    }
}

impl NukeTxModule {
    async fn start(&mut self) -> Result<()> {
        self.create_and_send_nuke_tx().await?;
        Ok(())
    }

    fn sign_data(&self, expected_data: &[u8]) -> ([u8; 32], [u8; 64]) {
        let mut hasher = sha2::Sha256::new();
        hasher.update(expected_data);
        let data_hash = hasher.finalize().into();
        let message = Message::from_digest(data_hash);
        let mut signature = self.secret_key.sign_ecdsa(message);
        signature.normalize_s();

        (data_hash, signature.serialize_compact())
    }

    async fn create_and_send_nuke_tx(&mut self) -> Result<()> {
        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &self.secret_key);

        // Create secp256k1 blob that signs the transaction hash data
        let expected_data =
            borsh::to_vec(&self.remaining_txs).context("Failed to serialize tx_hashes")?;
        let (data_hash, signature) = self.sign_data(&expected_data);

        // Create the NukeTxAction
        let nuke_action = NukeTxAction {
            txs: std::mem::take(&mut self.remaining_txs),
        };

        // Create the nuke blob
        let nuke_blob = nuke_action.as_blob("hyle".into(), None, None);

        let secp_blob_data = Secp256k1Blob {
            identity: Identity::new("hyle@hyle"),
            data: data_hash,
            public_key: public_key.serialize(),
            signature,
        };

        let secp_blob = secp_blob_data.as_blob();

        // Create and send the nuke transaction
        let nuke_tx = BlobTransaction::new(Identity::new("hyle@hyle"), vec![secp_blob, nuke_blob]);
        let tx_hash = self
            .node_client
            .send_tx_blob(nuke_tx)
            .await
            .context("Failed to send nuke transaction")?;

        tracing::info!("✅ Nuke transaction sent successfully for block!");
        tracing::info!("Transaction hash: {}", tx_hash);

        Ok(())
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

    pub node_url: String,
    pub secret_key: String,
}

impl Conf {
    pub fn new(config_files: Vec<String>) -> Result<Self, anyhow::Error> {
        let mut s = config::Config::builder().add_source(config::File::from_str(
            include_str!("nuke_tx_conf_defaults.toml"),
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
