use anyhow::{Context, Result};
use clap::{Parser, command};
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
use hyle_model::DataEvent;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

use hyle_contract_sdk::BlockHeight;
use hyle_modules::module_handle_messages;
use hyle_modules::modules::{Module, module_bus_client};
use hyle_modules::{
    bus::{SharedMessageBus, metrics::BusMetrics},
    modules::{ModulesHandler, da_listener::DAListenerConf, signed_da_listener::SignedDAListener},
    node_state::{NodeState, metrics::NodeStateMetrics},
    utils::logger::setup_tracing,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "config.toml")]
    pub config_file: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Conf::new(args.config_file).context("reading config file")?;

    setup_tracing(&config.log_format, "node state check".to_string())?;

    tracing::info!("Starting Node State Check with config: {:?}", config);

    let bus = SharedMessageBus::new(BusMetrics::global("node_state_check".to_string()));

    tracing::info!("Setting up modules");

    // Initialize modules
    let mut handler = ModulesHandler::new(&bus).await;

    handler
        .build_module::<SignedDAListener>(DAListenerConf {
            data_directory: config.data_directory.clone(),
            da_read_from: config.da_read_from.clone(),
            start_block: Some(BlockHeight(0)),
            timeout_client_secs: 10,
        })
        .await?;

    handler
        .build_module::<NodeStateCheck>(Ctx {
            config: config.clone(),
        })
        .await?;

    tracing::info!("Starting modules");

    // Run forever
    handler.start_modules().await?;
    handler.exit_process().await?;

    Ok(())
}

module_bus_client! {
    #[derive(Debug)]
    struct BlockReceiverBusClient {
        receiver(DataEvent),
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
struct Conf {
    /// The log format to use - "json", "node" or "full" (default)
    pub log_format: String,

    /// Directory name to store node state.
    pub data_directory: PathBuf,

    /// URL to connect to.
    pub da_read_from: String,

    /// URL to connect to the REST API for node state checks. Optional
    pub rest_api: Option<String>,

    /// Last block to process. If none, will process to current height (or everything if that fails).
    pub read_to: Option<BlockHeight>,

    /// Expected hash of the node state after processing. Optional.
    pub expected_hash: Option<String>,
}

impl Conf {
    pub fn new(config_files: Vec<String>) -> Result<Self, anyhow::Error> {
        let mut s = config::Config::builder().add_source(config::File::from_str(
            r#"
            log_format = "full"
            data_directory = "data_nsc"
            da_read_from = "localhost:4141"
            "#,
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
struct Ctx {
    config: Conf,
}

struct NodeStateCheck {
    config: Conf,
    bus: BlockReceiverBusClient,
    read_to: Option<BlockHeight>,
}

impl Module for NodeStateCheck {
    type Context = Ctx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = BlockReceiverBusClient::new_from_bus(bus.new_handle()).await;
        let mut read_to = ctx.config.read_to;
        if let Some(api_url) = &ctx.config.rest_api {
            let d = NodeApiHttpClient::new(api_url.clone())?;
            read_to = Some(d.get_block_height().await?);
        };
        Ok(NodeStateCheck {
            config: ctx.config,
            bus,
            read_to,
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await?;
        Ok(())
    }
}

impl NodeStateCheck {
    pub async fn start(&mut self) -> Result<()> {
        let mut node_state = NodeState {
            metrics: NodeStateMetrics::global("node_state_check".to_string(), "node_state_check"),
            store: Default::default(),
        };
        module_handle_messages! {
            on_self self,
            listen<DataEvent> event => {
                let DataEvent::OrderedSignedBlock(block) = event;
                let proc = node_state.handle_signed_block(&block)?;
                if let Some(max) = self.read_to {
                    if proc.block_height >= max {
                        tracing::info!("Reached read_to block height: {}", max);
                        break;
                    }
                }
            }
        };
        let mut hasher = Sha256::new();
        borsh::to_writer(&mut hasher, &node_state.store)?;
        let hash_result = hasher.finalize();
        tracing::info!("Node state SHA256: {:x}", hash_result);
        if let Some(expected_hash) = &self.config.expected_hash {
            if expected_hash != &format!("{:x}", hash_result) {
                tracing::error!(
                    "Node state hash mismatch! Expected: {}, Got: {:x}",
                    expected_hash,
                    hash_result
                );
                return Err(anyhow::anyhow!("Node state hash mismatch"));
            } else {
                tracing::info!("Node state hash matches expected value.");
            }
        }
        Ok(())
    }
}
