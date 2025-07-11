use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::Result;
use sdk::{BlockHeight, Hashed, MempoolStatusEvent, SignedBlock};
use tokio::task::yield_now;
use tracing::{debug, error, info, trace, warn};

use crate::{
    bus::{BusClientSender, SharedMessageBus},
    modules::{module_bus_client, Module},
    node_state::{metrics::NodeStateMetrics, module::NodeStateEvent, NodeState, NodeStateStore},
    utils::da_codec::{DataAvailabilityClient, DataAvailabilityEvent, DataAvailabilityRequest},
};
use crate::{log_error, module_handle_messages};

module_bus_client! {
#[derive(Debug)]
struct DAListenerBusClient {
    sender(NodeStateEvent),
    sender(MempoolStatusEvent),
}
}

/// Module that listens to the data availability stream and sends the blocks to the bus
pub struct DAListener {
    config: DAListenerConf,
    bus: DAListenerBusClient,
    node_state: NodeState,
    start_block: BlockHeight,
    block_buffer: BTreeMap<BlockHeight, SignedBlock>,
}

pub struct DAListenerConf {
    pub data_directory: PathBuf,
    pub da_read_from: String,
    pub start_block: Option<BlockHeight>,
    pub timeout_client_secs: u64,
}

impl Module for DAListener {
    type Context = DAListenerConf;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let node_state_store = Self::load_from_disk_or_default::<NodeStateStore>(
            ctx.data_directory
                .join("da_listener_node_state.bin")
                .as_path(),
        );
        if node_state_store.current_height != BlockHeight(0) {
            info!(
                "📂 Loaded node state from {} at height {}",
                ctx.data_directory
                    .join("da_listener_node_state.bin")
                    .display(),
                node_state_store.current_height
            );
        }

        let node_state = NodeState {
            store: node_state_store,
            metrics: NodeStateMetrics::global("da_listener".to_string(), "da_listener"),
        };

        let start_block = ctx.start_block.unwrap_or(
            // Annoying edge case: on startup this will be 0, but we do want to process block 0.
            // Otherwise, we've already processed the block so we don't actually need that.
            match node_state.current_height {
                BlockHeight(0) => BlockHeight(0),
                _ => node_state.current_height + 1,
            },
        );

        let bus = DAListenerBusClient::new_from_bus(bus.new_handle()).await;

        for name in node_state.contracts.keys() {
            info!("📝 Loaded contract state for {}", name);
        }

        Ok(DAListener {
            config: ctx,
            start_block,
            bus,
            node_state,
            block_buffer: BTreeMap::new(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await
    }

    async fn persist(&mut self) -> Result<()> {
        log_error!(
            Self::save_on_disk::<NodeStateStore>(
                self.config
                    .data_directory
                    .join("da_listener_node_state.bin")
                    .as_path(),
                &self.node_state,
            ),
            "Saving node state"
        )
    }
}

impl DAListener {
    async fn start_client(&self, block_height: BlockHeight) -> Result<DataAvailabilityClient> {
        let mut client = DataAvailabilityClient::connect_with_opts(
            "raw_da_listener".to_string(),
            Some(1024 * 1024 * 1024),
            self.config.da_read_from.clone(),
        )
        .await?;

        client.send(DataAvailabilityRequest(block_height)).await?;

        Ok(client)
    }

    async fn process_block(&mut self, block: SignedBlock) -> Result<()> {
        let block_height = block.height();

        if block_height == BlockHeight(0) && self.node_state.current_height == BlockHeight(0) {
            info!(
                "📦 Processing genesis block: {} {}",
                block.consensus_proposal.slot,
                block.consensus_proposal.hashed()
            );
            let processed_block = self.node_state.handle_signed_block(&block)?;
            self.bus
                .send_waiting_if_full(NodeStateEvent::NewBlock(Box::new(processed_block)))
                .await?;
            return Ok(());
        }

        // If this is the next block we expect, process it immediately, otherwise buffer it
        match block_height.cmp(&(self.node_state.current_height + 1)) {
            std::cmp::Ordering::Less => {
                // Block is from the past, log and ignore
                warn!(
                    "📦 Ignoring past block: {} {}",
                    block.consensus_proposal.slot,
                    block.consensus_proposal.hashed()
                );
            }
            std::cmp::Ordering::Equal => {
                if block_height.0 % 1000 == 0 {
                    info!(
                        "📦 Processing block: {} {}",
                        block.consensus_proposal.slot,
                        block.consensus_proposal.hashed()
                    );
                } else {
                    debug!(
                        "📦 Processing block: {} {}",
                        block.consensus_proposal.slot,
                        block.consensus_proposal.hashed()
                    );
                }
                let processed_block = self.node_state.handle_signed_block(&block)?;
                trace!("📦 Handled block outputs: {:?}", processed_block);
                self.bus
                    .send_waiting_if_full(NodeStateEvent::NewBlock(Box::new(processed_block)))
                    .await?;

                // Process any buffered blocks that are now in sequence
                self.process_buffered_blocks().await?;
            }
            std::cmp::Ordering::Greater => {
                // Block is from the future, buffer it
                debug!(
                    "📦 Buffering future block: {} {}",
                    block.consensus_proposal.slot,
                    block.consensus_proposal.hashed()
                );
                self.block_buffer.insert(block_height, block);
            }
        }

        Ok(())
    }

    async fn process_buffered_blocks(&mut self) -> Result<()> {
        if let Some((height, _)) = self.block_buffer.first_key_value() {
            if *height > self.node_state.current_height + 1 {
                return Ok(());
            }
        }

        while let Some((height, block)) = self.block_buffer.pop_first() {
            if height == self.node_state.current_height + 1 {
                debug!(
                    "📦 Processing buffered block: {} {}",
                    block.consensus_proposal.slot,
                    block.consensus_proposal.hashed()
                );
                let processed_block = self.node_state.handle_signed_block(&block)?;
                debug!("📦 Handled buffered block outputs: {:?}", processed_block);
                self.bus
                    .send_waiting_if_full(NodeStateEvent::NewBlock(Box::new(processed_block)))
                    .await?;
            } else {
                error!(
                    "📦 Buffered block is not in sequence: {} {}",
                    block.height(),
                    block.consensus_proposal.hashed()
                );
                if let Some(previous_block) = self.block_buffer.insert(height, block) {
                    debug!(
                        "Replaced an existing block at height {}: {:?}",
                        height,
                        previous_block.consensus_proposal.hashed()
                    );
                } else {
                    debug!("Inserted a new block at height {}", height);
                }
                break;
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        if let Some(folder) = self.config.da_read_from.strip_prefix("folder:") {
            info!("Reading blocks from folder {folder}");
            let mut blocks = vec![];
            let mut entries = std::fs::read_dir(folder)
                .unwrap_or_else(|_| std::fs::read_dir(".").unwrap())
                .filter_map(|e| e.ok())
                .collect::<Vec<_>>();
            entries.sort_by_key(|e| e.file_name());
            for entry in entries {
                let path = entry.path();
                if path.extension().map(|e| e == "bin").unwrap_or(false) {
                    if let Ok(bytes) = std::fs::read(&path) {
                        if let Ok((block, tx_count)) =
                            borsh::from_slice::<(SignedBlock, usize)>(&bytes)
                        {
                            blocks.push((block, tx_count));
                        }
                    }
                }
                yield_now().await; // Yield to allow other tasks to run
            }
            // Sort blocks by block_height (numeric order)
            blocks.sort_by_key(|b| b.0.consensus_proposal.slot);

            info!("Got {} blocks from folder. Processing...", blocks.len());
            for (block, _) in blocks {
                self.process_block(block).await?;
            }
            module_handle_messages! {
                on_self self,
            };
        } else {
            let mut client = self.start_client(self.start_block).await?;

            module_handle_messages! {
                on_self self,
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(self.config.timeout_client_secs)) => {
                    warn!("No blocks received in the last {} seconds, restarting client", self.config.timeout_client_secs);
                    client = self.start_client(self.node_state.current_height + 1).await?;
                }
                frame = client.recv() => {
                    if let Some(streamed_signed_block) = frame {
                        let _ = log_error!(self.processing_next_frame(streamed_signed_block).await, "Consuming da stream");
                        if let Err(e) = client.ping().await {
                            warn!("Ping failed: {}. Restarting client...", e);
                            client = self.start_client(self.node_state.current_height + 1).await?;
                        }
                    } else {
                        warn!("DA stream connection lost. Reconnecting...");
                        client = self.start_client(self.node_state.current_height + 1).await?;
                    }
                }
            };
        }

        Ok(())
    }

    async fn processing_next_frame(&mut self, event: DataAvailabilityEvent) -> Result<()> {
        match event {
            DataAvailabilityEvent::SignedBlock(block) => {
                self.process_block(block).await?;
            }
            DataAvailabilityEvent::MempoolStatusEvent(mempool_status_event) => {
                self.bus.send_waiting_if_full(mempool_status_event).await?;
            }
        }

        Ok(())
    }
}
