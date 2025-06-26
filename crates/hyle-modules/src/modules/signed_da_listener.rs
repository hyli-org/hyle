use std::collections::BTreeMap;

use anyhow::Result;
use sdk::{BlockHeight, DataEvent, Hashed, SignedBlock};
use tokio::task::yield_now;
use tracing::{debug, info, warn};

use crate::{
    bus::{BusClientSender, SharedMessageBus},
    modules::{da_listener::DAListenerConf, module_bus_client, Module},
    utils::da_codec::{DataAvailabilityClient, DataAvailabilityEvent, DataAvailabilityRequest},
};
use crate::{log_error, module_handle_messages};

module_bus_client! {
#[derive(Debug)]
struct SignedDAListenerBusClient {
    sender(DataEvent),
}
}

/// Module that listens to the raw data availability stream and sends the signed blocks to the bus
pub struct SignedDAListener {
    config: DAListenerConf,
    bus: SignedDAListenerBusClient,
    current_block: BlockHeight,
    block_buffer: BTreeMap<BlockHeight, SignedBlock>,
}

impl Module for SignedDAListener {
    type Context = DAListenerConf;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let current_block = ctx.start_block.unwrap_or_default();

        let bus = SignedDAListenerBusClient::new_from_bus(bus.new_handle()).await;

        Ok(SignedDAListener {
            config: ctx,
            current_block,
            bus,
            block_buffer: BTreeMap::new(),
        })
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }
}

impl SignedDAListener {
    async fn start_client(&self, block_height: BlockHeight) -> Result<DataAvailabilityClient> {
        let mut client = DataAvailabilityClient::connect_with_opts(
            "signed_da_listener".to_string(),
            Some(1024 * 1024 * 1024),
            self.config.da_read_from.clone(),
        )
        .await?;

        client.send(DataAvailabilityRequest(block_height)).await?;

        Ok(client)
    }

    async fn process_block(&mut self, block: SignedBlock) -> Result<()> {
        let block_height = block.height();

        // If this is the next block we expect, process it immediately, otherwise buffer it
        match block_height.cmp(&self.current_block) {
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
                        "📦 Sending block: {} {}",
                        block.consensus_proposal.slot,
                        block.consensus_proposal.hashed()
                    );
                } else {
                    debug!(
                        "📦 Sending block: {} {}",
                        block.consensus_proposal.slot,
                        block.consensus_proposal.hashed()
                    );
                }
                self.bus
                    .send_waiting_if_full(DataEvent::OrderedSignedBlock(block))
                    .await?;

                self.current_block = block_height + 1;

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
            if *height > self.current_block {
                return Ok(());
            }
        }

        while let Some((height, block)) = self.block_buffer.pop_first() {
            if height == self.current_block {
                debug!(
                    "📦 Processing buffered block: {} {}",
                    block.consensus_proposal.slot,
                    block.consensus_proposal.hashed()
                );
                self.bus
                    .send_waiting_if_full(DataEvent::OrderedSignedBlock(block))
                    .await?;
                self.current_block = height + 1;
            } else {
                // In general, DA isn't guaranteed to send blocks in order.
                info!(
                    "📦 Wanted block {}, next buffered is {}, waiting.",
                    self.current_block,
                    block.height(),
                );
                // Put the block back
                _ = self.block_buffer.insert(height, block);
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
            let mut client = self.start_client(self.current_block).await?;

            module_handle_messages! {
                on_self self,
                frame = client.recv() => {
                    if let Some(streamed_signed_block) = frame {
                        let _ = log_error!(self.processing_next_frame(streamed_signed_block).await, "Consuming da stream");
                        client.ping().await?;
                    } else {
                        warn!("Data availability stream ended, restarting client from block {}", self.current_block);
                        client = self.start_client(self.current_block).await?;
                    }
                }
            };
        }
        Ok(())
    }

    async fn processing_next_frame(&mut self, event: DataAvailabilityEvent) -> Result<()> {
        if let DataAvailabilityEvent::SignedBlock(block) = event {
            self.process_block(block).await?;
        }

        Ok(())
    }
}
