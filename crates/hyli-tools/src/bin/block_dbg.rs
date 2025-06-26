use anyhow::{Context, Result};
use clap::{Parser, command};
use client_sdk::{
    contract_indexer::utoipa::OpenApi, helpers::test::TxExecutorTestProver,
    rest_client::test::NodeApiMockClient,
};
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute, terminal,
};
use hyle_contract_sdk::{Block, NodeStateEvent, TransactionData, TxId, api::NodeInfo};
use hyle_contract_sdk::{BlockHeight, SignedBlock};
use hyle_model::DataEvent;
use hyle_modules::modules::{
    da_listener::DAListenerConf,
    prover::{AutoProver, AutoProverCtx},
    signed_da_listener::SignedDAListener,
};
use hyle_modules::{
    bus::{SharedMessageBus, metrics::BusMetrics},
    module_bus_client, module_handle_messages,
    modules::{
        BuildApiContextInner, Module, ModulesHandler,
        contract_state_indexer::{ContractStateIndexer, ContractStateIndexerCtx},
        rest::{ApiDoc, RestApi, RestApiRunContext, Router},
    },
    node_state::NodeState,
};
use ratatui::{
    prelude::*,
    widgets::{Block as TuiBlock, *},
};
use smt_token::{
    SmtTokenContract, account::AccountSMT, client::tx_executor_handler::SmtTokenProvableState,
};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::io::{self};
use std::time::Duration;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::time::MissedTickBehavior;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Folder to load/dump blocks
    #[arg(long, default_value = "dump")]
    pub folder: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    Sequenced,
    Success,
    Failed,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FocusPanel {
    BlockList,
    TxList,
    BlockJson,
    Logs,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let dump_folder = PathBuf::from(&args.folder);

    // Set up tracing to print to a buffer and a file

    // Create a buffer to hold logs
    let log_buffer: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let log_buffer_clone = log_buffer.clone();

    // Open log file, truncate it if it exists
    use std::fs::OpenOptions;
    let log_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open("log.log")?;

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?;
    let filter2 = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?;

    // Buffer writer for TUI
    struct BufferWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    use tracing_subscriber::fmt::MakeWriter;
    impl<'a> MakeWriter<'a> for BufferWriter {
        type Writer = BufferWriter;

        fn make_writer(&'a self) -> Self::Writer {
            BufferWriter {
                buffer: self.buffer.clone(),
            }
        }
    }

    impl Write for BufferWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let buffer_writer = BufferWriter {
        buffer: log_buffer_clone.clone(),
    };
    let file_writer = log_file;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .compact()
                .with_writer(buffer_writer)
                .with_filter(filter),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .compact()
                .with_writer(file_writer)
                .with_filter(filter2),
        )
        .init();

    tracing::info!("Starting block debugger");

    let bus = SharedMessageBus::new(BusMetrics::global("debug".to_string()));

    tracing::info!("Setting up modules");

    // Check if there are any blocks in the folder
    let has_blocks = std::fs::read_dir(&dump_folder)
        .map(|entries| {
            entries.filter_map(|e| e.ok()).any(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|e| e == "bin")
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);

    // Initialize modules
    let mut handler = ModulesHandler::new(&bus).await;

    if !has_blocks {
        handler
            .build_module::<SignedDAListener>(DAListenerConf {
                data_directory: PathBuf::from("data"),
                da_read_from: "localhost:4141".to_string(),
                start_block: Some(BlockHeight(0)),
                timeout_client_secs: 10,
            })
            .await?;
    } else {
        tracing::info!("Blocks found in folder, skipping DAListener startup");
    }

    handler
        .build_module::<BlockDbg>((log_buffer, dump_folder.clone()))
        .await?;

    // As an example/feature, run a prover alongside the block debugger.
    #[allow(dead_code)]
    if false {
        let prover = TxExecutorTestProver::<SmtTokenContract>::new();
        let node_client = NodeApiMockClient::new();
        node_client.set_block_height(BlockHeight(4000));
        handler
            .build_module::<AutoProver<SmtTokenProvableState>>(Arc::new(AutoProverCtx {
                data_directory: PathBuf::from("data"),
                prover: Arc::new(prover),
                contract_name: "oranj".into(),
                node: Arc::new(node_client),
                api: None,
                default_state: Default::default(),
                buffer_blocks: 0,
                max_txs_per_proof: 40,
                tx_working_window_size: 180,
            }))
            .await?;

        let build_api_ctx = Arc::new(BuildApiContextInner {
            router: Mutex::new(Some(Router::new())),
            openapi: Mutex::new(ApiDoc::openapi()),
        });

        handler
            .build_module::<ContractStateIndexer<AccountSMT>>(ContractStateIndexerCtx {
                contract_name: "oranj".into(),
                data_directory: dump_folder.clone(),
                api: build_api_ctx.clone(),
            })
            .await?;

        // Should come last so the other modules have nested their own routes.
        let router = build_api_ctx
            .router
            .lock()
            .expect("Context router should be available.")
            .take()
            .expect("Context router should be available.");
        let openapi = build_api_ctx
            .openapi
            .lock()
            .expect("OpenAPI should be available")
            .clone();

        handler
            .build_module::<RestApi>(
                RestApiRunContext::new(
                    4322,
                    NodeInfo {
                        id: "block_dbg".to_string(),
                        pubkey: None,
                        da_address: "localhost:4141".to_string(),
                    },
                    router.clone(),
                    10_000_000,
                    openapi,
                ), //.with_registry(registry),
            )
            .await?;
    }

    tracing::info!("Starting modules");

    // Run forever
    handler.start_modules().await?;
    handler.exit_process().await?;

    Ok(())
}

module_bus_client! {
struct DumpBusClient {
    sender(NodeStateEvent),
    receiver(DataEvent),
}
}
// Purpose: dump all blocks to a file in a specific folder
struct BlockDbg {
    bus: DumpBusClient,
    outfolder: PathBuf,
    log_buffer: Arc<Mutex<Vec<u8>>>,
}

struct DumpUiState {
    blocks: Vec<(SignedBlock, usize)>, // (block, nb tx)
    selected: usize,
    should_quit: bool,
    redraw: bool,
    scroll_padding: usize,
    command_mode: bool,
    command_input: String,
    command_message: Option<String>,
    log_buffer: Arc<Mutex<Vec<u8>>>,
    block_json_scroll: u16,
    log_scroll: u16,
    focused_panel: FocusPanel,

    // Node state and transaction statuses
    processing: bool,
    processed_height: Option<u64>,
    node_state: Option<NodeState>,
    tx_status: HashMap<TxId, TxStatus>,
    processed_blocks: HashMap<u64, Block>,
}

impl Module for BlockDbg {
    type Context = (Arc<Mutex<Vec<u8>>>, PathBuf);
    fn build(
        bus: SharedMessageBus,
        ctx: Self::Context,
    ) -> impl Future<Output = Result<Self>> + Send {
        BlockDbg::new(bus.new_handle(), ctx)
    }
    fn run(&mut self) -> impl Future<Output = Result<()>> + Send {
        self.start()
    }
}

impl BlockDbg {
    async fn new(bus: SharedMessageBus, ctx: (Arc<Mutex<Vec<u8>>>, PathBuf)) -> Result<Self> {
        let (log_buffer, outfolder) = ctx;
        Ok(Self {
            bus: DumpBusClient::new_from_bus(bus).await,
            outfolder,
            log_buffer,
        })
    }

    async fn start(&mut self) -> Result<()> {
        // Load all blocks from dump folder at startup
        let mut blocks = vec![];
        let mut entries = fs::read_dir(&self.outfolder)
            .unwrap_or_else(|_| fs::read_dir(".").unwrap())
            .filter_map(|e| e.ok())
            .collect::<Vec<_>>();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            if path.extension().map(|e| e == "bin").unwrap_or(false) {
                if let Ok(bytes) = fs::read(&path) {
                    if let Ok((block, tx_count)) = borsh::from_slice::<(SignedBlock, usize)>(&bytes)
                    {
                        blocks.push((block, tx_count));
                    }
                }
            }
        }
        // Sort blocks by block_height (numeric order)
        blocks.sort_by_key(|b| b.0.consensus_proposal.slot);

        let mut ui_state = DumpUiState {
            blocks,
            selected: 0,
            should_quit: false,
            redraw: true,
            scroll_padding: 10,
            command_mode: false,
            command_input: String::new(),
            command_message: None,
            log_buffer: self.log_buffer.clone(),
            processing: false,
            processed_height: None,
            node_state: None,
            tx_status: HashMap::new(),
            processed_blocks: HashMap::new(),
            block_json_scroll: 0,
            log_scroll: 0,
            focused_panel: FocusPanel::BlockList,
        };

        use ratatui::Terminal;
        use ratatui::backend::CrosstermBackend;

        terminal::enable_raw_mode()?;
        execute!(io::stdout(), terminal::EnterAlternateScreen)?;

        let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
        let mut interval = tokio::time::interval(Duration::from_millis(400));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        module_handle_messages! {
            on_self self,
            listen<DataEvent> event => {
                match event {
                    DataEvent::OrderedSignedBlock(block) => {
                        tracing::info!("Received block: {:?}, saving it to {:?}", block, self.outfolder);
                        let txs = block.count_txs();
                        ui_state.blocks.push((block.clone(), txs));
                        // Dump to
                        let block_path = self.outfolder.join(format!("block_{}.bin", block.height().0));
                        std::fs::create_dir_all(&self.outfolder)
                            .context("Failed to create output directory")?;
                        let mut file = std::fs::File::create(&block_path)
                            .context("Failed to create block file")?;
                        borsh::to_writer(&mut file, &(block, txs))
                            .context("Failed to serialize block")?;
                    }
                }
            },
            _ = interval.tick() => {
                if ui_state.should_quit {
                    terminal::disable_raw_mode()?;
                    execute!(io::stdout(), terminal::LeaveAlternateScreen)?;
                    return Ok(());
                }
                if ui_state.redraw {
                    Self::render_tui(&mut terminal, &ui_state)?;
                    ui_state.redraw = false;
                }
            }
            Ok(true) = async { event::poll(Duration::from_secs(0)) } => {
                if let Event::Key(key) = event::read()? {
                    if ui_state.command_mode {
                        match key.code {
                            KeyCode::Esc => {
                                ui_state.command_mode = false;
                                ui_state.command_input.clear();
                                ui_state.redraw = true;
                            }
                            KeyCode::Enter => {
                                // Parse and execute command
                                Self::handle_command(&mut ui_state);
                            }
                            KeyCode::Backspace => {
                                ui_state.command_input.pop();
                                ui_state.redraw = true;
                            }
                            KeyCode::Char(c) => {
                                tracing::info!("Char: {}", c);
                                ui_state.command_input.push(c);
                                ui_state.redraw = true;
                            }
                            _ => {}
                        }
                    } else {
                        match key.code {
                            KeyCode::Char(':') => {
                                ui_state.command_mode = true;
                                ui_state.command_input.clear();
                                ui_state.command_message = None;
                                ui_state.redraw = true;
                                tracing::info!("Command mode");
                            }
                            KeyCode::Char('q') => { ui_state.should_quit = true; },
                            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => { ui_state.should_quit = true; },
                            KeyCode::Tab => {
                                ui_state.focused_panel = match ui_state.focused_panel {
                                    FocusPanel::BlockList => FocusPanel::TxList,
                                    FocusPanel::TxList => FocusPanel::BlockJson,
                                    FocusPanel::BlockJson => FocusPanel::Logs,
                                    FocusPanel::Logs => FocusPanel::BlockList,
                                };
                                ui_state.redraw = true;
                            }
                            KeyCode::Down | KeyCode::Char('j') => {
                                match ui_state.focused_panel {
                                    FocusPanel::BlockList => {
                                        let display_blocks: Vec<_> = ui_state.blocks.iter().filter(|(_, tx_count)| *tx_count > 0).collect();
                                        if ui_state.selected + 1 < display_blocks.len() {
                                            ui_state.selected += 1;
                                        }
                                    }
                                    FocusPanel::BlockJson => {
                                        ui_state.block_json_scroll = ui_state.block_json_scroll.saturating_add(1);
                                    }
                                    FocusPanel::Logs => {
                                        ui_state.log_scroll = ui_state.log_scroll.saturating_add(1);
                                    }
                                    _ => {}
                                }
                                ui_state.redraw = true;
                            }
                            KeyCode::Up | KeyCode::Char('k') => {
                                match ui_state.focused_panel {
                                    FocusPanel::BlockList => {
                                        if ui_state.selected > 0 {
                                            ui_state.selected -= 1;
                                        }
                                    }
                                    FocusPanel::BlockJson => {
                                        ui_state.block_json_scroll = ui_state.block_json_scroll.saturating_sub(1);
                                    }
                                    FocusPanel::Logs => {
                                        ui_state.log_scroll = ui_state.log_scroll.saturating_sub(1);
                                    }
                                    _ => {}
                                }
                                ui_state.redraw = true;
                            }
                            KeyCode::Char(' ') => {
                                if !ui_state.processing {
                                    ui_state.processing = true;
                                    ui_state.processed_height = None;
                                    ui_state.redraw = true;

                                    // Clean up previous state
                                    ui_state.node_state = None;
                                    ui_state.tx_status.clear();
                                    ui_state.processed_blocks.clear();

                                    // Get blocks up to selected one
                                    let display_blocks: Vec<_> = ui_state.blocks.iter()
                                        .filter(|(_, tx_count)| *tx_count > 0)
                                        .collect();
                                    if let Some(selected_block) = display_blocks.get(ui_state.selected) {
                                        let target_height = selected_block.0.consensus_proposal.slot;
                                        tracing::info!("Processing blocks up to height {}", target_height);

                                        // Process blocks in order
                                        ui_state.node_state = Some(NodeState::create("block_dbg".to_string(), "block_dbg"));
                                        ui_state.tx_status.clear();
                                        let outputs = ui_state.blocks.iter().filter_map(|(block, _)|
                                                if block.consensus_proposal.slot <= target_height {
                                                    tracing::info!("Processing block {}", block.consensus_proposal.slot);
                                                    ui_state.node_state.as_mut().unwrap().handle_signed_block(block).ok()
                                                } else {
                                                    None
                                                }
                                            ).collect::<Vec<_>>();
                                        for block in outputs {
                                            ui_state.processed_height = Some(block.block_height.0);
                                            ui_state.process_block_outputs(&block);
                                            // Activate if you want provers on.
                                            // self.bus.send(NodeStateEvent::NewBlock(Box::new(block.clone())))?;
                                            ui_state.processed_blocks.insert(block.block_height.0, block);
                                            ui_state.redraw = true;
                                        }
                                    }
                                    // Dump state of node state to a file
                                    if let Some(node_state) = &ui_state.node_state {
                                        let mut file = std::fs::File::create("node_state.log")?;
                                        file.write_all(format!("{:#?}", node_state).as_bytes())?;
                                        tracing::info!("Node state dumped to {:?}", "node_state.log");
                                    }
                                    ui_state.processing = false;
                                    ui_state.redraw = true;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                if ui_state.redraw {
                    Self::render_tui(&mut terminal, &ui_state)?;
                    ui_state.redraw = false;
                }
            }
        };
        // Cleanup
        terminal::disable_raw_mode()?;
        execute!(io::stdout(), terminal::LeaveAlternateScreen)?;

        Ok(())
    }

    fn render_tui(
        terminal: &mut ratatui::Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>,
        ui_state: &DumpUiState,
    ) -> anyhow::Result<()> {
        let blocks = &ui_state.blocks;
        let selected = ui_state.selected;
        let scroll_padding = ui_state.scroll_padding;
        // Only display blocks with at least one data proposal
        let display_blocks: Vec<_> = blocks
            .iter()
            .filter(|(_, tx_count)| *tx_count > 0)
            .collect();
        terminal.draw(|f| {
            let size = f.area();
            // First split horizontally between block list and right panel
            let main_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
                .split(size);

            // Split right panel vertically between TXs and logs
            let right_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(20),
                    Constraint::Percentage(30),
                    Constraint::Percentage(50),
                ])
                .split(main_chunks[1]);

            // Block list (left panel)
            let items: Vec<ListItem> = display_blocks
                .iter()
                .map(|(b, tx_count)| {
                    let content = format!("Block {} ({} txs)", b.consensus_proposal.slot, tx_count);
                    ListItem::new(content)
                })
                .collect();
            let mut state = ListState::default();
            state.select(Some(selected.min(items.len().saturating_sub(1))));
            let block_title = if ui_state.processing {
                if let Some(height) = ui_state.processed_height {
                    format!("Blocks (Processing... {})", height)
                } else {
                    "Blocks (Processing...)".to_string()
                }
            } else if let Some(height) = ui_state.processed_height {
                format!("Blocks (Processed up to {})", height)
            } else {
                format!("Blocks (Loaded {}))", blocks.len())
            };
            let block_list = List::new(items)
                .block(TuiBlock::default().title(block_title).borders(Borders::ALL))
                .highlight_style(Style::default().bg(Color::Blue).fg(Color::White))
                .highlight_symbol("> ")
                .scroll_padding(scroll_padding);
            f.render_stateful_widget(block_list, main_chunks[0], &mut state);

            // TX list (top-right panel)
            let tx_text = if !display_blocks.is_empty() {
                let block = display_blocks[selected.min(display_blocks.len() - 1)];
                let mut lines = vec![format!("Block {}:", block.0.consensus_proposal.slot)];
                // Iterate over transactions in all DPS in all lanes
                for (_lane_id, tx_id, tx) in block.0.iter_txs_with_id() {
                    let status = match ui_state.tx_status.get(&tx_id) {
                        Some(TxStatus::Success) => "[OK]",
                        Some(TxStatus::Failed) => "[FAIL]",
                        Some(TxStatus::TimedOut) => "[TIMEOUT]",
                        _ => "[SEQ]",
                    };
                    let tx_type = match &tx.transaction_data {
                        TransactionData::Blob(_) => "Blob",
                        _ => "Other",
                    };
                    lines.push(format!("{} tx:{} type:{}", status, tx_id.1, tx_type));
                }
                lines.join("\n")
            } else {
                "No blocks with transactions loaded".to_string()
            };
            let tx_paragraph = Paragraph::new(tx_text)
                .block(
                    TuiBlock::default()
                        .title(if ui_state.focused_panel == FocusPanel::TxList {
                            "TXs [focused]"
                        } else {
                            "TXs"
                        })
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false });
            f.render_widget(tx_paragraph, right_chunks[0]);

            // Block JSON (middle-right panel)
            let json_text = if !display_blocks.is_empty() {
                let block = &display_blocks[selected.min(display_blocks.len() - 1)].0;
                if let Some(pb) = ui_state
                    .processed_blocks
                    .get(&block.consensus_proposal.slot)
                {
                    format!(
                        "Block {}\nSuccessful txs: {}\nFailed txs: {}\nTimed out txs: {}",
                        pb.block_height,
                        serde_json::to_string_pretty(&pb.successful_txs)
                            .unwrap_or("BAD JSON".to_string()),
                        serde_json::to_string_pretty(&pb.failed_txs)
                            .unwrap_or("BAD JSON".to_string()),
                        serde_json::to_string_pretty(&pb.timed_out_txs)
                            .unwrap_or("BAD JSON".to_string())
                    )
                } else {
                    "Block not yet processed (press space to process)".to_string()
                }
            } else {
                "No block selected".to_string()
            };
            let json_paragraph = Paragraph::new(json_text)
                .block(
                    TuiBlock::default()
                        .title(if ui_state.focused_panel == FocusPanel::BlockJson {
                            "Block JSON [focused]"
                        } else {
                            "Block JSON"
                        })
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false })
                .scroll((ui_state.block_json_scroll, 0));
            f.render_widget(json_paragraph, right_chunks[1]);

            // Logs (bottom-right panel)
            let log_text = if let Ok(buffer) = ui_state.log_buffer.lock() {
                let logs = String::from_utf8_lossy(&buffer);
                let lines: Vec<_> = logs.lines().collect();
                // Dynamically determine visible lines based on panel height
                let scroll = ui_state.log_scroll as usize;
                let visible_lines = right_chunks[2].height.saturating_sub(2) as usize; // Subtract borders
                let start = lines.len().saturating_sub(visible_lines + scroll);
                let end = lines.len().saturating_sub(scroll);
                let end = end.max(start); // Prevent end < start
                lines[start..end].join("\n")
            } else {
                "Unable to read logs".to_string()
            };
            let log_paragraph = Paragraph::new(log_text)
                .block(
                    TuiBlock::default()
                        .title(if ui_state.focused_panel == FocusPanel::Logs {
                            "Logs [focused]"
                        } else {
                            "Logs"
                        })
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false });
            f.render_widget(log_paragraph, right_chunks[2]);

            // Command input at the bottom if in command mode
            if ui_state.command_mode {
                let bottom = Rect {
                    x: 0,
                    y: size.height.saturating_sub(3),
                    width: size.width,
                    height: 3,
                };
                let cmd_text = if let Some(msg) = &ui_state.command_message {
                    msg.clone()
                } else {
                    format!(":{}", ui_state.command_input)
                };
                let cmd_paragraph = Paragraph::new(cmd_text)
                    .block(TuiBlock::default().title(":command").borders(Borders::ALL));
                f.render_widget(cmd_paragraph, bottom);
            }
        })?;
        Ok(())
    }

    fn handle_command(ui_state: &mut DumpUiState) {
        let cmd = ui_state.command_input.trim();
        if let Some(rest) = cmd.strip_prefix("g ") {
            if let Ok(height) = rest.trim().parse::<u64>() {
                // Find the nearest block with transactions to the requested height
                let display_blocks: Vec<_> = ui_state
                    .blocks
                    .iter()
                    .filter(|(_, tx_count)| *tx_count > 0)
                    .enumerate()
                    .collect();

                if let Some((idx, block)) = display_blocks
                    .iter()
                    .min_by_key(|(_, b)| (b.0.consensus_proposal.slot as i64 - height as i64).abs())
                {
                    ui_state.selected = *idx;
                    ui_state.command_message = Some(format!(
                        "Jumped to nearest block {} (requested {})",
                        block.0.consensus_proposal.slot, height
                    ));
                    tracing::info!(
                        "Jumped to block {} (requested {})",
                        block.0.consensus_proposal.slot,
                        height
                    );
                } else {
                    ui_state.command_message =
                        Some("No blocks with data proposals found".to_string());
                }
            } else {
                ui_state.command_message = Some("Invalid block height".to_string());
            }
        } else {
            ui_state.command_message = Some("Unknown command".to_string());
        }
        ui_state.command_mode = false;
        ui_state.command_input.clear();
        ui_state.redraw = true;
    }
}

impl DumpUiState {
    fn process_block_outputs(&mut self, block: &Block) {
        // Update tx statuses based on block outputs
        for tx_hash in &block.successful_txs {
            let tx_id = TxId(
                block.dp_parent_hashes.get(tx_hash).unwrap().clone(),
                tx_hash.clone(),
            );
            self.tx_status.insert(tx_id, TxStatus::Success);
        }
        for tx_hash in &block.failed_txs {
            let tx_id = TxId(
                block.dp_parent_hashes.get(tx_hash).unwrap().clone(),
                tx_hash.clone(),
            );
            self.tx_status.insert(tx_id, TxStatus::Failed);
        }
        for tx_hash in &block.timed_out_txs {
            let tx_id = TxId(
                block.dp_parent_hashes.get(tx_hash).unwrap().clone(),
                tx_hash.clone(),
            );
            self.tx_status.insert(tx_id, TxStatus::TimedOut);
        }
        // Set Sequenced status for any new transactions
        for (tx_id, _) in &block.txs {
            self.tx_status
                .entry(tx_id.clone())
                .or_insert(TxStatus::Sequenced);
        }
    }
}
