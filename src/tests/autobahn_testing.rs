//! This module is intended for "integration" testing of the consensus and other modules.

macro_rules! build_tuple {
    ($nodes:expr, 1) => {
        ($nodes)
    };
    ($nodes:expr, 2) => {
        ($nodes, $nodes)
    };
    ($nodes:expr, 3) => {
        ($nodes, $nodes, $nodes)
    };
    ($nodes:expr, 4) => {
        ($nodes, $nodes, $nodes, $nodes)
    };
    ($nodes:expr, 5) => {
        ($nodes, $nodes, $nodes, $nodes, $nodes)
    };
    ($nodes:expr, 6) => {
        ($nodes, $nodes, $nodes, $nodes, $nodes, $nodes)
    };
    ($nodes:expr, 7) => {
        ($nodes, $nodes, $nodes, $nodes, $nodes, $nodes, $nodes)
    };
    ($nodes:expr, $count:expr) => {
        panic!("More than {} nodes isn't supported", $count)
    };
}

macro_rules! broadcast {
    (description: $description:literal, from: $sender:expr, to: [$($node:expr),*]$(, message_matches: $pattern:pat $(=> $asserts:block)? )?) => {
        {
            // Construct the broadcast message with sender information
            let message = $sender.assert_broadcast(format!("[broadcast from: {}] {}", stringify!($sender), $description).as_str()).await;

            $({
                let msg_variant_name: &'static str = message.msg.clone().into();
                if let $pattern = &message.msg {
                    $($asserts)?
                } else {
                    panic!("[broadcast from: {}] {}: Message {} did not match {}", stringify!($sender), $description, msg_variant_name, stringify!($pattern));
                }
            })?

            // Distribute the message to each specified node
            $(
                $node.handle_msg(&message, (format!("[handling broadcast message from: {} at: {}] {}", stringify!($sender), stringify!($node), $description).as_str())).await;
            )*

            message
        }
    };
}

macro_rules! send {
    (
        description: $description:literal,
        from: [$($node:expr),+],
        to: $to:expr,
        message_matches: $pattern:pat
    ) => {
        // Distribute the message to the target node from all specified nodes
        ($({
            let answer = $node.assert_send(
                &$to.validator_pubkey(),
                format!("[send from: {} to: {}] {}", stringify!($node), stringify!($to), $description).as_str()
            ).await;

            // If `message_matches` is provided, perform the pattern match
            if let $pattern = &answer.msg {
                // Execute optional assertions if provided
            } else {
                let msg_variant_name: &'static str = answer.msg.clone().into();
                panic!(
                    "[send from: {}] {}: Message {} did not match {}",
                    stringify!($node),
                    $description,
                    msg_variant_name,
                    stringify!($pattern)
                );
            }

            // Handle the message
            $to.handle_msg(
                &answer,
                format!("[handling sent message from: {} to: {}] {}", stringify!($node), stringify!($to), $description).as_str()
            ).await;
            answer
        },)+)
    };

    (
        description: $description:literal,
        from: [$($node:expr; $pattern:pat $(=> $asserts:block)?),+],
        to: $to:expr
    ) => {
        // Distribute the message to the target node from all specified nodes
        ($({
            let answer = $node.assert_send(
                &$to.validator_pubkey(),
                format!("[send from: {} to: {}] {}", stringify!($node), stringify!($to), $description).as_str()
            ).await;

            // If `message_matches` is provided, perform the pattern match
            if let $pattern = &answer.msg {
                // Execute optional assertions if provided
                $($asserts)?
            } else {
                let msg_variant_name: &'static str = answer.msg.clone().into();
                panic!(
                    "[send from: {}] {}: Message {} did not match {}",
                    stringify!($node),
                    $description,
                    msg_variant_name,
                    stringify!($pattern)
                );
            }

            // Handle the message
            $to.handle_msg(
                &answer,
                format!("[handling sent message from: {} to: {}] {}", stringify!($node), stringify!($to), $description).as_str()
            ).await;
        },)+)
    };
}

macro_rules! simple_commit_round {
    (leader: $leader:expr, followers: [$($follower:expr),+]$(, joining: $joining:expr)?) => {{
        let round_consensus_proposal;
        let round_ticket;
        let view: u64;
        broadcast! {
            description: "Leader - Prepare",
            from: $leader, to: [$($follower),+$(,$joining)?],
            message_matches: ConsensusNetMessage::Prepare(cp, ticket, prep_view) => {
                round_consensus_proposal = cp.clone();
                round_ticket = ticket.clone();
                view = *prep_view;
            }
        };

        send! {
            description: "Follower - PrepareVote",
            from: [$($follower),+], to: $leader,
            message_matches: ConsensusNetMessage::PrepareVote(_)
        };

        broadcast! {
            description: "Leader - Confirm",
            from: $leader, to: [$($follower),+$(,$joining)?],
            message_matches: ConsensusNetMessage::Confirm(..)
        };

        send! {
            description: "Follower - Confirm Ack",
            from: [$($follower),+], to: $leader,
            message_matches: ConsensusNetMessage::ConfirmAck(_)
        };

        broadcast! {
            description: "Leader - Commit",
            from: $leader, to: [$($follower),+$(,$joining)?],
            message_matches: ConsensusNetMessage::Commit(..)
        };

        (round_consensus_proposal, round_ticket, view)
    }};
}

macro_rules! disseminate {
    (txs: [$($txs:expr),+], owner: $owner:expr, voters: [$($voter:expr),+]) => {{

        let lane_id = LaneId($owner.validator_pubkey().clone());
        let dp = $owner.create_data_proposal_on_top(lane_id, &[$($txs),+]);
        $owner
            .process_new_data_proposal(dp.clone())
            .unwrap();
        $owner.timer_tick().await.unwrap();

        let dp_msg = broadcast! {
            description: "Disseminate DataProposal",
            from: $owner, to: [$($voter),+],
            message_matches: MempoolNetMessage::DataProposal(_, _)
        };

        join_all(
            [$(&mut $voter),+]
                .iter_mut()
                .map(|ctx| ctx.handle_processed_data_proposals()),
        )
        .await;

        send! {
            description: "Disseminated DataProposal Vote",
            from: [$($voter),+], to: $owner,
            message_matches: MempoolNetMessage::DataVote(..)
        };

        let poda = broadcast! {
            description: "Disseminate Poda 1",
            from: $owner, to: [$($voter),+],
            message_matches: MempoolNetMessage::PoDAUpdate(hash, _signatures) => {
                assert_eq!(hash, &dp.hashed());
            }
        };

        let poda2 = broadcast! {
            description: "Disseminate Poda 2",
            from: $owner, to: [$($voter),+],
            message_matches: MempoolNetMessage::PoDAUpdate(hash, _signatures) => {
                assert_eq!(hash, &dp.hashed());
            }
        };

        let poda3 = broadcast! {
            description: "Disseminate Poda 3",
            from: $owner, to: [$($voter),+],
            message_matches: MempoolNetMessage::PoDAUpdate(hash, _signatures) => {
                assert_eq!(hash, &dp.hashed());
            }
        };

        (dp, dp_msg, poda, poda2, poda3)
    }};
}

macro_rules! assert_chanmsg_matches {
    ($chan: expr, $pat:pat => $block:block) => {{
        let var = $chan.try_recv().unwrap();
        if let $pat = var {
            $block
        } else {
            panic!("Var {:?} should match {}", var, stringify!($pat));
        }
    }};
}

pub(crate) use assert_chanmsg_matches;
use assertables::assert_matches;
pub(crate) use broadcast;
pub(crate) use build_tuple;
use futures::future::join_all;
use hyle_model::utils::TimestampMs;
use hyle_modules::utils::da_codec::DataAvailabilityServer;
pub(crate) use send;
pub(crate) use simple_commit_round;

macro_rules! build_nodes {
    ($count:tt) => {{
        async {
            let cryptos: Vec<BlstCrypto> = AutobahnTestCtx::generate_cryptos($count);

            let mut nodes = vec![];

            for i in 0..$count {
                let crypto = cryptos.get(i).unwrap().clone();
                let mut autobahn_node =
                    AutobahnTestCtx::new(format!("node-{i}").as_ref(), crypto).await;

                autobahn_node.consensus_ctx.setup_node(i, &cryptos);
                autobahn_node.mempool_ctx.setup_node(&cryptos);
                nodes.push(autobahn_node);
            }

            build_tuple!(nodes.remove(0), $count)
        }
    }};
}

use crate::bus::command_response::Query;
use crate::bus::dont_use_this::get_receiver;
use crate::bus::metrics::BusMetrics;
use crate::bus::{bus_client, SharedMessageBus};
use crate::consensus::test::ConsensusTestCtx;
use crate::consensus::{ConsensusEvent, ConsensusNetMessage, TCKind, Ticket, TimeoutKind};
use crate::mempool::test::{make_register_contract_tx, MempoolTestCtx};
use crate::mempool::{MempoolNetMessage, QueryNewCut, ValidatorDAG};
use crate::model::*;
use crate::node_state::module::NodeStateEvent;
use crate::p2p::network::OutboundMessage;
use crate::p2p::P2PCommand;
use hyle_crypto::BlstCrypto;
use hyle_modules::handle_messages;
use tracing::info;

bus_client!(
    pub struct AutobahnBusClient {
        receiver(Query<QueryNewCut, Cut>),
    }
);

pub struct AutobahnTestCtx {
    shared_bus: SharedMessageBus,
    consensus_ctx: ConsensusTestCtx,
    mempool_ctx: MempoolTestCtx,
}

impl AutobahnTestCtx {
    async fn new(name: &str, crypto: BlstCrypto) -> Self {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("global".to_string()));
        let event_receiver = get_receiver::<ConsensusEvent>(&shared_bus).await;
        let p2p_receiver = get_receiver::<P2PCommand>(&shared_bus).await;
        let consensus_out_receiver = get_receiver::<OutboundMessage>(&shared_bus).await;
        let mempool_out_receiver = get_receiver::<OutboundMessage>(&shared_bus).await;
        let mempool_event_receiver = get_receiver::<MempoolBlockEvent>(&shared_bus).await;
        let mempool_status_event_receiver = get_receiver::<MempoolStatusEvent>(&shared_bus).await;

        let consensus = ConsensusTestCtx::build_consensus(&shared_bus, crypto.clone()).await;
        let mempool = MempoolTestCtx::build_mempool(&shared_bus, crypto).await;

        AutobahnTestCtx {
            shared_bus,
            consensus_ctx: ConsensusTestCtx {
                out_receiver: consensus_out_receiver,
                _event_receiver: event_receiver,
                _p2p_receiver: p2p_receiver,
                consensus,
                name: name.to_string(),
            },
            mempool_ctx: MempoolTestCtx {
                name: name.to_string(),
                out_receiver: mempool_out_receiver,
                mempool_event_receiver,
                mempool_status_event_receiver,
                mempool,
            },
        }
    }

    pub fn generate_cryptos(nb: usize) -> Vec<BlstCrypto> {
        let mut res: Vec<_> = (0..nb)
            .map(|i| {
                let crypto = BlstCrypto::new(&format!("node-{i}")).unwrap();
                info!("node {}: {}", i, crypto.validator_pubkey());
                crypto
            })
            .collect();
        // Staking currently sorts the bonded validators so do the same
        res.sort_by_key(|c| c.validator_pubkey().clone());
        res
    }

    /// Spawn a coroutine to answer the command response call of start_round, with the current of mempool
    async fn start_round_with_cut_from_mempool(&mut self, ts: TimestampMs) {
        let staking = self.consensus_ctx.staking();
        let latest_cut: Cut = self.mempool_ctx.gen_cut(&staking);

        let mut autobahn_client_bus =
            AutobahnBusClient::new_from_bus(self.shared_bus.new_handle()).await;

        tokio::spawn(async move {
            handle_messages! {
                on_bus autobahn_client_bus,
                listen<Query<QueryNewCut, Cut>> qnc => {
                    if let Ok(value) = qnc.take() {
                        value.answer(latest_cut.clone()).expect("error when injecting cut");

                        break;
                    }
                }
            }
        });

        self.consensus_ctx.start_round_at(ts).await;
    }

    /// Just start the round and wait for the timeout
    async fn start_round_with_last_seen_cut(&mut self, ts: TimestampMs) {
        self.consensus_ctx.start_round_at(ts).await;
    }
}

fn create_poda(
    data_proposal_hash: DataProposalHash,
    line_size: LaneBytesSize,
    nodes: &[&AutobahnTestCtx],
) -> hyle_crypto::Signed<(DataProposalHash, LaneBytesSize), AggregateSignature> {
    let mut signed_messages: Vec<ValidatorDAG> = nodes
        .iter()
        .map(|node| {
            node.mempool_ctx
                .sign_data((data_proposal_hash.clone(), line_size))
                .unwrap()
        })
        .collect();
    signed_messages.sort_by(|a, b| a.signature.cmp(&b.signature));

    let aggregates: Vec<&ValidatorDAG> = signed_messages.iter().collect();
    BlstCrypto::aggregate((data_proposal_hash.clone(), line_size), &aggregates).unwrap()
}

#[test_log::test(tokio::test)]
async fn autobahn_basic_flow() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test2"));

    let dp = node1
        .mempool_ctx
        .create_data_proposal(None, &[register_tx, register_tx_2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(_, data) => {
            assert_eq!(data.txs.len(), 2);
        }
    };

    join_all(
        [
            &mut node2.mempool_ctx,
            &mut node3.mempool_ctx,
            &mut node4.mempool_ctx,
        ]
        .iter_mut()
        .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    send! {
        description: "Disseminated Tx Vote",
        from: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx], to: node1.mempool_ctx,
        message_matches: MempoolNetMessage::DataVote(..)
    };

    let data_proposal_hash_node1 = node1
        .mempool_ctx
        .current_hash(&node1.mempool_ctx.own_lane())
        .expect("Current hash should be there");
    let node1_l_size = node1.mempool_ctx.current_size().unwrap();

    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    let consensus_proposal;

    let poda = create_poda(
        data_proposal_hash_node1.clone(),
        node1_l_size,
        &[&node1, &node2, &node3, &node4],
    );

    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, ..) => {
            consensus_proposal = cp.clone();
            assert_eq!(
                cp
                    .cut
                    .clone()
                    .iter()
                    .find(|(lane_id, _hash, _size, _poda)|
                        lane_id == &node1.mempool_ctx.own_lane()
                    ),
                Some((node1.mempool_ctx.own_lane(), data_proposal_hash_node1, node1_l_size, poda.signature)).as_ref()
            );
        }
    };

    let (vote2, vote3, vote4) = send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    assert_matches!(
        vote2.msg,
        ConsensusNetMessage::PrepareVote(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );
    assert_matches!(
        vote3.msg,
        ConsensusNetMessage::PrepareVote(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );
    assert_matches!(
        vote4.msg,
        ConsensusNetMessage::PrepareVote(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );

    broadcast! {
        description: "Confirm",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    let (vote2, vote3, vote4) = send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    assert_matches!(
        vote2.msg,
        ConsensusNetMessage::ConfirmAck(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );
    assert_matches!(
        vote3.msg,
        ConsensusNetMessage::ConfirmAck(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );
    assert_matches!(
        vote4.msg,
        ConsensusNetMessage::ConfirmAck(Signed { msg: (cp, _), .. })
        if cp == consensus_proposal.hashed()
    );

    broadcast! {
        description: "Commit",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };
}

#[test_log::test(tokio::test)]
async fn mempool_broadcast_multiple_data_proposals() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    // First data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test2"));

    let dp = node1
        .mempool_ctx
        .create_data_proposal(None, &[register_tx, register_tx_2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(_, _)
    };

    join_all(
        [
            &mut node2.mempool_ctx,
            &mut node3.mempool_ctx,
            &mut node4.mempool_ctx,
        ]
        .iter_mut()
        .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    send! {
        description: "Disseminated Tx Vote",
        from: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx], to: node1.mempool_ctx,
        message_matches: MempoolNetMessage::DataVote(..)
    };

    node1.mempool_ctx.assert_broadcast("poda update f+1").await;
    node1.mempool_ctx.assert_broadcast("poda update 2f+1").await;
    node1.mempool_ctx.assert_broadcast("poda update 3f+1").await;

    // Second data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test3"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test4"));

    let dp = node1
        .mempool_ctx
        .create_data_proposal(Some(dp.hashed()), &[register_tx, register_tx_2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(_, _)
    };

    join_all(
        [
            &mut node2.mempool_ctx,
            &mut node3.mempool_ctx,
            &mut node4.mempool_ctx,
        ]
        .iter_mut()
        .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    send! {
        description: "Disseminated Tx Vote",
        from: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx], to: node1.mempool_ctx,
        message_matches: MempoolNetMessage::DataVote(..)
    };
}

#[test_log::test(tokio::test)]
async fn mempool_podaupdate_too_early() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    // First data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test1"));

    let dp = node1.mempool_ctx.create_data_proposal(None, &[register_tx]);
    let lane_id = LaneId(node1.mempool_ctx.validator_pubkey().clone());
    node1
        .mempool_ctx
        .process_new_data_proposal(dp.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    let dp_msg = broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(_, _)
    };

    join_all(
        [&mut node2.mempool_ctx, &mut node3.mempool_ctx]
            .iter_mut()
            .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    send! {
        description: "Disseminated Tx Vote",
        from: [node2.mempool_ctx, node3.mempool_ctx], to: node1.mempool_ctx,
        message_matches: MempoolNetMessage::DataVote(..)
    };

    let poda = broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx],
        message_matches: MempoolNetMessage::PoDAUpdate(hash, signatures) => {
            assert_eq!(hash, &dp.hashed());
            assert_eq!(2, signatures.len());
        }
    };

    let poda2 = broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx],
        message_matches: MempoolNetMessage::PoDAUpdate(hash, signatures) => {
            assert_eq!(hash, &dp.hashed());
            assert_eq!(3, signatures.len());
        }
    };

    let assert_nb_signatures = |node: &AutobahnTestCtx, n: usize| {
        assert_eq!(
            node.mempool_ctx.last_lane_entry(&lane_id).1,
            dp.clone().hashed()
        );
        assert_eq!(
            node.mempool_ctx
                .last_lane_entry(&lane_id)
                .0
                 .0
                .signatures
                .len(),
            n
        );
    };

    assert_nb_signatures(&node2, 3);
    assert_nb_signatures(&node3, 3);

    assert_eq!(node4.mempool_ctx.current_hash(&lane_id), None);

    // Handle Poda before data proposal (simulate a data proposal still being processed, not recorded yet)

    node4.mempool_ctx.handle_msg(&poda, "Poda handling").await;
    node4
        .mempool_ctx
        .handle_msg(&poda2, "Poda handling 2")
        .await;
    node4.mempool_ctx.handle_msg(&dp_msg, "Data Proposal").await;

    node4.mempool_ctx.handle_processed_data_proposals().await;

    // 4 because the 3 signatures are the ones of the other nodes, so + the node 4 signature it makes 4 signatures
    assert_nb_signatures(&node4, 4);

    send! {
        description: "Disseminated Tx Vote",
        from: [node4.mempool_ctx], to: node1.mempool_ctx,
        message_matches: MempoolNetMessage::DataVote(..)
    };

    broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::PoDAUpdate(hash, signatures) => {
            assert_eq!(hash, &dp.hashed());
            assert_eq!(4, signatures.len());
        }
    };

    assert_nb_signatures(&node2, 4);
    assert_nb_signatures(&node3, 4);
    assert_nb_signatures(&node4, 4);
}

#[test_log::test(tokio::test)]
async fn consensus_missed_prepare() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    // First data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test1"));

    disseminate! {
        txs: [register_tx.clone()],
        owner: node1.mempool_ctx,
        voters: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx]
    };

    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    // Normal round from Genesis
    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx]
    };

    disseminate! {
        txs: [register_tx],
        owner: node2.mempool_ctx,
        voters: [node1.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx]
    };

    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    // Node 3 misses the prepare
    simple_commit_round! {
        leader: node2.consensus_ctx,
        followers: [node1.consensus_ctx, node4.consensus_ctx]
    };

    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node4.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // node 3 should join the mutiny
    broadcast! {
        description: "Follower - Timeout",
        from: node4.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    node1
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 1")
        .await;
    node2
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 2")
        .await;

    // Node 4 is the next leader slot 3 view 1, has built the tc by joining the mutiny

    node4
        .start_round_with_cut_from_mempool(TimestampMs(4000))
        .await;

    broadcast! {
        description: "Leader Prepare with TC ticket",
        from: node4.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    // Node 3 can't vote yet
    // Node 3 needs to sync

    let sync_request = node3
        .consensus_ctx
        .assert_send(&node4.consensus_ctx.pubkey(), "Sync Request")
        .await;

    node4
        .consensus_ctx
        .handle_msg(&sync_request, "Handling Sync request")
        .await;

    let sync_reply = node4
        .consensus_ctx
        .assert_send(&node3.consensus_ctx.validator_pubkey(), "SyncReply")
        .await;

    node3
        .consensus_ctx
        .handle_msg(&sync_reply, "Handling Sync reply")
        .await;

    send! {
        description: "Voting after sync",
        from: [node3.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(..)
    };

    // Others vote too
    send! {
        description: "Voting",
        from: [node1.consensus_ctx, node2.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(..)
    };

    broadcast! {
        description: "Confirm",
        from: node4.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node4.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Weirdly, next leader is node 4 too on slot: 4 view: 0

    node4
        .start_round_with_cut_from_mempool(TimestampMs(5000))
        .await;

    // Making sure Consensus goes on

    simple_commit_round! {
        leader: node4.consensus_ctx,
        followers: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx]
    };
}

#[test_log::test(tokio::test)]
async fn mempool_fail_to_vote_on_fork() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    // Let's create data proposals that will be synchronized with 2 nodes, but not the 3rd.

    // First data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test2"));

    let dp1 = node1
        .mempool_ctx
        .create_data_proposal(None, &[register_tx, register_tx_2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp1.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    let dp1_check;

    broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(_, data) => {
            dp1_check = data.clone();
        }
    };

    assert_eq!(dp1, dp1_check);

    join_all(
        [
            &mut node2.mempool_ctx,
            &mut node3.mempool_ctx,
            &mut node4.mempool_ctx,
        ]
        .iter_mut()
        .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    send! {
        description: "Disseminated Tx Vote",
        from: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx], to: node1.mempool_ctx,
        message_matches: MempoolNetMessage::DataVote(..)
    };

    node1.mempool_ctx.assert_broadcast("poda update f+1").await;
    node1.mempool_ctx.assert_broadcast("poda update 2f+1").await;
    node1.mempool_ctx.assert_broadcast("poda update 3f+1").await;

    // Second data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test3"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test4"));

    let dp2 = node1
        .mempool_ctx
        .create_data_proposal(Some(dp1.hashed()), &[register_tx, register_tx_2]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp2.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    broadcast! {
        description: "Disseminate Tx",
        from: node1.mempool_ctx, to: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx],
        message_matches: MempoolNetMessage::DataProposal(_, _)
    };

    join_all(
        [
            &mut node2.mempool_ctx,
            &mut node3.mempool_ctx,
            &mut node4.mempool_ctx,
        ]
        .iter_mut()
        .map(|ctx| ctx.handle_processed_data_proposals()),
    )
    .await;

    send! {
        description: "Disseminated Tx Vote",
        from: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx], to: node1.mempool_ctx,
        message_matches: MempoolNetMessage::DataVote(..)
    };

    // BASIC FORK
    // dp1 <- dp2
    //     <- dp3

    let dp_fork_3 = DataProposal::new(Some(dp1.hashed()), vec![Transaction::default()]);

    let data_proposal_fork_3 = node1
        .mempool_ctx
        .create_net_message(MempoolNetMessage::DataProposal(
            dp_fork_3.hashed(),
            dp_fork_3.clone(),
        ))
        .unwrap();

    node2
        .mempool_ctx
        .handle_msg(&data_proposal_fork_3, "Fork 3")
        .await;
    node3
        .mempool_ctx
        .handle_msg(&data_proposal_fork_3, "Fork 3")
        .await;
    node4
        .mempool_ctx
        .handle_msg(&data_proposal_fork_3, "Fork 3")
        .await;

    // Check fork has not been consumed

    assert_ne!(
        node2
            .mempool_ctx
            .last_lane_entry(&node1.mempool_ctx.own_lane())
            .1,
        dp_fork_3.hashed()
    );
    assert_ne!(
        node3
            .mempool_ctx
            .last_lane_entry(&node1.mempool_ctx.own_lane())
            .1,
        dp_fork_3.hashed()
    );
    assert_ne!(
        node4
            .mempool_ctx
            .last_lane_entry(&node1.mempool_ctx.own_lane())
            .1,
        dp_fork_3.hashed()
    );
}

#[test_log::test(tokio::test)]
async fn autobahn_rejoin_flow() {
    let mut server = DataAvailabilityServer::start(7890, "DaServer")
        .await
        .unwrap();
    let (mut node1, mut node2) = build_nodes!(2).await;

    // Let's setup the consensus so our joining node has some blocks to catch up.
    ConsensusTestCtx::setup_for_round(
        &mut [&mut node1.consensus_ctx, &mut node2.consensus_ctx],
        0,
        3,
        0,
    );

    let crypto = BlstCrypto::new("node-3").unwrap();
    let mut joining_node = AutobahnTestCtx::new("node-3", crypto).await;
    joining_node
        .consensus_ctx
        .setup_for_joining(&[&node1.consensus_ctx, &node2.consensus_ctx]);

    // Let's setup a DataAvailability on this bus
    let mut da = crate::data_availability::tests::DataAvailabilityTestCtx::new(
        joining_node.shared_bus.new_handle(),
    )
    .await;

    let mut blocks = vec![SignedBlock {
        data_proposals: vec![],
        certificate: AggregateSignature::default(),
        consensus_proposal: ConsensusProposal {
            slot: 0,
            ..ConsensusProposal::default()
        },
    }];

    for _ in 0..2 {
        blocks.push(SignedBlock {
            data_proposals: vec![],
            certificate: AggregateSignature::default(),
            consensus_proposal: ConsensusProposal {
                slot: blocks.len() as u64,
                parent_hash: blocks[blocks.len() - 1].hashed(),
                ..ConsensusProposal::default()
            },
        });
    }

    let mut ns_event_receiver = get_receiver::<NodeStateEvent>(&joining_node.shared_bus).await;

    // Catchup up to the last block, but don't actually process the last block message yet.
    for block in blocks.get(0..blocks.len() - 1).unwrap() {
        da.handle_signed_block(block.clone(), &mut server).await;
    }
    while let Ok(event) = ns_event_receiver.try_recv() {
        info!("{:?}", event);
        joining_node
            .consensus_ctx
            .handle_node_state_event(event)
            .await
            .expect("should handle data event");
    }

    // Do a few rounds of consensus-with-lag and note that we don't actually catch up.
    // (this is expected because DA stopped receiving new blocks, as it did indeed catch up)
    for i in 1..4 {
        node1
            .start_round_with_cut_from_mempool(TimestampMs(1000 * i))
            .await;

        simple_commit_round! {
            leader: node1.consensus_ctx,
            followers: [node2.consensus_ctx],
            joining: joining_node.consensus_ctx
        };

        // Swap so we handle leader changes correctly
        std::mem::swap(&mut node1, &mut node2);
    }

    // Now process block 2
    da.handle_signed_block(blocks.get(2).unwrap().clone(), &mut server)
        .await;
    while let Ok(event) = ns_event_receiver.try_recv() {
        info!("{:?}", event);
        joining_node
            .consensus_ctx
            .handle_node_state_event(event)
            .await
            .expect("should handle data event");
    }

    // Process round
    node1
        .start_round_with_cut_from_mempool(TimestampMs(5000))
        .await;

    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node2.consensus_ctx],
        joining: joining_node.consensus_ctx
    };

    std::mem::swap(&mut node1, &mut node2);

    // We still aren't caught up
    assert!(joining_node.consensus_ctx.is_joining());

    // Catch up
    for _ in 0..4 {
        let block = SignedBlock {
            data_proposals: vec![],
            certificate: AggregateSignature::default(),
            consensus_proposal: ConsensusProposal {
                slot: blocks.len() as u64,
                parent_hash: blocks[blocks.len() - 1].hashed(),
                ..ConsensusProposal::default()
            },
        };
        da.handle_signed_block(block.clone(), &mut server).await;
        blocks.push(block);
        while let Ok(event) = ns_event_receiver.try_recv() {
            info!("{:?}", event);
            joining_node
                .consensus_ctx
                .handle_node_state_event(event)
                .await
                .expect("should handle data event");
        }
    }

    // Process round
    node1
        .start_round_with_cut_from_mempool(TimestampMs(6000))
        .await;

    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node2.consensus_ctx],
        joining: joining_node.consensus_ctx
    };

    // We are caught up
    assert!(!joining_node.consensus_ctx.is_joining());
}

#[test_log::test(tokio::test)]
async fn protocol_fees() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    // First data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test1"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test2"));

    let (dp, _, _, _, _) = disseminate! {
        txs: [register_tx, register_tx_2],
        owner: node1.mempool_ctx,
        voters: [node2.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx]
    };

    let dp_size_1 = LaneBytesSize(dp.estimate_size() as u64);
    assert_eq!(node1.mempool_ctx.current_size(), Some(dp_size_1));
    assert_eq!(
        node1
            .mempool_ctx
            .current_size_of(&node2.mempool_ctx.own_lane()),
        None
    );

    assert_eq!(node2.mempool_ctx.current_size(), None);
    assert_eq!(
        node2
            .mempool_ctx
            .current_size_of(&node1.mempool_ctx.own_lane()),
        Some(dp_size_1)
    );
    assert_eq!(
        node3
            .mempool_ctx
            .current_size_of(&node1.mempool_ctx.own_lane()),
        Some(dp_size_1)
    );
    assert_eq!(
        node4
            .mempool_ctx
            .current_size_of(&node1.mempool_ctx.own_lane()),
        Some(dp_size_1)
    );

    // Second data proposal

    let register_tx = make_register_contract_tx(ContractName::new("test3"));
    let register_tx_2 = make_register_contract_tx(ContractName::new("test4"));
    let register_tx_3 = make_register_contract_tx(ContractName::new("test5"));

    let (dp, _, _, _, _) = disseminate! {
        txs: [register_tx, register_tx_2, register_tx_3],
        owner: node2.mempool_ctx,
        voters: [node1.mempool_ctx, node3.mempool_ctx, node4.mempool_ctx]
    };

    let dp_size_2 = LaneBytesSize(dp.estimate_size() as u64);
    assert_eq!(node2.mempool_ctx.current_size(), Some(dp_size_2));
    assert_eq!(
        node2
            .mempool_ctx
            .current_size_of(&node1.mempool_ctx.own_lane()),
        Some(dp_size_1)
    );

    assert_eq!(node1.mempool_ctx.current_size(), Some(dp_size_1));
    assert_eq!(
        node1
            .mempool_ctx
            .current_size_of(&node2.mempool_ctx.own_lane()),
        Some(dp_size_2)
    );

    assert_eq!(
        node3
            .mempool_ctx
            .current_size_of(&node1.mempool_ctx.own_lane()),
        Some(dp_size_1)
    );
    assert_eq!(
        node3
            .mempool_ctx
            .current_size_of(&node2.mempool_ctx.own_lane()),
        Some(dp_size_2)
    );

    // Let's do a consensus round

    node1
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, ..) => {
            assert_eq!(cp.staking_actions.len(), 2);
            assert_eq!(
                cp.staking_actions[0],
                ConsensusStakingAction::PayFeesForDaDi {
                    lane_id: node1.mempool_ctx.own_lane(),
                    cumul_size: dp_size_1
                }
            );
            assert_eq!(
                cp.staking_actions[1],
                ConsensusStakingAction::PayFeesForDaDi {
                    lane_id: node2.mempool_ctx.own_lane(),
                    cumul_size: dp_size_2
                }
            );
        }
    };
}

/// P = Proposal
/// Cf = Confirm
/// C = Commit
///
/// Normal case: P1 -> Cf1 -> C1 -> P2 -> Cf2 -> C2
/// Test case: P1 -> C1 -> P2 -> C2
///
/// Confirm messages can be ignored if not received
#[test_log::test(tokio::test)]
async fn autobahn_missed_a_confirm_message() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
            &mut node4.consensus_ctx,
        ],
        0,
        5,
        0,
    );

    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    // Slot from node-1 not yet sent to node 4
    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    // Node 4 doesn't receive confirm
    broadcast! {
        description: "Confirm",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    // But Node 4 receive commit
    broadcast! {
        description: "Commit",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 6 starts with new leader, sending to all
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };
}

/// P = Proposal
/// Cf = Confirm
/// C = Commit
///
/// Normal case: P1 -> Cf1 -> C1 -> P2 -> Cf2 -> C2
/// Test case: P1 -> P2 -> Cf2 -> C2
///
/// Confirm and commit can be ignored if next porposal is received
#[test_log::test(tokio::test)]
async fn autobahn_missed_confirm_and_commit_messages() {
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
            &mut node4.consensus_ctx,
        ],
        0,
        5,
        0,
    );

    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare - Slot from node-1 not yet sent to node 4",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm - Node 4 doesn't receive confirm",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit - Node 4 doesn't receive commit",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 6 starts with new leader, sending to all
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote - Node 4 has fast-forwarded to the next proposal",
        from: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };
}

#[test_log::test(tokio::test)]
async fn autobahn_buffer_early_messages() {
    // node 4 got disconnected for a slot
    let (mut node1, mut node2, mut node3, mut node4) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
            &mut node4.consensus_ctx,
        ],
        0,
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node1
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx, node3.consensus_ctx, node4.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm - Node4 disconnected",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit - Node4 still disconnected",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 4 starts with new leader with node4 disconnected
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node3.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node3.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 5 starts with new leader but node4 is back online
    node3
        .start_round_with_cut_from_mempool(TimestampMs(3000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx], to: node3.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    let confirm = node3.consensus_ctx.assert_broadcast("Confirm").await;

    send! {
        description: "SyncRequest - Node4 ask for missed proposal Slot 4",
        from: [node4.consensus_ctx], to: node3.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncRequest(_)
    };

    send! {
        description: "SyncReply - Node 3 replies with proposal Slot 4",
        from: [node3.consensus_ctx], to: node4.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncReply(_)
    };

    send! {
        description: "PrepareVote - Node4 votes on slot 5",
        from: [node4.consensus_ctx], to: node3.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    node1
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node3 at: node1] Confirm",
        )
        .await;
    node2
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node3 at: node2] Confirm",
        )
        .await;
    node4
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node3 at: node4] Confirm",
        )
        .await;

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node4.consensus_ctx], to: node3.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node4.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 6 starts with node4 as leader
    node4
        .start_round_with_cut_from_mempool(TimestampMs(4000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node4.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };
}

#[test_log::test(tokio::test)]
async fn autobahn_got_timed_out_during_sync() {
    // node1 is 2nd leader but got disconnected
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        0,
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm - Node1 disconnected",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit - Node1 still disconnected",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Make node0 and node2 timeout, node3 will not timeout but follow mutiny
    // , because at f+1, mutiny join
    ConsensusTestCtx::timeout(&mut [&mut node0.consensus_ctx, &mut node2.consensus_ctx]).await;

    broadcast! {
        description: "Follower - Timeout",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // node 3 should join the mutiny
    broadcast! {
        description: "Follower - Timeout",
        from: node3.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    node0
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 1")
        .await;
    // Node 2 is next leader, and does not emits a timeout certificate since it will broadcast the next Prepare with it
    node2
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 3");
    node3
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 4")
        .await;

    // Slot 6 starts with new leader with node1 disconnected
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node0.consensus_ctx, node3.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node0.consensus_ctx, node3.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Slot 5 starts but node1 is back online - leader is again node2
    node2
        .start_round_with_cut_from_mempool(TimestampMs(3000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node0.consensus_ctx, node3.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    let confirm = node2.consensus_ctx.assert_broadcast("Confirm").await;

    send! {
        description: "SyncRequest - Node1 ask for missed proposal Slot 4",
        from: [node1.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncRequest(_)
    };

    send! {
        description: "SyncReply - Node 2 replies with proposal Slot 4",
        from: [node2.consensus_ctx], to: node1.consensus_ctx,
        message_matches: ConsensusNetMessage::SyncReply(_)
    };

    send! {
        description: "PrepareVote - Node1 votes on slot 7",
        from: [node1.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    node0
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node2 at: node0] Confirm",
        )
        .await;
    node1
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node2 at: node1] Confirm",
        )
        .await;
    node3
        .consensus_ctx
        .handle_msg(
            &confirm,
            "[handling broadcast message from: node2 at: node3] Confirm",
        )
        .await;

    send! {
        description: "ConfirmAck",
        from: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx], to: node2.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    broadcast! {
        description: "Commit",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };
}

#[test_log::test(tokio::test)]
async fn autobahn_commit_different_views_for_f() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        0,
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    // At this point, node0 has committed S5V0. It now disconnects.
    // (process the broadcast silently)
    broadcast! {
        description: "Commit",
        from: node0.consensus_ctx, to: [],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Nodes 1, 2, 3 ultimately timeout.
    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    // Node 0 is still out.
    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Node 1 is next leader, and does not emits a timeout certificate since it will broadcast the next Prepare with it
    node1
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 1");
    node2
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 2")
        .await;
    node3
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 3")
        .await;

    node1
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;
    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node2.consensus_ctx, node3.consensus_ctx]
    };

    // Node 1 is the leader of the next slot as well
    node1
        .start_round_with_last_seen_cut(TimestampMs(2000))
        .await;

    // At this point node 0 silently reconnects. However, since it has already committed slot 5,
    // it will emit a warn on receiving a different CQC than the one it buffered.
    // This still proceeds fine as we 'cheat' for now.
    simple_commit_round! {
        leader: node1.consensus_ctx,
        followers: [node0.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx]
    };
}

#[test_log::test(tokio::test)]
async fn autobahn_commit_different_views_for_fplusone() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        0,
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(..)
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    // Simulate a weird case - node0 has committed the new proposal, and node3 has received the Commit message.
    // We know have two nodes that have committed, and two that haven't and timeout.
    broadcast! {
        description: "Node3 commits",
        from: node0.consensus_ctx, to: [node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Commit(..)
    };

    // Nodes 1, 2 ultimately timeout, broadcasting to all now reconnected.
    ConsensusTestCtx::timeout(&mut [&mut node1.consensus_ctx, &mut node2.consensus_ctx]).await;
    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Nobody makes a TC as the nodes that have already committed won't answer.
    node1
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 1");
    node2
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 2");

    // At this point we are essentially stuck.
    // TODO: fix this by sending commit messages to the nodes that are timing out so they can unlock themselves.
}

#[test_log::test(tokio::test)]
async fn autobahn_commit_byzantine_across_views_attempts() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        0,
        5,
        0,
    );

    // Goal of the test: at slot 5 view 0, we have nodes voting on a prepare. A commit could in theory be created if someone side-channels the confirmacks
    // so we must ensure that we cannot commit another value. Attempt to do so and notice failures.
    // Node 1 fails to receive the initial proposal and proposes a different one.
    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    let initial_cp;

    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, ..) => {
            initial_cp = cp.clone();
        }
    };

    send! {
        description: "PrepareVote",
        from: [node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    broadcast! {
        description: "Confirm",
        from: node0.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    // Check that they sent their vote
    node2
        .consensus_ctx
        .assert_send(&node0.consensus_ctx.validator_pubkey(), "confirmack")
        .await;
    node3
        .consensus_ctx
        .assert_send(&node0.consensus_ctx.validator_pubkey(), "confirmack")
        .await;

    // Nodes 0, 1, 2, 3 ultimately timeout.
    ConsensusTestCtx::timeout(&mut [
        &mut node0.consensus_ctx,
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Follower - Timeout",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node1.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Timeout",
        from: node2.consensus_ctx, to: [node0.consensus_ctx, node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout((_, TimeoutKind::PrepareQC(..)))
    };
    // node 3 won't even timeout, it will process the TC directly.

    // Node 1 is next leader, and does not emits a timeout certificate since it will broadcast the next Prepare with it
    node0
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 0")
        .await;
    node1
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 1");
    node2
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 2")
        .await;
    node3
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 3")
        .await;

    // Change the proposal.
    let dp = node1.mempool_ctx.create_data_proposal(None, &[]);
    node1
        .mempool_ctx
        .process_new_data_proposal(dp.clone())
        .unwrap();
    node1.mempool_ctx.timer_tick().await.unwrap();

    node1
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    // Check that the node is reproposing the same.
    broadcast! {
        description: "Prepare",
        from: node1.consensus_ctx, to: [node0.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, Ticket::TimeoutQC(.., TCKind::PrepareQC((_, tcp))), _) => {
            assert_eq!(cp, &initial_cp);
            assert_eq!(tcp, &initial_cp);
        }
    };
}

#[test_log::test(tokio::test)]
async fn autobahn_commit_prepare_qc_across_multiple_views() {
    let (mut node0, mut node1, mut node2, mut node3) = build_nodes!(4).await;

    ConsensusTestCtx::setup_for_round(
        &mut [
            &mut node0.consensus_ctx,
            &mut node1.consensus_ctx,
            &mut node2.consensus_ctx,
            &mut node3.consensus_ctx,
        ],
        0,
        5,
        0,
    );

    // Slot 5 starts, all nodes receive the prepare
    node0
        .start_round_with_cut_from_mempool(TimestampMs(1000))
        .await;

    let initial_cp;
    broadcast! {
        description: "Prepare",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, ..) => {
            initial_cp = cp.clone();
        }
    };

    send! {
        description: "PrepareVote",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::PrepareVote(_)
    };

    // Node0 gets the confirm message out but then crashes before sending commit
    broadcast! {
        description: "Confirm",
        from: node0.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Confirm(..)
    };

    send! {
        description: "ConfirmAck",
        from: [node1.consensus_ctx, node2.consensus_ctx, node3.consensus_ctx], to: node0.consensus_ctx,
        message_matches: ConsensusNetMessage::ConfirmAck(_)
    };

    // First timeout - nodes 1,2,3 timeout and move to view 1
    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Follower - First Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - First Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - First Timeout",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Node 1 is next leader (slot 5 + view 1 = 6 % 4 = 2), doesn't emit timeout certificate
    node1
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 1");
    node2
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 2")
        .await;
    node3
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 3")
        .await;

    // Second timeout - move to view 2
    ConsensusTestCtx::timeout(&mut [
        &mut node1.consensus_ctx,
        &mut node2.consensus_ctx,
        &mut node3.consensus_ctx,
    ])
    .await;

    broadcast! {
        description: "Follower - Second Timeout",
        from: node1.consensus_ctx, to: [node2.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Second Timeout",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };
    broadcast! {
        description: "Follower - Second Timeout",
        from: node3.consensus_ctx, to: [node1.consensus_ctx, node2.consensus_ctx],
        message_matches: ConsensusNetMessage::Timeout(..)
    };

    // Node 2 is next leader (slot 5 + view 2 = 7 % 4 = 3), doesn't emit timeout certificate
    node1
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 1")
        .await;
    node2
        .consensus_ctx
        .assert_no_broadcast("Timeout Certificate 2");
    node3
        .consensus_ctx
        .assert_broadcast("Timeout Certificate 3")
        .await;

    // Start next round with node2 as leader
    node2
        .start_round_with_cut_from_mempool(TimestampMs(2000))
        .await;

    // Check that node2 is reproposing the same CP from view 0
    broadcast! {
        description: "Prepare",
        from: node2.consensus_ctx, to: [node1.consensus_ctx, node3.consensus_ctx],
        message_matches: ConsensusNetMessage::Prepare(cp, Ticket::TimeoutQC(.., TCKind::PrepareQC((_, tcp))), _) => {
            assert_eq!(cp, &initial_cp, "Leader must propose the same CP from view 0 even after multiple view changes");
            assert_eq!(tcp, &initial_cp, "TimeoutQC must reference the same CP from view 0");
        }
    };
}
