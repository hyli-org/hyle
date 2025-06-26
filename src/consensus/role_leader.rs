use anyhow::{anyhow, bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::HashSet;

use crate::{
    bus::command_response::CmdRespClient,
    consensus::*,
    mempool::QueryNewCut,
    model::{Hashed, ValidatorPublicKey},
};
use hyle_model::{utils::TimestampMs, ConsensusProposal, ConsensusStakingAction};
use staking::state::MIN_STAKE;
use tokio::sync::broadcast;
use tracing::{debug, error, trace};

use super::Consensus;

#[derive(BorshSerialize, BorshDeserialize, Default, Debug)]
pub enum Step {
    #[default]
    StartNewSlot,
    PrepareVote,
    ConfirmAck,
}

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub struct LeaderState {
    pub(super) step: Step,
    pub(super) prepare_votes: HashSet<PrepareVote>,
    pub(super) confirm_ack: HashSet<ConfirmAck>,
    pub(super) pending_ticket: Option<Ticket>,
}

impl Consensus {
    pub(super) async fn start_round(
        &mut self,
        current_timestamp: TimestampMs,
        may_delay: Option<TimestampMs>,
    ) -> Result<()> {
        if !matches!(self.bft_round_state.leader.step, Step::StartNewSlot) {
            bail!(
                "Cannot start a new slot while in step {:?}",
                self.bft_round_state.leader.step
            );
        }

        if !self.is_round_leader() {
            bail!(
                "I ({}) am not the leader for slot {} view {}, expected {}",
                self.crypto.validator_pubkey(),
                self.bft_round_state.slot,
                self.bft_round_state.view,
                self.round_leader()?,
            );
        }

        let ticket = self
            .bft_round_state
            .leader
            .pending_ticket
            .take()
            .ok_or(anyhow!("No ticket available for this slot"))?;

        // If we already have a consensusproposal for this slot, then we voted on it,
        // and so we must repropose it (in case a commit was reached somewhere)
        if self.bft_round_state.current_proposal.slot == self.bft_round_state.slot {
            debug!("♻️ Starting new view with the same ConsensusProposal as previous views")
        } else {
            // Creates ConsensusProposal
            // Query new cut to Mempool
            trace!(
                "Querying Mempool for a new cut with Staking: {:#?}",
                self.bft_round_state.staking
            );

            let cut = match tokio::time::timeout(
                self.config.consensus.slot_duration,
                self.bus.shutdown_aware_request::<Self>(QueryNewCut(
                    self.bft_round_state.staking.clone(),
                )),
            )
            .await
            .context("Timeout while querying Mempool")
            {
                Ok(Ok(cut)) => {
                    // If the cut is the same as before (and we didn't time out), then check if we should delay.
                    if may_delay
                        .as_ref()
                        .map(|ts| ts > &current_timestamp)
                        .unwrap_or(true)
                        && !matches!(ticket, Ticket::TimeoutQC(..))
                        && cut == self.bft_round_state.parent_cut
                    {
                        debug!("⏳ Delaying slot start");
                        self.bft_round_state.leader.pending_ticket = Some(ticket);
                        let command_sender = hyle_modules::utils::static_type_map::Pick::<
                            broadcast::Sender<ConsensusCommand>,
                        >::get(&self.bus)
                        .clone();
                        let max_delay = may_delay.unwrap_or_else(|| {
                            current_timestamp.clone() + self.config.consensus.slot_duration
                        });
                        let sleep_for = std::cmp::min(
                            max_delay.clone() - current_timestamp,
                            Duration::from_millis(500),
                        );
                        tokio::spawn(async move {
                            tokio::time::sleep(sleep_for).await;
                            let _ = command_sender
                                .send(ConsensusCommand::StartNewSlot(Some(max_delay)));
                        });
                        return Ok(());
                    }
                    cut
                }
                Ok(Err(err)) | Err(err) => {
                    // In case of an error, we reuse the last cut to avoid being considered byzantine
                    // (we also never delay because we already delayed by at least slot_duration)
                    error!(
                        "Could not get a new cut from Mempool {:?}. Reusing previous one...",
                        err
                    );
                    self.bft_round_state.parent_cut.clone()
                }
            };

            // TODO: keep candidates around?
            let mut new_validators_to_bond = std::mem::take(&mut self.validator_candidates);
            new_validators_to_bond.retain(|v| {
                self.bft_round_state
                    .staking
                    .get_stake(&v.signature.validator)
                    .unwrap_or(0)
                    > MIN_STAKE
                    && !self
                        .bft_round_state
                        .staking
                        .is_bonded(&v.signature.validator)
            });

            debug!(
                "🚀 Starting new slot {} (view {}) with {} existing validators and {} candidates. Cut: {}",
                self.bft_round_state.slot,
                self.bft_round_state.view,
                self.bft_round_state.staking.bonded().len(),
                new_validators_to_bond.len(),
                CutDisplay(&cut)
            );

            let mut staking_actions: Vec<ConsensusStakingAction> = new_validators_to_bond
                .into_iter()
                .map(|v| v.into())
                .collect();

            for tx in cut.iter() {
                debug!("📦 Lane {} cumulated size: {}", tx.0, tx.2);
                staking_actions.push(ConsensusStakingAction::PayFeesForDaDi {
                    lane_id: tx.0.clone(),
                    cumul_size: tx.2,
                });
            }

            // Start Consensus with following cut
            self.bft_round_state.current_proposal = ConsensusProposal {
                slot: self.bft_round_state.slot,
                cut,
                staking_actions,
                timestamp: current_timestamp,
                parent_hash: self.bft_round_state.parent_hash.clone(),
            };
        }
        self.bft_round_state.leader.step = Step::PrepareVote;

        let prepare = (
            self.crypto.validator_pubkey().clone(),
            self.bft_round_state.current_proposal.clone(),
            ticket.clone(),
            self.bft_round_state.view,
        );
        self.follower_state().buffered_prepares.push(prepare);

        self.metrics.start_new_round(self.bft_round_state.slot);

        // Verifies that to-be-built block is large enough (?)

        // Broadcasts Prepare message to all validators
        debug!(
            proposal_hash = %self.bft_round_state.current_proposal.hashed(),
            "🌐 Slot {} started. Broadcasting Prepare message", self.bft_round_state.slot,
        );
        self.broadcast_net_message(ConsensusNetMessage::Prepare(
            self.bft_round_state.current_proposal.clone(),
            ticket,
            self.bft_round_state.view,
        ))?;

        Ok(())
    }

    pub(super) fn is_round_leader(&self) -> bool {
        matches!(self.bft_round_state.state_tag, StateTag::Leader)
    }

    pub(super) fn on_prepare_vote(&mut self, prepare_vote: PrepareVote) -> Result<()> {
        if !matches!(self.bft_round_state.state_tag, StateTag::Leader) {
            debug!(
                sender = %prepare_vote.signature.validator,
                proposal_hash = %prepare_vote.msg.0,
                "PrepareVote received while not leader. Ignoring."
            );
            return Ok(());
        }
        if !matches!(self.bft_round_state.leader.step, Step::PrepareVote) {
            debug!(
                proposal_hash = %prepare_vote.msg.0,
                sender = %prepare_vote.signature.validator,
                "PrepareVote received at wrong step (step = {:?})",
                self.bft_round_state.leader.step
            );
            return Ok(());
        }

        // Verify that the PrepareVote is for the correct proposal.
        // This also checks slot/view as those are part of the hash.
        if prepare_vote.msg.0 != self.bft_round_state.current_proposal.hashed() {
            bail!("PrepareVote has not received valid consensus proposal hash");
        }

        // Save vote message
        self.store
            .bft_round_state
            .leader
            .prepare_votes
            .insert(prepare_vote);

        // Get matching vote count
        let validated_votes = self
            .bft_round_state
            .leader
            .prepare_votes
            .iter()
            .map(|signed_message| signed_message.signature.validator.clone())
            .collect::<Vec<ValidatorPublicKey>>();

        let votes_power = self
            .bft_round_state
            .staking
            .compute_voting_power(&validated_votes);
        let voting_power = votes_power + self.get_own_voting_power();

        // Waits for at least n-f = 2f+1 matching PrepareVote messages
        let f = self.bft_round_state.staking.compute_f();

        debug!(
            "📩 Slot {} validated votes: {} / {} ({} validators for a total bond = {})",
            self.bft_round_state.slot,
            voting_power,
            2 * f + 1,
            self.bft_round_state.staking.bonded().len(),
            self.bft_round_state.staking.total_bond()
        );

        if voting_power > 2 * f {
            // Get all received signatures
            let aggregates: &Vec<&PrepareVote> =
                &self.bft_round_state.leader.prepare_votes.iter().collect();

            let proposal_hash_hint = self.bft_round_state.current_proposal.hashed();
            // Aggregates them into a *Prepare* Quorum Certificate
            let prepvote_signed_aggregation = self
                .crypto
                .sign_aggregate((proposal_hash_hint.clone(), PrepareVoteMarker), aggregates)?;

            // Process the Confirm message locally, then send it to peers.
            self.bft_round_state.leader.step = Step::ConfirmAck;

            // if fast-path ... TODO
            // else send Confirm message to validators

            // Broadcast the *Prepare* Quorum Certificate to all validators
            debug!(
                "Slot {} PrepareVote message validated. Broadcasting Confirm",
                self.bft_round_state.slot
            );
            self.broadcast_net_message(ConsensusNetMessage::Confirm(
                QuorumCertificate(prepvote_signed_aggregation.signature, PrepareVoteMarker),
                proposal_hash_hint,
            ))?;
        }
        // TODO(?): Update behaviour when having more ?
        // else if validated_votes > 2 * f + 1 {}
        Ok(())
    }

    pub(super) fn on_confirm_ack(&mut self, confirm_ack: ConfirmAck) -> Result<()> {
        if !matches!(self.bft_round_state.state_tag, StateTag::Leader) {
            debug!(
                proposal_hash = %confirm_ack.msg.0,
                sender = %confirm_ack.signature.validator,
                "ConfirmAck received while not leader"
            );
            return Ok(());
        }

        if !matches!(self.bft_round_state.leader.step, Step::ConfirmAck) {
            debug!(
                proposal_hash = %confirm_ack.msg.0,
                sender = %confirm_ack.signature.validator,
                "ConfirmAck received at wrong step (step ={:?})",
                self.bft_round_state.leader.step
            );
            return Ok(());
        }

        // Verify that the ConfirmAck is for the correct proposal
        if confirm_ack.msg.0 != self.bft_round_state.current_proposal.hashed() {
            debug!(
                sender = %confirm_ack.signature.validator,
                "Got {} expected {}",
                confirm_ack.msg.0,
                self.bft_round_state.current_proposal.hashed()
            );
            bail!("ConfirmAck got invalid consensus proposal hash");
        }

        // Save ConfirmAck. Ends if the message already has been processed
        if !self
            .store
            .bft_round_state
            .leader
            .confirm_ack
            .insert(confirm_ack.clone())
        {
            trace!("ConfirmAck has already been processed");

            return Ok(());
        }

        // Compute voting power so far and hope for >= 2f+1
        let confirmed_ack_validators = self
            .bft_round_state
            .leader
            .confirm_ack
            .iter()
            .map(|signed_message| signed_message.signature.validator.clone())
            .collect::<Vec<ValidatorPublicKey>>();

        let confirmed_power = self
            .bft_round_state
            .staking
            .compute_voting_power(&confirmed_ack_validators);
        let voting_power = confirmed_power + self.get_own_voting_power();

        let f = self.bft_round_state.staking.compute_f();

        debug!(
            "✅ Slot {} confirmed acks: {} / {} ({} validators for a total bond = {})",
            self.bft_round_state.slot,
            voting_power,
            2 * f + 1,
            self.bft_round_state.staking.bonded().len(),
            self.bft_round_state.staking.total_bond()
        );

        if voting_power > 2 * f {
            // Get all signatures received and change ValidatorPublicKey for ValidatorPubKey
            let aggregates: &Vec<&ConfirmAck> =
                &self.bft_round_state.leader.confirm_ack.iter().collect();

            // Aggregates them into a *Commit* Quorum Certificate
            let commit_signed_aggregation = self.crypto.sign_aggregate(
                (
                    self.bft_round_state.current_proposal.hashed(),
                    ConfirmAckMarker,
                ),
                aggregates,
            )?;

            // Buffers the *Commit* Quorum Certificate
            let commit_quorum_certificate =
                QuorumCertificate(commit_signed_aggregation.signature, ConfirmAckMarker);

            // Broadcast the *Commit* Quorum Certificate to all validators
            self.broadcast_net_message(ConsensusNetMessage::Commit(
                commit_quorum_certificate.clone(),
                confirm_ack.msg.0,
            ))?;

            // Process the same locally.
            self.verify_commit_quorum_certificate_against_current_proposal(
                &commit_quorum_certificate,
            )?;
            self.emit_commit_event(&commit_quorum_certificate)?;
            self.advance_round(Ticket::CommitQC(commit_quorum_certificate))?;
        }
        // TODO(?): Update behaviour when having more ?
        Ok(())
    }
}
