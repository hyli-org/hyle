use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use anyhow::{bail, Context};
use borsh::{BorshDeserialize, BorshSerialize};
use hyle_crypto::BlstCrypto;
use sdk::{hyle_model_utils::TimestampMs, SignedByValidator, ValidatorPublicKey};
use tokio::{
    task::{AbortHandle, JoinSet},
    time::Interval,
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    clock::TimestampMsClock,
    metrics::P2PMetrics,
    ordered_join_set::OrderedJoinSet,
    tcp::{tcp_client::TcpClient, Handshake},
};

use super::{tcp_server::TcpServer, Canal, NodeConnectionData, P2PTcpMessage, TcpEvent};

#[derive(Debug)]
pub enum P2PServerEvent<Msg> {
    NewPeer {
        name: String,
        pubkey: ValidatorPublicKey,
        height: u64,
        da_address: String,
    },
    P2PMessage {
        msg: Msg,
    },
}

#[derive(Debug)]
pub enum P2PTcpEvent<Data: BorshDeserialize + BorshSerialize> {
    TcpEvent(TcpEvent<Data>),
    HandShakeTcpClient(String, TcpClient<Data, Data>, Canal),
    PingPeers,
}

#[derive(Clone, Debug)]
pub struct PeerSocket {
    // Timestamp of the lastest handshake
    timestamp: TimestampMs,
    // This is the socket_addr used in the tcp_server for the current peer
    pub socket_addr: String,
}

#[derive(Clone, Debug)]
pub struct PeerInfo {
    // Hashmap containing a sockets for all canals of this peer
    pub canals: HashMap<Canal, PeerSocket>,
    // The address that will be used to reconnect to that peer
    #[allow(dead_code)]
    pub node_connection_data: NodeConnectionData,
}

type HandShakeJoinSet<Data> = JoinSet<(String, anyhow::Result<TcpClient<Data, Data>>, Canal)>;

type CanalJob = (HashSet<ValidatorPublicKey>, Result<Vec<u8>, std::io::Error>);
type CanalJobResult = (
    Canal,
    HashSet<ValidatorPublicKey>,
    Result<Vec<u8>, std::io::Error>,
);

#[derive(Debug)]
pub enum HandshakeOngoing {
    TcpClientStartedAt(TimestampMs, AbortHandle),
    HandshakeStartedAt(String, TimestampMs),
}

/// P2PServer is a wrapper around TcpServer that manages peer connections
/// Its role is to process a full handshake with a peer, in order to get its public key.
/// Once handshake is done, the peer is added to the list of peers.
/// The connection is kept alive, hance restarted if it disconnects.
pub struct P2PServer<Msg>
where
    Msg: std::fmt::Debug + BorshDeserialize + BorshSerialize,
{
    // Crypto object used to sign and verify messages
    crypto: Arc<BlstCrypto>,
    node_id: String,
    metrics: P2PMetrics,
    // Hashmap containing the last attempts to connect
    pub connecting: HashMap<(String, Canal), HandshakeOngoing>,
    node_p2p_public_address: String,
    node_da_public_address: String,
    pub current_height: u64,
    max_frame_length: Option<usize>,
    pub tcp_server: TcpServer<P2PTcpMessage<Msg>, P2PTcpMessage<Msg>>,
    pub peers: HashMap<ValidatorPublicKey, PeerInfo>,
    handshake_clients_tasks: HandShakeJoinSet<P2PTcpMessage<Msg>>,
    peers_ping_ticker: Interval,
    // Serialization of messages can take time so we offload them.
    canal_jobs: HashMap<Canal, OrderedJoinSet<CanalJob>>,
    _phantom: std::marker::PhantomData<Msg>,
}

impl<Msg> P2PServer<Msg>
where
    Msg: std::fmt::Debug + BorshDeserialize + BorshSerialize + Send + 'static,
{
    pub async fn new(
        crypto: Arc<BlstCrypto>,
        node_id: String,
        port: u16,
        max_frame_length: Option<usize>,
        node_p2p_public_address: String,
        node_da_public_address: String,
        canals: HashSet<Canal>,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            crypto,
            node_id: node_id.clone(),
            metrics: P2PMetrics::global(node_id.clone()),
            connecting: HashMap::default(),
            max_frame_length,
            node_p2p_public_address,
            node_da_public_address,
            current_height: 0,
            tcp_server: TcpServer::start_with_opts(
                port,
                max_frame_length,
                format!("P2P-{}", node_id).as_str(),
            )
            .await?,
            peers: HashMap::new(),
            handshake_clients_tasks: JoinSet::new(),
            peers_ping_ticker: tokio::time::interval(std::time::Duration::from_secs(2)),
            canal_jobs: canals
                .into_iter()
                .map(|canal| (canal, OrderedJoinSet::new()))
                .collect(),
            _phantom: std::marker::PhantomData,
        })
    }

    fn poll_hashmap(
        jobs: &mut HashMap<Canal, OrderedJoinSet<CanalJob>>,
        cx: &mut std::task::Context,
    ) -> Poll<CanalJobResult> {
        for (canal, jobs) in jobs.iter_mut() {
            if let Poll::Ready(Some(result)) = jobs.poll_join_next(cx) {
                match result {
                    Ok((p, r)) => {
                        return Poll::Ready((canal.clone(), p, r));
                    }
                    Err(e) => {
                        warn!("Error in canal jobs: {:?}", e);
                    }
                }
            }
        }
        Poll::Pending
    }

    pub async fn listen_next(&mut self) -> P2PTcpEvent<P2PTcpMessage<Msg>> {
        // Await either of the joinsets in the self.canal_jobs hashmap

        loop {
            tokio::select! {
                Some(tcp_event) = self.tcp_server.listen_next() => {
                    return P2PTcpEvent::TcpEvent(tcp_event);
                },
                Some(joinset_result) = self.handshake_clients_tasks.join_next() => {
                    match joinset_result {
                        Ok(task_result) =>{
                            if let (public_addr, Ok(tcp_client), canal) = task_result {
                                return P2PTcpEvent::HandShakeTcpClient(public_addr, tcp_client, canal);
                            }
                            else {
                                warn!("Error during TcpClient connection, retrying on {}/{}", task_result.0, task_result.2);
                                _ = self.try_start_connection(task_result.0, task_result.2);
                                continue
                            }
                        },
                        Err(e) =>
                        {
                            debug!("Error during joinset execution of handshake task (probably): {:?}", e);
                            continue
                        }
                    }
                },
                (canal, pubkeys, data) = std::future::poll_fn(|cx| Self::poll_hashmap(&mut self.canal_jobs, cx)) => {
                    let Ok(msg) = data else {
                        warn!("Error in canal jobs: {:?}", data);
                        continue
                    };
                    // TODO: handle errors?
                    self.actually_send_to(pubkeys, canal, msg).await;
                }
                _ = self.peers_ping_ticker.tick() => {
                    return P2PTcpEvent::PingPeers;
                }
            }
        }
    }

    /// Handle a P2PTCPEvent. This is done as separate function to easily handle async tasks
    pub async fn handle_p2p_tcp_event(
        &mut self,
        p2p_tcp_event: P2PTcpEvent<P2PTcpMessage<Msg>>,
    ) -> anyhow::Result<Option<P2PServerEvent<Msg>>> {
        match p2p_tcp_event {
            P2PTcpEvent::TcpEvent(tcp_event) => match tcp_event {
                TcpEvent::Message {
                    dest,
                    data: P2PTcpMessage::Handshake(handshake),
                } => self.handle_handshake(dest, handshake).await,
                TcpEvent::Message {
                    dest,
                    data: P2PTcpMessage::Data(msg),
                } => {
                    if let Some(peer) = self.get_peer_by_socket_addr(&dest) {
                        self.metrics.message_received(
                            peer.1.node_connection_data.p2p_public_address.clone(),
                            peer.0.clone(),
                        );
                    }
                    Ok(Some(P2PServerEvent::P2PMessage { msg }))
                }
                TcpEvent::Error { dest, error } => {
                    if let Some(peer) = self.get_peer_by_socket_addr(&dest) {
                        self.metrics.message_error(
                            peer.1.node_connection_data.p2p_public_address.clone(),
                            peer.0.clone(),
                        );
                    }
                    self.handle_error_event(dest, error).await;
                    Ok(None)
                }
                TcpEvent::Closed { dest } => {
                    if let Some(peer) = self.get_peer_by_socket_addr(&dest) {
                        self.metrics.message_closed(
                            peer.1.node_connection_data.p2p_public_address.clone(),
                            peer.0.clone(),
                        );
                    }
                    self.handle_closed_event(dest);
                    Ok(None)
                }
            },
            P2PTcpEvent::HandShakeTcpClient(public_addr, tcp_client, canal) => {
                if let Err(e) = self
                    .do_handshake(public_addr.clone(), tcp_client, canal.clone())
                    .await
                {
                    warn!("Error during handshake: {:?}", e);
                    let _ = self.try_start_connection(public_addr, canal);
                }
                Ok(None)
            }
            P2PTcpEvent::PingPeers => {
                let sockets: Vec<(ValidatorPublicKey, Canal, String, PeerSocket)> = self
                    .peers
                    .iter()
                    .flat_map(move |(k, v)| {
                        let cloned = k.clone();
                        let addr = v.node_connection_data.da_public_address.clone();
                        v.canals
                            .iter()
                            .map(move |(c, s)| (cloned.clone(), c.clone(), addr.clone(), s.clone()))
                    })
                    .collect();

                for (pubkey, canal, public_addr, socket) in sockets {
                    self.metrics.ping(public_addr, canal.clone());
                    if let Err(e) = self.tcp_server.ping(socket.socket_addr.clone()).await {
                        debug!("Error pinging peer {}: {:?}", socket.socket_addr, e);
                        let _ = self.try_start_connection_for_peer(&pubkey, canal.clone());
                    }
                }
                Ok(None)
            }
        }
    }

    pub fn find_socket_addr(&self, canal: &Canal, vid: &ValidatorPublicKey) -> Option<&String> {
        self.peers
            .get(vid)
            .and_then(|p| p.canals.get(canal).map(|socket| &socket.socket_addr))
    }

    pub fn get_socket_mut(
        &mut self,
        canal: &Canal,
        vid: &ValidatorPublicKey,
    ) -> Option<&mut PeerSocket> {
        self.peers
            .get_mut(vid)
            .and_then(|p| p.canals.get_mut(canal))
    }

    pub fn get_peer_by_socket_addr(
        &self,
        dest: &String,
    ) -> Option<(&Canal, &PeerInfo, &PeerSocket)> {
        self.peers.values().find_map(|peer_info| {
            peer_info
                .canals
                .iter()
                .find(|(_canal, peer_socket)| &peer_socket.socket_addr == dest)
                .map(|(canal, peer_socket)| (canal, peer_info, peer_socket))
        })
    }

    async fn handle_error_event(
        &mut self,
        dest: String,
        _error: String,
    ) -> Option<P2PServerEvent<Msg>> {
        warn!("Error with peer connection: {:?}", _error);
        // There was an error with the connection with the peer. We try to reconnect.

        // TODO: An error can happen when a message was no *sent* correctly. Investigate how to handle that specific case
        // TODO: match the error type to decide what to do
        self.tcp_server.drop_peer_stream(dest.clone());
        if let Some((canal, info, _)) = self.get_peer_by_socket_addr(&dest) {
            self.start_connection_task(
                info.node_connection_data.p2p_public_address.clone(),
                canal.clone(),
            )
        }
        None
    }

    fn handle_closed_event(&mut self, dest: String) {
        // TODO: investigate how to properly handle this case
        // The connection has been closed by peer. We remove the peer and try to reconnect.

        // When we receive a close event
        // It is a closed connection that need to be removed from tcp server clients in all cases
        // If it is a connection matching a canal/peer, it means we can retry
        self.tcp_server.drop_peer_stream(dest.clone());
        if let Some((canal, info, _)) = self.get_peer_by_socket_addr(&dest) {
            self.start_connection_task(
                info.node_connection_data.p2p_public_address.clone(),
                canal.clone(),
            )
        }
    }

    async fn handle_handshake(
        &mut self,
        dest: String,
        handshake: Handshake,
    ) -> anyhow::Result<Option<P2PServerEvent<Msg>>> {
        match handshake {
            Handshake::Hello((canal, v, timestamp)) => {
                self.metrics
                    .handshake_hello_received(v.msg.p2p_public_address.clone(), canal.clone());

                // Verify message signature
                BlstCrypto::verify(&v).context("Error verifying Hello message")?;

                info!(
                    "👋 [{}] Processing Hello handshake message {:?}",
                    canal, v.msg
                );
                match self.create_signed_node_connection_data() {
                    Ok(verack) => {
                        // Send Verack response
                        if let Err(e) = self
                            .tcp_server
                            .send(
                                dest.clone(),
                                P2PTcpMessage::<Msg>::Handshake(Handshake::Verack((
                                    canal.clone(),
                                    verack,
                                    timestamp.clone(),
                                ))),
                            )
                            .await
                        {
                            bail!("Error sending Verack message to {dest}: {:?}", e);
                        }

                        self.metrics.handshake_verack_emitted(
                            v.msg.p2p_public_address.clone(),
                            canal.clone(),
                        );
                    }
                    Err(e) => {
                        bail!("Error creating signed node connection data: {:?}", e);
                    }
                }

                Ok(self.handle_peer_update(canal, &v, timestamp, dest))
            }
            Handshake::Verack((canal, v, timestamp)) => {
                self.metrics
                    .handshake_verack_received(v.msg.p2p_public_address.clone(), canal.clone());

                // Verify message signature
                BlstCrypto::verify(&v).context("Error verifying Verack message")?;

                info!(
                    "👋 [{}] Processing Verack handshake message {:?}",
                    canal, v.msg
                );
                Ok(self.handle_peer_update(canal, &v, timestamp, dest))
            }
        }
    }

    fn handle_peer_update(
        &mut self,
        canal: Canal,
        v: &SignedByValidator<NodeConnectionData>,
        timestamp: TimestampMs,
        dest: String,
    ) -> Option<P2PServerEvent<Msg>> {
        let peer_pubkey = v.signature.validator.clone();

        // in case timestamps are equal -_-
        let local_pubkey = self.crypto.validator_pubkey().clone();

        self.connecting
            .remove(&(v.msg.p2p_public_address.clone(), canal.clone()));

        if let Some(peer_socket) = self.get_socket_mut(&canal, &peer_pubkey) {
            let peer_addr_to_drop = if peer_socket.timestamp < timestamp || {
                peer_socket.timestamp == timestamp
                    && local_pubkey.cmp(&peer_pubkey) == Ordering::Less
            } {
                debug!(
                    "Local peer {}/{} ({}): dropping socket {} in favor of more recent one {}",
                    v.msg.p2p_public_address, canal, peer_pubkey, peer_socket.socket_addr, dest
                );
                let socket_addr = peer_socket.socket_addr.clone();
                peer_socket.timestamp = timestamp;
                peer_socket.socket_addr = dest.clone();
                socket_addr.clone()
            } else {
                debug!(
                    "Local peer {}/{} ({}): keeping socket {} and discard too old {}",
                    v.msg.p2p_public_address, canal, peer_pubkey, peer_socket.socket_addr, dest
                );
                dest
            };
            self.tcp_server.drop_peer_stream(peer_addr_to_drop);
            None
        } else {
            // If the validator exists, but not this canal, we create it
            if let Some(validator) = self.peers.get_mut(&peer_pubkey) {
                debug!(
                    "Local peer {}/{} ({}): creating canal for existing peer on socket {}",
                    v.msg.p2p_public_address, canal, peer_pubkey, dest
                );
                validator.canals.insert(
                    canal.clone(),
                    PeerSocket {
                        timestamp,
                        socket_addr: dest,
                    },
                );
            }
            // If the validator was never created before
            else {
                debug!(
                    "Local peer {}/{} ({}): creating new peer and canal on socket {}",
                    v.msg.p2p_public_address, canal, peer_pubkey, dest
                );
                let peer_info = PeerInfo {
                    canals: HashMap::from_iter(vec![(
                        canal.clone(),
                        PeerSocket {
                            timestamp,
                            socket_addr: dest,
                        },
                    )]),
                    node_connection_data: v.msg.clone(),
                };

                self.peers.insert(peer_pubkey.clone(), peer_info);
            }

            self.metrics.peers_snapshot(self.peers.len() as u64);
            tracing::info!("New peer connected on canal {}: {}", canal, peer_pubkey);
            Some(P2PServerEvent::NewPeer {
                name: v.msg.name.to_string(),
                pubkey: v.signature.validator.clone(),
                da_address: v.msg.da_public_address.clone(),
                height: v.msg.current_height,
            })
        }
    }

    pub fn remove_peer(&mut self, peer_pubkey: &ValidatorPublicKey, canal: Canal) {
        if let Some(peer_info) = self.peers.get_mut(peer_pubkey) {
            if let Some(removed) = peer_info.canals.remove(&canal) {
                self.tcp_server
                    .drop_peer_stream(removed.socket_addr.clone());
            }
        }
    }

    // TODO: Make P2P server generic with this payload ? This data is 100% custom
    /// Create a payload with data that will be transmitted during handshake
    fn create_signed_node_connection_data(
        &self,
    ) -> anyhow::Result<SignedByValidator<NodeConnectionData>> {
        let node_connection_data = NodeConnectionData {
            version: 1,
            name: self.node_id.clone(),
            current_height: self.current_height,
            p2p_public_address: self.node_p2p_public_address.clone(),
            da_public_address: self.node_da_public_address.clone(),
        };
        self.crypto.sign(node_connection_data)
    }

    fn try_start_connection_for_peer(
        &mut self,
        pubkey: &ValidatorPublicKey,
        canal: Canal,
    ) -> anyhow::Result<()> {
        let peer = self
            .peers
            .get(pubkey)
            .context(format!("Peer not found {}", pubkey))?;

        tracing::info!(
            "Attempt to reconnect to {}/{}",
            peer.node_connection_data.p2p_public_address,
            canal
        );

        self.try_start_connection(peer.node_connection_data.p2p_public_address.clone(), canal)?;

        Ok(())
    }

    /// Checks if creating a fresh tcp client is relevant and do it if so
    /// Start a task, cancellation safe
    pub fn try_start_connection(
        &mut self,
        peer_address: String,
        canal: Canal,
    ) -> anyhow::Result<()> {
        if peer_address == self.node_p2p_public_address {
            trace!("Trying to connect to self");
            return Ok(());
        }

        let now = TimestampMsClock::now();

        // A connection is already started for this public address ? If it is too old, let 's try retry one
        // If it is recent, let's wait for it to finish
        if let Some(ongoing) = self.connecting.get(&(peer_address.clone(), canal.clone())) {
            match ongoing {
                HandshakeOngoing::TcpClientStartedAt(last_connect_attempt, abort_handle) => {
                    if now.clone() - last_connect_attempt.clone() < Duration::from_secs(3) {
                        {
                            return Ok(());
                        }
                    }
                    abort_handle.abort();
                }
                HandshakeOngoing::HandshakeStartedAt(addr, last_handshake_started_at) => {
                    if now.clone() - last_handshake_started_at.clone() < Duration::from_secs(3) {
                        {
                            return Ok(());
                        }
                    }
                    self.tcp_server.drop_peer_stream(addr.to_string());
                }
            }
        }

        self.start_connection_task(peer_address, canal);
        Ok(())
    }

    /// Creates a task that attempts to create a tcp client
    pub fn start_connection_task(&mut self, peer_address: String, canal: Canal) {
        let mfl = self.max_frame_length;
        let now = TimestampMsClock::now();
        let peer_address_clone = peer_address.clone();
        let canal_clone = canal.clone();

        tracing::info!("Starting connecting to {}/{}", peer_address, canal);

        let abort_handle = self.handshake_clients_tasks.spawn(async move {
            let handshake_task = TcpClient::connect_with_opts(
                "p2p_server_handshake",
                mfl,
                peer_address_clone.clone(),
            );

            let result = handshake_task.await;
            (peer_address_clone, result, canal_clone)
        });

        self.connecting.insert(
            (peer_address.clone(), canal.clone()),
            HandshakeOngoing::TcpClientStartedAt(now, abort_handle),
        );

        self.metrics
            .handshake_connection_emitted(peer_address.clone(), canal);
    }

    async fn do_handshake(
        &mut self,
        public_addr: String,
        tcp_client: TcpClient<P2PTcpMessage<Msg>, P2PTcpMessage<Msg>>,
        canal: Canal,
    ) -> anyhow::Result<()> {
        let signed_node_connection_data = self.create_signed_node_connection_data()?;
        let timestamp = TimestampMsClock::now();

        debug!(
            "Doing handshake on {}({})/{}",
            public_addr,
            tcp_client.socket_addr.to_string(),
            canal
        );

        let addr = format!("{}/{}", public_addr, canal);

        self.connecting.insert(
            (public_addr.clone(), canal.clone()),
            HandshakeOngoing::HandshakeStartedAt(addr.clone(), timestamp.clone()),
        );

        self.tcp_server.setup_client(addr.clone(), tcp_client);
        self.tcp_server
            .send(
                addr,
                P2PTcpMessage::<Msg>::Handshake(Handshake::Hello((
                    canal.clone(),
                    signed_node_connection_data.clone(),
                    timestamp,
                ))),
            )
            .await?;

        self.metrics.handshake_hello_emitted(public_addr, canal);

        Ok(())
    }

    pub async fn send(
        &mut self,
        validator_pub_key: ValidatorPublicKey,
        canal: Canal,
        msg: Msg,
    ) -> anyhow::Result<()> {
        if let Some(jobs) = self.canal_jobs.get_mut(&canal) {
            if !jobs.is_empty() {
                jobs.spawn(async move {
                    (
                        HashSet::from_iter(std::iter::once(validator_pub_key)),
                        borsh::to_vec(&P2PTcpMessage::Data(msg)),
                    )
                });
                return Ok(());
            }
        }

        let (pub_addr, peer_info) = match self.peers.get(&validator_pub_key).and_then(|peer| {
            let addr = peer.node_connection_data.p2p_public_address.clone();
            peer.canals.get(&canal).map(|c| (addr, c))
        }) {
            Some((addr, info)) => (addr, info),
            None => {
                warn!(
                    "Trying to send message to unknown Peer {}/{}. Unable to proceed.",
                    validator_pub_key, canal
                );
                return Ok(());
            }
        };

        if let Err(e) = self
            .tcp_server
            .send(peer_info.socket_addr.clone(), P2PTcpMessage::Data(msg))
            .await
        {
            self.try_start_connection_for_peer(&validator_pub_key, canal)
                .context(format!(
                    "Re-handshaking after message sending error with peer {}",
                    validator_pub_key
                ))?;
            bail!(
                "Failed to send message to peer {}: {:?}",
                validator_pub_key,
                e
            );
        }

        self.metrics.message_emitted(pub_addr, canal);
        Ok(())
    }

    pub fn broadcast(&mut self, msg: Msg, canal: Canal) {
        let Some(jobs) = self.canal_jobs.get_mut(&canal) else {
            error!("Canal {:?} does not exist in P2P server", canal);
            return;
        };
        let peers = self.peers.keys().cloned().collect();
        jobs.spawn(async move { (peers, borsh::to_vec(&P2PTcpMessage::Data(msg))) });
    }

    pub fn broadcast_only_for(
        &mut self,
        only_for: &HashSet<ValidatorPublicKey>,
        canal: Canal,
        msg: Msg,
    ) {
        let Some(jobs) = self.canal_jobs.get_mut(&canal) else {
            error!("Canal {:?} does not exist in P2P server", canal);
            return;
        };
        let peers = only_for.clone();
        jobs.spawn(async move { (peers, borsh::to_vec(&P2PTcpMessage::Data(msg))) });
    }

    async fn actually_send_to(
        &mut self,
        only_for: HashSet<ValidatorPublicKey>,
        canal: Canal,
        msg: Vec<u8>,
    ) -> HashMap<ValidatorPublicKey, anyhow::Error> {
        let peer_addr_to_pubkey: HashMap<String, ValidatorPublicKey> = self
            .peers
            .iter()
            .filter_map(|(pubkey, peer)| {
                if only_for.contains(pubkey) {
                    peer.canals
                        .get(&canal)
                        .map(|socket| (socket.socket_addr.clone(), pubkey.clone()))
                } else {
                    None
                }
            })
            .collect();

        let res = self
            .tcp_server
            .raw_send_parallel(peer_addr_to_pubkey.keys().cloned().collect(), msg)
            .await;

        HashMap::from_iter(res.into_iter().filter_map(|(k, v)| {
            peer_addr_to_pubkey.get(&k).map(|pubkey| {
                error!("Error sending message to {} during broadcast: {}", k, v);
                if let Err(e) = self.try_start_connection_for_peer(pubkey, canal.clone()) {
                    warn!("Problem when triggering re-handshake after message sending error with peer {}/{}: {}", pubkey, canal, e);
                }
                (pubkey.clone(), v)
            })
        }))
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;

    use anyhow::Result;
    use borsh::{BorshDeserialize, BorshSerialize};
    use hyle_crypto::BlstCrypto;
    use tokio::net::TcpListener;

    use crate::tcp::{p2p_server::P2PServer, Canal, Handshake, P2PTcpMessage, TcpEvent};

    use super::P2PTcpEvent;

    pub async fn find_available_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        addr.port()
    }
    // Simple message type for testing
    #[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq, PartialOrd, Ord)]
    pub struct TestMessage(String);

    macro_rules! receive_and_handle_event {
        ($server:expr, $pattern:pat, $error_msg:expr) => {{
            let event = receive_event($server, $error_msg).await?;
            assert!(
                matches!(event, $pattern),
                "Expected {:?}, got {:?}",
                stringify!($pattern),
                event,
            );
            $server.handle_p2p_tcp_event(event).await?;
        }};
    }

    async fn setup_p2p_server_pair() -> Result<(
        (u16, super::P2PServer<TestMessage>),
        (u16, super::P2PServer<TestMessage>),
    )> {
        let crypto1 = BlstCrypto::new_random().unwrap();
        let crypto2 = BlstCrypto::new_random().unwrap();

        let port1 = find_available_port().await;
        let port2 = find_available_port().await;

        tracing::info!("Starting P2P server1 on port {port1}");
        tracing::info!("Starting P2P server2 on port {port2}");
        let p2p_server1 = P2PServer::new(
            crypto1.into(),
            "node1".to_string(),
            port1,
            None,
            format!("127.0.0.1:{port1}"),
            "127.0.0.1:4321".into(), // send some dummy address for DA,
            HashSet::from_iter(vec![Canal::new("A"), Canal::new("B")]),
        )
        .await?;
        let p2p_server2 = P2PServer::new(
            crypto2.into(),
            "node2".to_string(),
            port2,
            None,
            format!("127.0.0.1:{port2}"),
            "127.0.0.1:4321".into(), // send some dummy address for DA
            HashSet::from_iter(vec![Canal::new("A"), Canal::new("B")]),
        )
        .await?;

        Ok(((port1, p2p_server1), (port2, p2p_server2)))
    }

    async fn receive_event(
        p2p_server: &mut super::P2PServer<TestMessage>,
        error_msg: &str,
    ) -> Result<P2PTcpEvent<P2PTcpMessage<TestMessage>>> {
        loop {
            let timeout_duration = std::time::Duration::from_millis(100);

            match tokio::time::timeout(timeout_duration, p2p_server.listen_next()).await {
                Ok(event) => match event {
                    P2PTcpEvent::PingPeers => {
                        continue;
                    }
                    _ => return Ok(event),
                },
                Err(_) => {
                    anyhow::bail!("Timed out while waiting for message: {error_msg}");
                }
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn p2p_server_concurrent_handshake_test() -> Result<()> {
        let ((port1, mut p2p_server1), (port2, mut p2p_server2)) = setup_p2p_server_pair().await?;

        // Initiate handshake from p2p_server1 to p2p_server2
        _ = p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

        // Initiate handshake from p2p_server2 to p2p_server1
        _ = p2p_server2.try_start_connection(format!("127.0.0.1:{port1}"), Canal::new("A"));

        // For TcpClient to connect
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );

        // For handshake Hello message
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_))
            }),
            "Expected HandShake Hello message"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_))
            }),
            "Expected HandShake Hello message"
        );

        // For handshake Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_))
            }),
            "Expected HandShake Verack"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_))
            }),
            "Expected HandShake Verack"
        );

        // Verify that both servers have each other in their peers map
        assert_eq!(p2p_server1.peers.len(), 1);
        assert_eq!(p2p_server2.peers.len(), 1);

        assert_eq!(p2p_server1.peers.values().last().unwrap().canals.len(), 1);
        assert_eq!(p2p_server2.peers.values().last().unwrap().canals.len(), 1);

        // Both peers should have each other's ValidatorPublicKey in their maps
        let p1_peer_key = p2p_server1.peers.keys().next().unwrap();
        let p2_peer_key = p2p_server2.peers.keys().next().unwrap();
        assert_eq!(p1_peer_key.0, p2p_server2.crypto.validator_pubkey().0);
        assert_eq!(p2_peer_key.0, p2p_server1.crypto.validator_pubkey().0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn p2p_server_multi_canal_handshake() -> Result<()> {
        let ((port1, mut p2p_server1), (port2, mut p2p_server2)) = setup_p2p_server_pair().await?;

        // Initiate handshake from p2p_server1 to p2p_server2 on canal A
        let _ = p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

        // Initiate handshake from p2p_server2 to p2p_server1 on canal A
        let _ = p2p_server2.try_start_connection(format!("127.0.0.1:{port1}"), Canal::new("A"));

        // Initiate handshake from p2p_server1 to p2p_server2 on canal B
        let _ = p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("B"));

        // Initiate handshake from p2p_server2 to p2p_server1 on canal B
        let _ = p2p_server2.try_start_connection(format!("127.0.0.1:{port1}"), Canal::new("B"));

        // For TcpClient to connect
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );

        // For handshake Hello message
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_))
            }),
            "Expected HandShake Hello message canal A"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_))
            }),
            "Expected HandShake Hello message canal B"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_))
            }),
            "Expected HandShake Hello message canal A"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_))
            }),
            "Expected HandShake Hello message canal B"
        );

        // For handshake Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_))
            }),
            "Expected HandShake Verack"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_))
            }),
            "Expected HandShake Verack"
        );

        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_))
            }),
            "Expected HandShake Verack"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_))
            }),
            "Expected HandShake Verack"
        );
        // Verify that both servers have each other in their peers map
        assert_eq!(p2p_server1.peers.len(), 1);
        assert_eq!(p2p_server2.peers.len(), 1);

        assert_eq!(p2p_server1.peers.values().last().unwrap().canals.len(), 2);
        assert_eq!(p2p_server2.peers.values().last().unwrap().canals.len(), 2);

        assert_eq!(
            HashSet::<String>::from_iter(
                p2p_server1
                    .peers
                    .values()
                    .flat_map(|v| v.canals.values().map(|v2| v2.socket_addr.clone()))
                    .collect::<Vec<_>>()
            ),
            HashSet::from_iter(p2p_server1.tcp_server.connected_clients())
        );
        assert_eq!(
            HashSet::<String>::from_iter(
                p2p_server2
                    .peers
                    .values()
                    .flat_map(|v| v.canals.values().map(|v2| v2.socket_addr.clone()))
                    .collect::<Vec<_>>()
            ),
            HashSet::from_iter(p2p_server2.tcp_server.connected_clients())
        );

        // Both peers should have each other's ValidatorPublicKey in their maps
        let p1_peer_key = p2p_server1.peers.keys().next().unwrap();
        let p2_peer_key = p2p_server2.peers.keys().next().unwrap();
        assert_eq!(p1_peer_key.0, p2p_server2.crypto.validator_pubkey().0);
        assert_eq!(p2_peer_key.0, p2p_server1.crypto.validator_pubkey().0);

        _ = receive_event(&mut p2p_server2, "Should be a Closed event").await;
        _ = receive_event(&mut p2p_server1, "Should be a Closed event").await;
        _ = receive_event(&mut p2p_server2, "Should be a Closed event").await;
        _ = receive_event(&mut p2p_server1, "Should be a Closed event").await;

        // Canal A (1 -> 2)
        p2p_server1.broadcast(TestMessage("blabla".to_string()), Canal::new("A"));

        let evt = loop {
            p2p_server1.listen_next().await; // Process the waiting job.
            if let Ok(evt) = receive_event(&mut p2p_server2, "Should be a TestMessage event").await
            {
                break evt;
            }
        };

        assert!(matches!(
            evt,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Data(_)
            }),
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            dest: _,
            data: P2PTcpMessage::Data(data),
        }) = evt
        else {
            panic!("test");
        };

        assert_eq!(data, TestMessage("blabla".to_string()));

        // Canal B (1 -> 2)
        p2p_server1.broadcast(TestMessage("blabla2".to_string()), Canal::new("B"));
        let evt = loop {
            p2p_server1.listen_next().await; // Process the waiting job.
            if let Ok(evt) = receive_event(&mut p2p_server2, "Should be a TestMessage event").await
            {
                break evt;
            }
        };

        assert!(matches!(
            evt,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Data(_)
            }),
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            dest: _,
            data: P2PTcpMessage::Data(data),
        }) = evt
        else {
            panic!("test");
        };

        assert_eq!(data, TestMessage("blabla2".to_string()));

        // Canal A (2 -> 1)
        p2p_server2.broadcast(TestMessage("babal".to_string()), Canal::new("A"));

        let evt = loop {
            p2p_server2.listen_next().await; // Process the waiting job.
            if let Ok(evt) = receive_event(&mut p2p_server1, "Should be a TestMessage event").await
            {
                break evt;
            }
        };

        assert!(matches!(
            evt,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Data(_)
            }),
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            dest: _,
            data: P2PTcpMessage::Data(data),
        }) = evt
        else {
            panic!("test");
        };

        assert_eq!(data, TestMessage("babal".to_string()));

        // Canal B (2 -> 1)
        p2p_server2.broadcast(TestMessage("babal2".to_string()), Canal::new("B"));

        let evt = loop {
            p2p_server2.listen_next().await; // Process the waiting job.
            if let Ok(evt) = receive_event(&mut p2p_server1, "Should be a TestMessage event").await
            {
                break evt;
            }
        };

        assert!(matches!(
            evt,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Data(_)
            }),
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            dest: _,
            data: P2PTcpMessage::Data(data),
        }) = evt
        else {
            panic!("test");
        };

        assert_eq!(data, TestMessage("babal2".to_string()));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn p2p_server_reconnection_test() -> Result<()> {
        let ((_, mut p2p_server1), (port2, mut p2p_server2)) = setup_p2p_server_pair().await?;

        // Initial connection
        let _ = p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

        // Server1 waits for TcpClient to connect
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        // Server2 receives Hello message
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_))
            }),
            "Expected HandShake Hello message"
        );
        // Server1 receives Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_))
            }),
            "Expected HandShake Verack"
        );

        // Verify initial connection
        assert_eq!(p2p_server1.peers.len(), 1);

        // Simulate disconnection by dropping peer from server2
        p2p_server2.remove_peer(p2p_server1.crypto.validator_pubkey(), Canal::new("A"));

        // Server1 receives Closed message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Closed { dest: _ }),
            "Expected Tcp Error message"
        );

        // Server1 waits for TcpClient to reconnect
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        // Server2 receives Hello message
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_))
            }),
            "Expected HandShake Hello message"
        );
        // Server1 receives Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_))
            }),
            "Expected HandShake Verack"
        );

        assert_eq!(
            p2p_server1.peers.len(),
            1,
            "Server1 should connected to only server2"
        );
        assert_eq!(
            p2p_server2.peers.len(),
            1,
            "Server2 should have reconnected to server1"
        );

        Ok(())
    }
}
