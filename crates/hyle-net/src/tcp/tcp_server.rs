use std::{
    collections::HashMap,
    io::ErrorKind,
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Bytes;
use futures::{
    stream::{SplitSink, SplitStream},
    FutureExt, SinkExt, StreamExt,
};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::{
    clock::TimestampMsClock,
    logged_task::logged_task,
    metrics::TcpServerMetrics,
    net::{TcpListener, TcpStream},
    tcp::{to_tcp_message, TcpMessage},
};
use tracing::{debug, error, trace, warn};

use super::{tcp_client::TcpClient, SocketStream, TcpEvent};

pub struct TcpServer<Req, Res>
where
    Res: BorshSerialize + std::fmt::Debug,
    Req: BorshDeserialize + std::fmt::Debug,
{
    tcp_listener: TcpListener,
    max_frame_length: Option<usize>,
    pool_sender: Sender<Box<TcpEvent<Req>>>,
    pool_receiver: Receiver<Box<TcpEvent<Req>>>,
    ping_sender: Sender<String>,
    ping_receiver: Receiver<String>,
    sockets: HashMap<String, SocketStream>,
    metrics: TcpServerMetrics,
    _marker: PhantomData<(Req, Res)>,
}

impl<Req, Res> TcpServer<Req, Res>
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug,
{
    pub async fn start(port: u16, pool_name: &str) -> anyhow::Result<Self> {
        Self::start_with_opts(port, None, pool_name).await
    }

    pub async fn start_with_opts(
        port: u16,
        max_frame_length: Option<usize>,
        pool_name: &str,
    ) -> anyhow::Result<Self> {
        let tcp_listener = TcpListener::bind(&(Ipv4Addr::UNSPECIFIED, port)).await?;
        let (pool_sender, pool_receiver) = tokio::sync::mpsc::channel(100);
        let (ping_sender, ping_receiver) = tokio::sync::mpsc::channel(100);
        debug!(
            "Starting TcpConnectionPool {}, listening for stream requests on {} with max_frame_len: {:?}",
            &pool_name, port, max_frame_length
        );
        Ok(TcpServer {
            sockets: HashMap::new(),
            max_frame_length,
            tcp_listener,
            pool_sender,
            pool_receiver,
            ping_sender,
            ping_receiver,
            metrics: TcpServerMetrics::global(pool_name.to_string()),
            _marker: PhantomData,
        })
    }

    pub async fn listen_next(&mut self) -> Option<TcpEvent<Req>> {
        loop {
            tokio::select! {
                Ok((stream, socket_addr)) = self.tcp_listener.accept() => {
                    let mut codec = LengthDelimitedCodec::new();
                    if let Some(len) = self.max_frame_length {
                        codec.set_max_frame_length(len);
                    }

                    let (sender, receiver) = Framed::new(stream, codec).split();
                    self.setup_stream(sender, receiver, &socket_addr.to_string());
                }

                Some(socket_addr) = self.ping_receiver.recv() => {
                    trace!("Received ping from {}", socket_addr);
                    if let Some(socket) = self.sockets.get_mut(&socket_addr) {
                        socket.last_ping = TimestampMsClock::now();
                    }
                }
                message = self.pool_receiver.recv() => {
                    return message.map(|message| *message);
                }
            }
        }
    }

    /// Local_addr of the underlying tcp_listener
    pub fn local_addr(&self) -> anyhow::Result<SocketAddr> {
        self.tcp_listener
            .local_addr()
            .context("Getting local_addr from TcpListener in TcpServer")
    }

    /// Adresses of currently connected clients (no health check)
    pub fn connected_clients(&self) -> Vec<String> {
        self.sockets.keys().cloned().collect::<Vec<String>>()
    }

    pub async fn broadcast(&mut self, msg: Res) -> HashMap<String, anyhow::Error> {
        let mut tasks = vec![];

        let Ok(binary_data) = to_tcp_message(&msg) else {
            return self
                .sockets
                .iter()
                .map(|addr| {
                    (
                        addr.0.clone(),
                        anyhow::anyhow!("Failed to serialize message"),
                    )
                })
                .collect();
        };
        debug!("Broadcasting msg {:?} to all", binary_data);
        for (name, socket) in self.sockets.iter_mut() {
            debug!(" - to {}", name);
            tasks.push(
                socket
                    .sender
                    .send(binary_data.clone())
                    .map(|res| (name.clone(), res)),
            );
        }

        let all = futures::future::join_all(tasks).await;

        HashMap::from_iter(all.into_iter().filter_map(|(client_name, send_result)| {
            send_result.err().map(|error| {
                (
                    client_name.clone(),
                    anyhow::anyhow!("Sending message to client {}: {}", client_name, error),
                )
            })
        }))
    }

    pub async fn raw_send_parallel(
        &mut self,
        socket_addrs: Vec<String>,
        msg: Vec<u8>,
    ) -> HashMap<String, anyhow::Error> {
        // Getting targetted addrs that are not in the connected sockets list
        let unknown_socket_addrs = {
            let mut res = socket_addrs.clone();
            res.retain(|addr| !self.sockets.contains_key(addr));
            res
        };

        // Send the message to all targets concurrently and wait for them to finish
        let all_sent = {
            let message = TcpMessage::Data(Arc::new(msg));
            debug!("Broadcasting msg {:?} to all", message);
            let mut tasks = vec![];
            for (name, socket) in self
                .sockets
                .iter_mut()
                .filter(|s| socket_addrs.contains(s.0))
            {
                debug!(" - to {}", name);
                tasks.push(
                    socket
                        .sender
                        .send(message.clone())
                        .map(|res| (name.clone(), res)),
                );
            }
            futures::future::join_all(tasks).await
        };

        // Regroup future results in a map keyed with addrs
        let mut result = HashMap::from_iter(all_sent.into_iter().filter_map(
            |(client_name, send_result)| {
                send_result.err().map(|error| {
                    (
                        client_name.clone(),
                        anyhow::anyhow!("Sending message to client {}: {}", client_name, error),
                    )
                })
            },
        ));

        // Filling the map with errors for unknown targets
        for unknown in unknown_socket_addrs {
            result.insert(
                unknown.clone(),
                anyhow::anyhow!("Unknown socket_addr {}", unknown),
            );
        }

        result
    }
    pub async fn send(&mut self, socket_addr: String, msg: Res) -> anyhow::Result<()> {
        debug!("Sending msg {:?} to {}", msg, socket_addr);
        let stream = self
            .sockets
            .get_mut(&socket_addr)
            .context(format!("Retrieving client {}", socket_addr))?;

        let binary_data = to_tcp_message(&msg)?;
        stream
            .sender
            .send(binary_data)
            .await
            .map_err(|e| anyhow::anyhow!("Sending msg to client {}: {}", socket_addr, e))
    }

    pub fn try_send(&mut self, socket_addr: String, msg: Res) -> anyhow::Result<()> {
        debug!("Try Sending msg {:?} to {}", msg, socket_addr);
        let stream = self
            .sockets
            .get_mut(&socket_addr)
            .context(format!("Retrieving client {}", socket_addr))?;

        let binary_data = to_tcp_message(&msg)?;
        stream
            .sender
            .try_send(binary_data)
            .map_err(|e| anyhow::anyhow!("Try sending msg to client {}: {}", socket_addr, e))
    }

    pub async fn ping(&mut self, socket_addr: String) -> anyhow::Result<()> {
        let stream = self
            .sockets
            .get_mut(&socket_addr)
            .context(format!("Retrieving client {}", socket_addr))?;

        stream
            .sender
            .send(TcpMessage::Ping)
            .await
            .map_err(|e| anyhow::anyhow!("Sending ping to client {}: {}", socket_addr, e))
    }

    /// Setup stream in the managed list for a new client
    fn setup_stream(
        &mut self,
        mut sender: SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>,
        mut receiver: SplitStream<Framed<TcpStream, LengthDelimitedCodec>>,
        socket_addr: &String,
    ) {
        // Start a task to process pings from the peer.
        // We do the processing in the main select! loop to keep things synchronous.
        // This makes it easier to store data in the same struct without mutexing.
        let ping_sender = self.ping_sender.clone();
        let pool_sender = self.pool_sender.clone();
        let cloned_socket_addr = socket_addr.clone();
        let metrics = self.metrics.clone();

        // This task is responsible for reception of ping and message.
        // If an error occurs and is not an InvalidData error, we assume the task is to be aborted.
        // If the stream is closed, we also assume the task is to be aborted.
        let abort_receiver_task = logged_task(async move {
            loop {
                match receiver.next().await {
                    Some(Ok(bytes)) => {
                        if *bytes == *b"PING" {
                            _ = ping_sender.send(cloned_socket_addr.clone()).await;
                        } else {
                            debug!(
                                "Received data from socket {}: {} bytes ({}...)",
                                cloned_socket_addr,
                                bytes.len(),
                                hex::encode(bytes.iter().take(10).cloned().collect::<Vec<_>>())
                            );
                            metrics.message_received();
                            metrics.message_received_bytes(bytes.len() as u64);
                            let _ = pool_sender
                                .send(Box::new(match borsh::from_slice(&bytes) {
                                    Ok(data) => TcpEvent::Message {
                                        dest: cloned_socket_addr.clone(),
                                        data,
                                    },
                                    Err(io) => {
                                        metrics.message_error();
                                        TcpEvent::Error {
                                            dest: cloned_socket_addr.clone(),
                                            error: io.to_string(),
                                        }
                                    }
                                }))
                                .await;
                        }
                    }

                    Some(Err(err)) => {
                        if err.kind() == ErrorKind::InvalidData {
                            error!("Received invalid data in socket {cloned_socket_addr} event loop: {err}",);
                        } else {
                            // If the error is not invalid data, we can assume the socket is closed.
                            warn!(
                                "Closing socket {} after error: {:?}",
                                cloned_socket_addr,
                                err.kind()
                            );
                            metrics.message_error();
                            let _ = pool_sender
                                .send(Box::new(TcpEvent::Error {
                                    dest: cloned_socket_addr.clone(),
                                    error: err.to_string(),
                                }))
                                .await;
                            break;
                        }
                    }
                    None => {
                        // If we reach here, the stream has been closed.
                        warn!("Socket {} closed", cloned_socket_addr);
                        metrics.message_closed();
                        let _ = pool_sender
                            .send(Box::new(TcpEvent::Closed {
                                dest: cloned_socket_addr.clone(),
                            }))
                            .await;
                        break;
                    }
                }
            }
        });

        let (sender_snd, mut sender_recv) = tokio::sync::mpsc::channel::<TcpMessage>(10000);
        let metrics = self.metrics.clone();

        let abort_sender_task = logged_task({
            let cloned_socket_addr = socket_addr.clone();
            async move {
                while let Some(msg) = sender_recv.recv().await {
                    let Ok(msg_bytes) = msg.try_into() else {
                        error!(
                            "Failed to serialize message to send to peer {}",
                            cloned_socket_addr
                        );
                        metrics.message_send_error();
                        break;
                    };
                    let start = std::time::Instant::now();
                    let nb_bytes: usize = (&msg_bytes as &Bytes).len();
                    match tokio::time::timeout(Duration::from_secs(10), sender.send(msg_bytes))
                        .await
                    {
                        Err(e) => {
                            error!(
                                "Timeout sending message to peer {}: {}",
                                cloned_socket_addr, e
                            );
                            metrics.message_send_error();
                            break;
                        }
                        Ok(Err(e)) => {
                            error!("Sending message to peer {}: {}", cloned_socket_addr, e);
                            metrics.message_send_error();
                            break;
                        }
                        Ok(Ok(_)) => {
                            metrics.message_emitted();
                            metrics.message_emitted_bytes(nb_bytes as u64);
                        }
                    }
                    metrics.message_send_time(start.elapsed().as_secs_f64());
                }
            }
        });

        tracing::debug!("Socket {} connected", socket_addr);
        // Store socket in the list.
        self.metrics.peers_snapshot(self.sockets.len() as u64 + 1);
        self.sockets.insert(
            socket_addr.to_string(),
            SocketStream {
                last_ping: TimestampMsClock::now(),
                sender: sender_snd,
                abort_sender_task,
                abort_receiver_task,
            },
        );
    }

    pub fn setup_client(&mut self, addr: String, tcp_client: TcpClient<Req, Res>) {
        let (sender, receiver) = tcp_client.split();
        self.setup_stream(sender, receiver, &addr);
    }

    pub fn drop_peer_stream(&mut self, peer_ip: String) {
        if let Some(peer_stream) = self.sockets.remove(&peer_ip) {
            peer_stream.abort_sender_task.abort();
            peer_stream.abort_receiver_task.abort();
            tracing::debug!("Client {} dropped & disconnected", peer_ip);
            self.metrics.peers_snapshot(self.sockets.len() as u64);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use crate::tcp::{tcp_client::TcpClient, to_tcp_message, TcpEvent, TcpMessage};

    use anyhow::Result;
    use borsh::{BorshDeserialize, BorshSerialize};
    use bytes::Bytes;
    use futures::TryStreamExt;

    use super::TcpServer;

    #[derive(BorshDeserialize, BorshSerialize, Clone, Debug, PartialEq, Eq)]
    pub struct DataAvailabilityRequest(pub usize);

    #[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
    pub enum DataAvailabilityEvent {
        SignedBlock(String),
    }

    type DAServer = TcpServer<DataAvailabilityRequest, DataAvailabilityEvent>;
    type DAClient = TcpClient<DataAvailabilityRequest, DataAvailabilityEvent>;

    #[tokio::test]
    async fn tcp_test() -> Result<()> {
        let mut server = DAServer::start(2345, "DaServer").await?;

        let mut client = DAClient::connect("me".to_string(), "0.0.0.0:2345").await?;

        // Ping
        client.ping().await?;

        // Send data to server
        client.send(DataAvailabilityRequest(2)).await?;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let d = match server.listen_next().await.unwrap() {
            TcpEvent::Message { data, .. } => data,
            _ => panic!("Expected a Message event"),
        };

        assert_eq!(DataAvailabilityRequest(2), d);
        assert!(server.pool_receiver.try_recv().is_err());

        // From server to client
        _ = server
            .broadcast(DataAvailabilityEvent::SignedBlock("blabla".to_string()))
            .await;

        assert_eq!(
            client.recv().await.unwrap(),
            DataAvailabilityEvent::SignedBlock("blabla".to_string())
        );

        let client_socket_addr = server.connected_clients().first().unwrap().clone();

        server.ping(client_socket_addr).await?;

        assert_eq!(
            client.receiver.try_next().await.unwrap().unwrap(),
            TryInto::<Bytes>::try_into(TcpMessage::Ping).unwrap()
        );

        Ok(())
    }

    #[tokio::test]
    async fn tcp_broadcast() -> Result<()> {
        let mut server = DAServer::start(0, "DaServer").await?;

        let mut client1 = DAClient::connect(
            "me1".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;

        let mut client2 = DAClient::connect(
            "me2".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        server
            .broadcast(DataAvailabilityEvent::SignedBlock("test".to_string()))
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let res1 = client1.receiver.try_next().await;
        assert!(res1.is_ok());
        assert_eq!(
            res1.unwrap().unwrap(),
            TryInto::<Bytes>::try_into(
                to_tcp_message(&DataAvailabilityEvent::SignedBlock("test".to_string())).unwrap()
            )
            .unwrap()
        );
        let res2 = client2.receiver.try_next().await;
        assert!(res2.is_ok());
        assert_eq!(
            res2.unwrap().unwrap(),
            TryInto::<Bytes>::try_into(
                to_tcp_message(&DataAvailabilityEvent::SignedBlock("test".to_string())).unwrap()
            )
            .unwrap()
        );

        Ok(())
    }

    #[tokio::test]
    async fn tcp_send_parallel() -> Result<()> {
        let mut server = DAServer::start(0, "DAServer").await?;

        let mut client1 = DAClient::connect(
            "me1".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;

        let client1_addr = server.connected_clients().clone().first().unwrap().clone();

        let mut client2 = DAClient::connect(
            "me2".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;
        let client2_addr = server
            .connected_clients()
            .clone()
            .into_iter()
            .filter(|addr| addr != &client1_addr)
            .next_back()
            .unwrap();

        server
            .raw_send_parallel(
                vec![client2_addr.to_string()],
                borsh::to_vec(&DataAvailabilityEvent::SignedBlock("test".to_string())).unwrap(),
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let res1 =
            tokio::time::timeout(Duration::from_millis(200), client1.receiver.try_next()).await;
        assert!(res1.is_err());

        let res2 = client2.receiver.try_next().await;
        assert!(res2.is_ok());
        assert_eq!(
            res2.unwrap().unwrap(),
            TryInto::<Bytes>::try_into(
                to_tcp_message(&DataAvailabilityEvent::SignedBlock("test".to_string())).unwrap()
            )
            .unwrap()
        );

        Ok(())
    }

    #[tokio::test]
    async fn tcp_send() -> Result<()> {
        let mut server = DAServer::start(0, "DAServer").await?;

        let mut client1 = DAClient::connect(
            "me1".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;
        let client1_addr = server.connected_clients().first().unwrap().clone();

        let mut client2 = DAClient::connect(
            "me2".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;
        let client2_addr = server
            .connected_clients()
            .clone()
            .into_iter()
            .filter(|addr| addr != &client1_addr)
            .next_back()
            .unwrap();

        _ = server
            .send(
                client2_addr.to_string(),
                DataAvailabilityEvent::SignedBlock("test".to_string()),
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let res1 =
            tokio::time::timeout(Duration::from_millis(200), client1.receiver.try_next()).await;
        assert!(res1.is_err());

        let res2 = client2.receiver.try_next().await;
        assert!(res2.is_ok());
        assert_eq!(
            res2.unwrap().unwrap(),
            TryInto::<Bytes>::try_into(
                to_tcp_message(&DataAvailabilityEvent::SignedBlock("test".to_string())).unwrap()
            )
            .unwrap()
        );

        Ok(())
    }

    type BytesServer = TcpServer<Vec<u8>, Vec<u8>>;
    type BytesClient = TcpClient<Vec<u8>, Vec<u8>>;

    #[test_log::test(tokio::test)]
    async fn tcp_with_max_frame_length() -> Result<()> {
        let mut server = BytesServer::start_with_opts(0, Some(100), "Test").await?;

        let mut client = BytesClient::connect_with_opts(
            "me".to_string(),
            Some(100),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;

        // Send data to server
        // A vec will be prefixed with 4 bytes (u32) containing the size of the payload
        // Here we reach 100 bytes <= 100
        client.send(vec![0b_0; 96]).await?;

        let data = match server.listen_next().await.unwrap() {
            TcpEvent::Message { data, .. } => data,
            _ => panic!("Expected a Message event"),
        };

        assert_eq!(data.len(), 96);
        assert!(server.pool_receiver.try_recv().is_err());

        // Send data to server
        // Here we reach 101 bytes, it should explode the limit
        let sent = client.send(vec![0b_0; 97]).await;
        tracing::warn!("Sent: {:?}", sent);
        assert!(sent.is_err_and(|e| e.to_string().contains("frame size too big")));

        let mut client_relaxed = BytesClient::connect(
            "me".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;

        // Should be ok server side
        client_relaxed.send(vec![0b_0; 96]).await?;

        let data = match server.listen_next().await.unwrap() {
            TcpEvent::Message { data, .. } => data,
            _ => panic!("Expected a Message event"),
        };
        assert_eq!(data.len(), 96);

        // Should explode server side
        client_relaxed.send(vec![0b_0; 97]).await?;

        let received_data = server.listen_next().await;
        assert!(received_data.is_some_and(|tcp_event| matches!(tcp_event, TcpEvent::Closed { .. })));

        Ok(())
    }
}
