use opentelemetry::{
    metrics::{Counter, Gauge, Histogram},
    InstrumentationScope, KeyValue,
};

use crate::tcp::Canal;

macro_rules! build {
    ($meter:ident, $type:ty, $name:expr) => {
        paste::paste! { $meter.[<u64_ $type>](stringify!([<"p2p_server_" $name>])).build() }
    };
}

pub(crate) struct P2PMetrics {
    ping: Counter<u64>,
    peers: Gauge<u64>,
    message: Counter<u64>,
    message_error: Counter<u64>,
    message_closed: Counter<u64>,
    handshake_connection: Counter<u64>,
    handshake_hello: Counter<u64>,
    handshake_verack: Counter<u64>,
}

impl P2PMetrics {
    pub fn global(node_name: String) -> P2PMetrics {
        let scope = InstrumentationScope::builder(node_name).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);

        P2PMetrics {
            ping: build!(my_meter, counter, "ping"),
            peers: build!(my_meter, gauge, "peers"),
            message: build!(my_meter, counter, "message"),
            message_error: build!(my_meter, counter, "message_error"),
            message_closed: build!(my_meter, counter, "message_closed"),
            handshake_connection: build!(my_meter, counter, "handshake_connection"),
            handshake_hello: build!(my_meter, counter, "handshake_hello"),
            handshake_verack: build!(my_meter, counter, "handshake_verack"),
        }
    }

    pub fn peers_snapshot(&mut self, nb: u64) {
        self.peers.record(nb, &[]);
    }

    pub fn message_received(&self, from: String, canal: Canal) {
        self.message.add(
            1,
            &[
                KeyValue::new("from", from),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }

    pub fn message_error(&self, from: String, canal: Canal) {
        self.message_error.add(
            1,
            &[
                KeyValue::new("from", from),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }

    pub fn message_closed(&self, from: String, canal: Canal) {
        self.message_closed.add(
            1,
            &[
                KeyValue::new("from", from),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }

    pub fn ping(&self, peer: String, canal: Canal) {
        self.ping.add(
            1,
            &[
                KeyValue::new("peer", peer),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }

    pub fn message_emitted(&self, to: String, canal: Canal) {
        self.message.add(
            1,
            &[
                KeyValue::new("to", to),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }

    pub fn handshake_connection_emitted(&self, to: String, canal: Canal) {
        self.handshake_connection.add(
            1,
            &[
                KeyValue::new("to", to),
                KeyValue::new("canal", canal.to_string()),
            ],
        )
    }

    pub fn handshake_hello_emitted(&self, to: String, canal: Canal) {
        self.handshake_hello.add(
            1,
            &[
                KeyValue::new("to", to),
                KeyValue::new("canal", canal.to_string()),
            ],
        )
    }

    pub fn handshake_hello_received(&self, from: String, canal: Canal) {
        self.handshake_hello.add(
            1,
            &[
                KeyValue::new("from", from),
                KeyValue::new("canal", canal.to_string()),
            ],
        )
    }

    pub fn handshake_verack_emitted(&self, to: String, canal: Canal) {
        self.handshake_verack.add(
            1,
            &[
                KeyValue::new("to", to),
                KeyValue::new("canal", canal.to_string()),
            ],
        )
    }

    pub fn handshake_verack_received(&self, from: String, canal: Canal) {
        self.handshake_verack.add(
            1,
            &[
                KeyValue::new("from", from),
                KeyValue::new("canal", canal.to_string()),
            ],
        )
    }
}

#[derive(Clone)]
pub struct TcpServerMetrics {
    peers: Gauge<u64>,
    message_received: Counter<u64>,
    message_received_bytes: Counter<u64>,
    message_emitted: Counter<u64>,
    message_emitted_bytes: Counter<u64>,
    message_error: Counter<u64>,
    message_closed: Counter<u64>,
    message_send_error: Counter<u64>,
    message_send_time: Histogram<f64>,
    server_name_label: Vec<KeyValue>,
}

impl TcpServerMetrics {
    pub fn global(pool_name: String) -> TcpServerMetrics {
        let scope = InstrumentationScope::builder(pool_name.clone()).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);
        TcpServerMetrics {
            peers: my_meter.u64_gauge("tcp_server_peers").build(),
            message_received: my_meter.u64_counter("tcp_server_message_received").build(),
            message_received_bytes: my_meter
                .u64_counter("tcp_server_message_received_bytes")
                .build(),
            message_emitted: my_meter.u64_counter("tcp_server_message_emitted").build(),
            message_emitted_bytes: my_meter
                .u64_counter("tcp_server_message_emitted_bytes")
                .build(),
            message_error: my_meter.u64_counter("tcp_server_message_error").build(),
            message_closed: my_meter.u64_counter("tcp_server_message_closed").build(),
            message_send_error: my_meter
                .u64_counter("tcp_server_message_send_error")
                .build(),
            message_send_time: my_meter
                .f64_histogram("tcp_server_message_send_time_seconds")
                .build(),
            server_name_label: vec![KeyValue::new("server_name", pool_name)],
        }
    }

    pub fn peers_snapshot(&self, nb: u64) {
        self.peers.record(nb, &self.server_name_label);
    }

    pub fn message_received(&self) {
        self.message_received.add(1, &self.server_name_label);
    }

    pub fn message_emitted(&self) {
        self.message_emitted.add(1, &self.server_name_label);
    }

    pub fn message_emitted_bytes(&self, len: u64) {
        self.message_emitted_bytes.add(len, &self.server_name_label);
    }

    pub fn message_error(&self) {
        self.message_error.add(1, &self.server_name_label);
    }

    pub fn message_closed(&self) {
        self.message_closed.add(1, &self.server_name_label);
    }

    pub fn message_send_error(&self) {
        self.message_send_error.add(1, &self.server_name_label);
    }

    pub fn message_send_time(&self, duration: f64) {
        self.message_send_time
            .record(duration, &self.server_name_label);
    }

    pub(crate) fn message_received_bytes(&self, len: u64) {
        self.message_received_bytes
            .add(len, &self.server_name_label);
    }
}
