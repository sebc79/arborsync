use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Instant;

use arborsync_core::LoadedMaster;
use arborsync_core::config::SlaveAcl;
use arborsync_core::keys::{format_hex_key, public_from_secret, write_static_key};
use arborsync_core::master::{Master, MemoryContent, Reply};
use arborsync_core::meta::hash_bytes;
use arborsync_core::protocol::{BulkEncoding, BulkHeader, ProtocolMessage};
use arborsync_core::slave::Slave;
use arborsync_core::test_support::{MemoryStorage, SyncSandbox, p};
use arborsync_core::transfer::BulkTransfer;
use arborsync_core::transport::{
    AttemptLimiter, MemoryTransport, Transport, TransportError, client_endpoint, connect, listen,
};
use quinn::Connection;

fn write_key(dir: &std::path::Path, name: &str) -> [u8; 32] {
    write_static_key(dir.join(name)).unwrap()
}

#[test]
fn attempt_limiter_caps_one_ip_inside_the_window() {
    let mut limiter = AttemptLimiter::new(2);
    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let t0 = Instant::now();
    assert!(limiter.allow(ip, t0));
    assert!(limiter.allow(ip, t0));
    assert!(limiter.limited(ip, t0));
    assert!(!limiter.allow(ip, t0));
    assert!(limiter.allow(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), t0));
}

#[test]
fn attempt_limiter_set_max_applies_to_the_next_allow() {
    let mut limiter = AttemptLimiter::new(2);
    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let t0 = Instant::now();
    assert!(limiter.allow(ip, t0));
    limiter.set_max(1);
    assert!(!limiter.allow(ip, t0));
}

#[tokio::test]
async fn xx_accept_exposes_the_peer_static_key() {
    let dir = tempfile::tempdir().unwrap();
    let master_pub = write_key(dir.path(), "master.key");
    let slave_pub = write_key(dir.path(), "slave.key");
    let master_secret = arborsync_core::read_static_key(dir.path().join("master.key")).unwrap();
    let slave_secret = arborsync_core::read_static_key(dir.path().join("slave.key")).unwrap();
    assert_eq!(public_from_secret(&master_secret), master_pub);
    assert_eq!(public_from_secret(&slave_secret), slave_pub);

    let server = listen(SocketAddr::from(([127, 0, 0, 1], 0)), &master_secret).unwrap();
    let addr = server.local_addr().unwrap();
    let client = client_endpoint(&slave_secret).unwrap();

    let incoming = tokio::spawn(async move {
        let connecting = server.accept().await.expect("accept");
        connecting.await.expect("handshake")
    });
    let client_conn = connect(&client, addr).await.unwrap();
    let server_conn = incoming.await.unwrap();

    assert_eq!(server_conn.peer_static_key().unwrap(), slave_pub);
    assert_eq!(client_conn.peer_static_key().unwrap(), master_pub);
}

#[tokio::test]
async fn subscribe_over_xx_acks_a_pinned_slave() {
    let sandbox = SyncSandbox::new();
    let dir = sandbox.path();
    let master_pub = write_key(dir, "master.key");
    let slave_pub = write_key(dir, "slave.key");
    let master_secret = arborsync_core::read_static_key(dir.join("master.key")).unwrap();
    let slave_secret = arborsync_core::read_static_key(dir.join("slave.key")).unwrap();

    sandbox.write_master_config(vec![SlaveAcl {
        id: "dev-alice".into(),
        public_keys: vec![format_hex_key(&slave_pub)],
        allowed_prefixes: vec!["/src".into()],
    }]);
    let master_cfg = LoadedMaster::load(sandbox.master_config_path()).unwrap();
    let mut master = Master::open(master_cfg, MemoryStorage::new(), MemoryContent::new()).unwrap();

    let local = sandbox.add_checkout("dev-alice", "src");
    let slave_cfg = sandbox.write_slave_config(
        "dev-alice",
        vec![arborsync_core::config::CheckoutConfig {
            id: "src".into(),
            central: "/src".into(),
            local: local.to_string_lossy().into_owned(),
        }],
        vec![format_hex_key(&master_pub)],
    );
    let slave = Slave::open(
        arborsync_core::LoadedSlave::load(&slave_cfg).unwrap(),
        MemoryStorage::new(),
        MemoryContent::new(),
    )
    .unwrap();

    let server = listen(SocketAddr::from(([127, 0, 0, 1], 0)), &master_secret).unwrap();
    let addr = server.local_addr().unwrap();
    let client = client_endpoint(&slave_secret).unwrap();

    let server_task = tokio::spawn(async move {
        let conn = server.accept().await.expect("accept").await.expect("hs");
        let peer = conn.peer_static_key().unwrap();
        let (send, mut recv) = conn.accept_control().await.unwrap();
        let msg = Connection::read_control(&mut recv).await.unwrap();
        (peer, msg, send, recv, conn)
    });

    let conn = connect(&client, addr).await.unwrap();
    slave.pin_check(conn.peer_static_key().unwrap()).unwrap();
    let (mut send, mut recv) = conn.open_control().await.unwrap();
    Connection::write_control(&mut send, &slave.subscribe())
        .await
        .unwrap();

    let (peer, incoming, mut server_send, _server_recv, _server_conn) = server_task.await.unwrap();
    match master.handle(peer, incoming).unwrap() {
        Reply::Send(ProtocolMessage::SubscribeAck { checkouts }) => {
            assert_eq!(checkouts.len(), 1);
            assert_eq!(checkouts[0].id, "src");
            Connection::write_control(
                &mut server_send,
                &ProtocolMessage::SubscribeAck { checkouts },
            )
            .await
            .unwrap();
        }
        other => panic!("expected SubscribeAck, got {other:?}"),
    }

    match Connection::read_control(&mut recv).await.unwrap() {
        ProtocolMessage::SubscribeAck { checkouts } => assert_eq!(checkouts[0].id, "src"),
        other => panic!("expected SubscribeAck, got {other:?}"),
    }
}

#[test]
fn memory_pair_exposes_the_peer_static_keys() {
    let a = [0x11u8; 32];
    let b = [0x22u8; 32];
    let (left, right) = MemoryTransport::pair(a, b);
    assert_eq!(left.peer_static_key().unwrap(), b);
    assert_eq!(right.peer_static_key().unwrap(), a);
}

#[tokio::test]
async fn memory_control_roundtrip() {
    let (left, right) = MemoryTransport::pair([0x11u8; 32], [0x22u8; 32]);
    let (mut left_send, mut left_recv) = left.open_control().await.unwrap();
    let (mut right_send, mut right_recv) = right.accept_control().await.unwrap();

    MemoryTransport::write_control(
        &mut left_send,
        &ProtocolMessage::Disconnect {
            reason: "bye".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        MemoryTransport::read_control(&mut right_recv)
            .await
            .unwrap(),
        ProtocolMessage::Disconnect {
            reason: "bye".into()
        }
    );

    MemoryTransport::write_control(
        &mut right_send,
        &ProtocolMessage::Disconnect {
            reason: "ack".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        MemoryTransport::read_control(&mut left_recv).await.unwrap(),
        ProtocolMessage::Disconnect {
            reason: "ack".into()
        }
    );
}

#[tokio::test]
async fn memory_bulk_roundtrip() {
    let (left, right) = MemoryTransport::pair([0x11u8; 32], [0x22u8; 32]);
    let body = b"hello".to_vec();
    let want_hash = hash_bytes(&body);
    let xfer = BulkTransfer {
        header: BulkHeader {
            path: p("/src/foo.rs"),
            checkout_id: "src".into(),
            want_hash,
            encoding: BulkEncoding::Whole,
            size: body.len() as u64,
        },
        body,
    };

    left.write_bulk(&xfer).await.unwrap();
    let (header, body) = right.accept_bulk().await.unwrap();
    assert_eq!(
        header,
        BulkHeader {
            path: p("/src/foo.rs"),
            checkout_id: "src".into(),
            want_hash,
            encoding: BulkEncoding::Whole,
            size: 5,
        }
    );
    assert_eq!(body, b"hello");
}

#[tokio::test]
async fn memory_close_stops_reads() {
    let (left, right) = MemoryTransport::pair([0x11u8; 32], [0x22u8; 32]);
    let (_left_send, _left_recv) = left.open_control().await.unwrap();
    let (_right_send, mut right_recv) = right.accept_control().await.unwrap();
    left.close();
    assert!(matches!(
        MemoryTransport::read_control(&mut right_recv).await,
        Err(TransportError::Closed)
    ));
}

#[tokio::test]
async fn subscribe_over_memory_acks_a_pinned_slave() {
    let sandbox = SyncSandbox::new();
    let dir = sandbox.path();
    let master_pub = write_key(dir, "master.key");
    let slave_pub = write_key(dir, "slave.key");

    sandbox.write_master_config(vec![SlaveAcl {
        id: "dev-alice".into(),
        public_keys: vec![format_hex_key(&slave_pub)],
        allowed_prefixes: vec!["/src".into()],
    }]);
    let master_cfg = LoadedMaster::load(sandbox.master_config_path()).unwrap();
    let mut master = Master::open(master_cfg, MemoryStorage::new(), MemoryContent::new()).unwrap();

    let local = sandbox.add_checkout("dev-alice", "src");
    let slave_cfg = sandbox.write_slave_config(
        "dev-alice",
        vec![arborsync_core::config::CheckoutConfig {
            id: "src".into(),
            central: "/src".into(),
            local: local.to_string_lossy().into_owned(),
        }],
        vec![format_hex_key(&master_pub)],
    );
    let slave = Slave::open(
        arborsync_core::LoadedSlave::load(&slave_cfg).unwrap(),
        MemoryStorage::new(),
        MemoryContent::new(),
    )
    .unwrap();

    let (slave_t, master_t) = MemoryTransport::pair(slave_pub, master_pub);
    slave.pin_check(slave_t.peer_static_key().unwrap()).unwrap();
    let (mut send, mut recv) = slave_t.open_control().await.unwrap();
    MemoryTransport::write_control(&mut send, &slave.subscribe())
        .await
        .unwrap();

    let (mut server_send, mut server_recv) = master_t.accept_control().await.unwrap();
    let peer = master_t.peer_static_key().unwrap();
    let incoming = MemoryTransport::read_control(&mut server_recv)
        .await
        .unwrap();
    match master.handle(peer, incoming).unwrap() {
        Reply::Send(ProtocolMessage::SubscribeAck { checkouts }) => {
            assert_eq!(checkouts.len(), 1);
            assert_eq!(checkouts[0].id, "src");
            MemoryTransport::write_control(
                &mut server_send,
                &ProtocolMessage::SubscribeAck { checkouts },
            )
            .await
            .unwrap();
        }
        other => panic!("expected SubscribeAck, got {other:?}"),
    }

    match MemoryTransport::read_control(&mut recv).await.unwrap() {
        ProtocolMessage::SubscribeAck { checkouts } => assert_eq!(checkouts[0].id, "src"),
        other => panic!("expected SubscribeAck, got {other:?}"),
    }
}
