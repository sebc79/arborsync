use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Instant;

use arborsync_core::LoadedMaster;
use arborsync_core::config::SlaveAcl;
use arborsync_core::keys::{format_hex_key, public_from_secret, write_static_key};
use arborsync_core::master::{Master, MemoryContent, Reply};
use arborsync_core::protocol::ProtocolMessage;
use arborsync_core::slave::Slave;
use arborsync_core::test_support::{MemoryStorage, SyncSandbox};
use arborsync_core::transport::{
    AttemptLimiter, accept_control, client_endpoint, connect, listen, open_control,
    peer_static_key, read_control, write_control,
};

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

    assert_eq!(peer_static_key(&server_conn).unwrap(), slave_pub);
    assert_eq!(peer_static_key(&client_conn).unwrap(), master_pub);
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
        let peer = peer_static_key(&conn).unwrap();
        let (send, mut recv) = accept_control(&conn).await.unwrap();
        let msg = read_control(&mut recv).await.unwrap();
        (peer, msg, send, recv, conn)
    });

    let conn = connect(&client, addr).await.unwrap();
    slave.pin_check(peer_static_key(&conn).unwrap()).unwrap();
    let (mut send, mut recv) = open_control(&conn).await.unwrap();
    write_control(&mut send, &slave.subscribe()).await.unwrap();

    let (peer, incoming, mut server_send, _server_recv, _server_conn) = server_task.await.unwrap();
    match master.handle(peer, incoming).unwrap() {
        Reply::Send(ProtocolMessage::SubscribeAck { checkouts }) => {
            assert_eq!(checkouts.len(), 1);
            assert_eq!(checkouts[0].id, "src");
            write_control(
                &mut server_send,
                &ProtocolMessage::SubscribeAck { checkouts },
            )
            .await
            .unwrap();
        }
        other => panic!("expected SubscribeAck, got {other:?}"),
    }

    match read_control(&mut recv).await.unwrap() {
        ProtocolMessage::SubscribeAck { checkouts } => assert_eq!(checkouts[0].id, "src"),
        other => panic!("expected SubscribeAck, got {other:?}"),
    }
}
