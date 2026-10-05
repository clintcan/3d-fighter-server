//! M2 acceptance tests: UDP bind, hole-punch coordination and relay.

mod common;

use std::net::{SocketAddr, SocketAddrV4};
use std::time::Duration;

use common::{pair_up, start_test};
use fighter_protocol::udp::{BindRole, UdpDatagram};
use tokio::net::UdpSocket;

async fn bind_socket(server: SocketAddr, token: [u8; 16], role: BindRole) -> UdpSocket {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let observed = send_bind(&socket, server, token, role, Vec::new()).await;
    assert_eq!(observed, socket.local_addr().unwrap());
    socket
}

async fn send_bind(
    socket: &UdpSocket,
    server: SocketAddr,
    token: [u8; 16],
    role: BindRole,
    candidates: Vec<SocketAddrV4>,
) -> SocketAddr {
    let dg = UdpDatagram::Bind {
        session_token: token,
        role,
        candidates,
    }
    .encode();
    socket.send_to(&dg, server).await.unwrap();
    let mut buf = [0u8; 64];
    let (n, _) = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buf))
        .await
        .expect("BOUND timeout")
        .unwrap();
    match UdpDatagram::decode(&buf[..n]).unwrap() {
        UdpDatagram::Bound { observed } => SocketAddr::V4(observed),
        other => panic!("expected BOUND, got {other:?}"),
    }
}

fn relay_datagram(key: [u8; 8], payload: Vec<u8>) -> Vec<u8> {
    UdpDatagram::Relay {
        relay_key: key,
        payload,
    }
    .encode()
}

fn payload_for(seq: u32) -> Vec<u8> {
    let mut p = vec![0u8; 64];
    p[0..4].copy_from_slice(&seq.to_le_bytes());
    for (i, b) in p.iter_mut().enumerate().skip(4) {
        *b = (seq as u8).wrapping_add(i as u8);
    }
    p
}

#[tokio::test]
async fn relay_forwards_10000_datagrams_intact() {
    let (running, clock) = start_test().await;
    let pair = pair_up(&running.ws_url(), false, false).await;

    let host_sock = bind_socket(running.udp_addr, pair.host_token, BindRole::Host).await;
    let guest_sock = bind_socket(running.udp_addr, pair.guest_token, BindRole::Guest).await;

    // Both players are told each other's endpoints.
    let mut host = pair.host;
    let mut guest = pair.guest;
    let host_peers = host.recv_type("peer_endpoints").await;
    guest.recv_type("peer_endpoints").await;
    let candidates = host_peers["candidates"].as_array().unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0]["kind"], "public");
    assert_eq!(candidates[0]["ip"], "127.0.0.1");

    let mut buf = [0u8; 1400];
    for seq in 0..10_000u32 {
        // Advance 10 ms per datagram: 100/s, under the 200/s relay budget, and
        // enough virtual time for the token bucket to refill.
        clock.advance(10);
        let payload = payload_for(seq);
        guest_sock
            .send_to(
                &relay_datagram(pair.guest_key, payload.clone()),
                running.udp_addr,
            )
            .await
            .unwrap();
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), host_sock.recv_from(&mut buf))
            .await
            .expect("relayed datagram timeout")
            .unwrap();
        match UdpDatagram::decode(&buf[..n]).unwrap() {
            UdpDatagram::Relayed { payload: got } => assert_eq!(got, payload, "seq {seq}"),
            other => panic!("expected RELAYED, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn relay_only_reveals_no_candidates() {
    let (running, _clock) = start_test().await;
    let pair = pair_up(&running.ws_url(), true, false).await;

    let _host_sock = bind_socket(running.udp_addr, pair.host_token, BindRole::Host).await;
    let _guest_sock = bind_socket(running.udp_addr, pair.guest_token, BindRole::Guest).await;

    let mut host = pair.host;
    let mut guest = pair.guest;
    let host_peers = host.recv_type("peer_endpoints").await;
    let guest_peers = guest.recv_type("peer_endpoints").await;
    assert!(host_peers["candidates"].as_array().unwrap().is_empty());
    assert!(guest_peers["candidates"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn unbound_and_wrong_key_relay_is_dropped() {
    let (running, _clock) = start_test().await;
    let pair = pair_up(&running.ws_url(), false, false).await;

    let host_sock = bind_socket(running.udp_addr, pair.host_token, BindRole::Host).await;
    // Guest never binds; relay from it must be dropped (source unknown).
    let guest_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    guest_sock
        .send_to(
            &relay_datagram(pair.guest_key, vec![1, 2, 3, 4]),
            running.udp_addr,
        )
        .await
        .unwrap();
    // A completely unknown relay key is dropped too.
    guest_sock
        .send_to(
            &relay_datagram([9u8; 8], vec![1, 2, 3, 4]),
            running.udp_addr,
        )
        .await
        .unwrap();

    let mut buf = [0u8; 64];
    let result =
        tokio::time::timeout(Duration::from_millis(300), host_sock.recv_from(&mut buf)).await;
    assert!(result.is_err(), "unbound relay should not be forwarded");
}

#[tokio::test]
async fn rebind_moves_the_endpoint() {
    let (running, _clock) = start_test().await;
    let pair = pair_up(&running.ws_url(), false, false).await;

    let socket_a = bind_socket(running.udp_addr, pair.host_token, BindRole::Host).await;
    let guest_sock = bind_socket(running.udp_addr, pair.guest_token, BindRole::Guest).await;

    let mut host = pair.host;
    host.recv_type("peer_endpoints").await;

    // Re-BIND the host from a new source port.
    let socket_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let observed = send_bind(
        &socket_b,
        running.udp_addr,
        pair.host_token,
        BindRole::Host,
        Vec::new(),
    )
    .await;
    assert_eq!(observed, socket_b.local_addr().unwrap());
    assert_ne!(
        socket_a.local_addr().unwrap(),
        socket_b.local_addr().unwrap()
    );

    // The peer is told again after the endpoint moves.
    host.recv_type("peer_endpoints").await;

    // A relay now lands on the new port, not the old one.
    guest_sock
        .send_to(
            &relay_datagram(pair.guest_key, vec![7, 7, 7]),
            running.udp_addr,
        )
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let (n, _) = tokio::time::timeout(Duration::from_secs(2), socket_b.recv_from(&mut buf))
        .await
        .expect("relay to new endpoint")
        .unwrap();
    assert!(matches!(
        UdpDatagram::decode(&buf[..n]).unwrap(),
        UdpDatagram::Relayed { .. }
    ));
    let stale =
        tokio::time::timeout(Duration::from_millis(200), socket_a.recv_from(&mut buf)).await;
    assert!(stale.is_err(), "old endpoint should receive nothing");
}

#[tokio::test]
async fn udp_ping_returns_pong() {
    let (running, _clock) = start_test().await;
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let ping = UdpDatagram::Ping {
        nonce: 0x0102_0304,
        client_time_ms: 1000,
    }
    .encode();
    sock.send_to(&ping, running.udp_addr).await.unwrap();
    let mut buf = [0u8; 64];
    let (n, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .expect("PONG timeout")
        .unwrap();
    // PONG mirrors PING exactly except for the type byte, so sizes are equal.
    assert_eq!(n, ping.len());
    assert!(matches!(
        UdpDatagram::decode(&buf[..n]).unwrap(),
        UdpDatagram::Pong {
            nonce: 0x0102_0304,
            client_time_ms: 1000
        }
    ));
}

#[tokio::test]
async fn bad_bind_token_is_dropped() {
    let (running, _clock) = start_test().await;
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dg = UdpDatagram::Bind {
        session_token: [0u8; 16],
        role: BindRole::Host,
        candidates: Vec::new(),
    }
    .encode();
    sock.send_to(&dg, running.udp_addr).await.unwrap();
    let mut buf = [0u8; 64];
    let result = tokio::time::timeout(Duration::from_millis(300), sock.recv_from(&mut buf)).await;
    assert!(result.is_err(), "invalid BIND must get no BOUND");
}
