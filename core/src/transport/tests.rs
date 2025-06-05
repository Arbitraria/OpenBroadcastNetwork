//! Tests for the transport layer implementations

#[cfg(test)]
mod tests {
    use super::super::interface::{Transport, TransportEvent, TransportError, Connection, ConnectionId};
    use super::super::webrtc::{WebRtcTransport, WebRtcConfig, IceTransportPolicy};
    use super::super::quic::{QuicTransport, QuicConfig};
    use std::net::SocketAddr;
    use std::str::FromStr;

    /// A mock transport for testing
    struct MockTransport {
        running: bool,
    }

    impl MockTransport {
        fn new() -> Self {
            Self { running: false }
        }
    }

    impl Transport for MockTransport {
        fn start(&mut self) -> Result<(), TransportError> {
            self.running = true;
            Ok(())
        }
        
        fn stop(&mut self) -> Result<(), TransportError> {
            self.running = false;
            Ok(())
        }
        
        fn connect(&mut self, addr: SocketAddr) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Connection, TransportError>> + Send>> {
            // Return a successful connection
            Box::pin(async move {
                let conn_id = ConnectionId(vec![1, 2, 3, 4]);
                Ok(Connection {
                    id: conn_id,
                    remote_addr: Some(addr),
                    remote_peer_id: None,
                })
            })
        }
        
        fn send(&mut self, _conn_id: &ConnectionId, _data: Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send>> {
            // Simulate successful send
            Box::pin(async { Ok(()) })
        }
        
        fn close_connection(&mut self, _conn_id: &ConnectionId) -> Result<(), TransportError> {
            Ok(())
        }
        
        fn is_running(&self) -> bool {
            self.running
        }
        
        fn next_event(&mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<TransportEvent>> + Send>> {
            // No events for test
            Box::pin(async { None })
        }
    }

    #[tokio::test]
    async fn test_mock_transport_lifecycle() {
        let mut transport = MockTransport::new();
        
        // Test initial state
        assert!(!transport.is_running());
        
        // Test start
        transport.start().expect("Failed to start transport");
        assert!(transport.is_running());
        
        // Test stop
        transport.stop().expect("Failed to stop transport");
        assert!(!transport.is_running());
    }
    
    #[tokio::test]
    async fn test_mock_transport_connect() {
        let mut transport = MockTransport::new();
        transport.start().expect("Failed to start transport");
        
        // Test connect
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();
        let conn_result = transport.connect(addr).await;
        
        assert!(conn_result.is_ok());
        let conn = conn_result.unwrap();
        assert_eq!(conn.remote_addr, Some(addr));
        
        // Test send
        let data = vec![1, 2, 3, 4];
        let send_result = transport.send(&conn.id, data).await;
        assert!(send_result.is_ok());
        
        // Test close
        let close_result = transport.close_connection(&conn.id);
        assert!(close_result.is_ok());
    }

    // Placeholder for more detailed transport tests once actual implementations are complete
    use crate::test_report::{TestReport, TestResult};
    use std::time::{Instant, SystemTime};

    #[test]
    fn test_webrtc_config() {
        let test_start = Instant::now();
        let mut report = TestReport::new();
        let mut test_status = "pass".to_string();
        let mut error_msg = None;
        let test_result = std::panic::catch_unwind(|| {
            let config = WebRtcConfig {
                stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
                turn_servers: vec![],
                ice_transport_policy: IceTransportPolicy::All,
            };
            assert_eq!(config.stun_servers.len(), 1);
            assert_eq!(config.stun_servers[0], "stun:stun.l.google.com:19302");
            assert_eq!(config.turn_servers.len(), 0);
            assert_eq!(config.ice_transport_policy, IceTransportPolicy::All);
        });
        if let Err(e) = test_result {
            test_status = "fail".to_string();
            error_msg = Some(format!("{e:?}"));
        }
        let duration_ms = test_start.elapsed().as_millis();
        let result = TestResult {
            name: "test_webrtc_config".to_string(),
            module: "transport".to_string(),
            status: test_status,
            error: error_msg,
            duration_ms,
            timestamp: SystemTime::now(),
        };
        report.add_result(result);
        let _ = report.save_to_file("test_report.json");
    }

    #[test]
    fn test_quic_config() {
        let test_start = Instant::now();
        let mut report = TestReport::new();
        let mut test_status = "pass".to_string();
        let mut error_msg = None;
        let test_result = std::panic::catch_unwind(|| {
            let addr = SocketAddr::from_str("0.0.0.0:0").unwrap();
            let config = QuicConfig {
                bind_addr: addr,
                cert_path: None,
                key_path: None,
                max_concurrent_streams: 100,
                enable_hole_punching: true,
            };
            assert_eq!(config.bind_addr, addr);
            assert_eq!(config.cert_path, None);
            assert_eq!(config.key_path, None);
            assert_eq!(config.max_concurrent_streams, 100);
            assert!(config.enable_hole_punching);
        });
        if let Err(e) = test_result {
            test_status = "fail".to_string();
            error_msg = Some(format!("{e:?}"));
        }
        let duration_ms = test_start.elapsed().as_millis();
        let result = TestResult {
            name: "test_quic_config".to_string(),
            module: "transport".to_string(),
            status: test_status,
            error: error_msg,
            duration_ms,
            timestamp: SystemTime::now(),
        };
        report.add_result(result);
        let _ = report.save_to_file("test_report.json");
    }
} 