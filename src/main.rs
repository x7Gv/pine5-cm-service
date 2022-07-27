pub mod database;
pub mod fcm;
pub mod model;
pub mod rpc;

use cm::cm_message_server::CmMessageServer;
use cm::cm_token_server::CmTokenServer;

use tonic::transport::Server;

use rpc::cm;
use rpc::cm_message::CmMessageService;
use tracing::{info, Instrument, Level};
use std::sync::Arc;

use crate::{rpc::cm_token::CmTokenService, fcm::FCMService};

fn setup_log() {
    if cfg!(debug_assertions) {
        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .init();
    } else {
        tracing_subscriber::fmt().init();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_log();

    let addr = "[::1]:10000".parse().unwrap();

    let message = CmMessageService::new(Arc::new(FCMService::new("abc")), tokio::sync::broadcast::channel(16));
    let token = CmTokenService::default();

    let message_svc = CmMessageServer::new(message);
    let token_svc = CmTokenServer::new(token);

    info!(message = "Starting server.", %addr);

    Server::builder()
        .trace_fn(|_| tracing::info_span!("cm_server"))
        .add_service(message_svc)
        .add_service(token_svc)
        .serve(addr)
        .await?;

    Ok(())
}
