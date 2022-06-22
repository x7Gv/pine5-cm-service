pub mod database;
pub mod model;
pub mod rpc;

use cm::cm_message_server::CmMessageServer;
use cm::cm_token_server::CmTokenServer;

use tonic::transport::Server;

use rpc::cm;
use rpc::cm_message::CmMessageService;

use crate::rpc::cm_token::CmTokenService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:10000".parse().unwrap();

    println!("Service listening on: {}", addr);

    let message = CmMessageService::default();
    let token = CmTokenService::default();

    let message_svc = CmMessageServer::new(message);
    let token_svc = CmTokenServer::new(token);

    Server::builder()
        .add_service(message_svc)
        .add_service(token_svc)
        .serve(addr)
        .await?;

    Ok(())
}
