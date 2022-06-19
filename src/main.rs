use cm::cm_message_server::{CmMessage, CmMessageServer};
use cm::cm_token_server::{CmToken, CmTokenServer};
use cm::message_broadcast::Operation;
use cm::message_send_response::SendStatus;
use cm::token_broadcast::{self};
use cm::{
    token_register_response, HealthCheckRequest, HealthCheckResponse, MessageBroadcast,
    MessageSendRequest, MessageSendResponse, MessageSubscribeRequest, TokenBroadcast, TokenKey,
    TokenRegisterRequest, TokenRegisterResponse, TokenSubscribeRequest, TokenUpdateRequest,
    TokenUpdateResponse,
};

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::async_trait;

pub mod model;

pub mod cm {
    tonic::include_proto!("cm");
}

#[async_trait]
pub trait TokenDb: Send + Sync + 'static {
    async fn insert(&self, token: &model::TokenKey) -> Result<model::Token, TokenDbError>;
    async fn update(&self, token: &model::TokenKey) -> Result<model::Token, TokenDbError>;
    async fn invalidate(&self, token: &model::TokenKey) -> Result<model::Token, TokenDbError>;
}

pub enum TokenDbInsertResult {
    Success,
    Invalid,
    AlreadyPresent,
    Unknown,
}

pub struct TokenDbInMemory {
    db: Arc<RwLock<HashMap<model::TokenKey, model::Token>>>,
}

impl TokenDbInMemory {
    pub fn new() -> Self {
        Self {
            db: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

pub enum TokenDbError {
    TokenNotPresent(model::TokenKey),
    Unknown,
}

#[async_trait]
impl TokenDb for TokenDbInMemory {
    async fn insert(&self, token: &model::TokenKey) -> Result<model::Token, TokenDbError> {
        {
            let mut w0 = self.db.write().await;
            let t = model::Token::from(token.clone());
            w0.insert(token.clone(), t.clone());
            return Ok(t)
        }
    }

    async fn update(&self, token: &model::TokenKey) -> Result<model::Token, TokenDbError> {
        {
            let r0 = self.db.read().await;
            if r0.contains_key(&token) {
                let mut w0 = self.db.write().await;
                let t = model::Token::from(token.clone());
                w0.insert(token.clone(), t.clone());
                return Ok(t)
            } else {
            }
        }
    }

    async fn invalidate(&self, token: String) -> Result<(), Box<dyn std::error::Error>> {
        {
            let mut w0 = self.db.write().await;
            w0.remove(&token);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct CmMessageService {
    subscribe_tx: broadcast::Sender<MessageBroadcast>,
}

#[derive(Debug)]
pub struct CmTokenService<Db: TokenDb> {
    subscribe_tx: broadcast::Sender<TokenBroadcast>,
    db: Arc<Db>,
}

fn message_subscribe_filter(request: &MessageSubscribeRequest, key: &TokenKey) -> bool {
    if let Some(filter) = request.filter.as_ref() {
        if let Some(predicate) = &filter.predicate {
            match predicate {
                cm::message_subscribe_filter::Predicate::Complement(complement) => {
                    return !complement.keys.contains(&key)
                }
                cm::message_subscribe_filter::Predicate::Intersection(intersection) => {
                    return intersection.keys.contains(&key);
                }
                cm::message_subscribe_filter::Predicate::Union(_) => {
                    return true;
                }
            }
        }
    }

    false
}

#[async_trait]
impl CmMessage for CmMessageService {
    async fn message_send(
        &self,
        request: Request<MessageSendRequest>,
    ) -> Result<Response<MessageSendResponse>, Status> {
        let message = request.into_inner().inner.unwrap();

        let bcast = MessageBroadcast {
            operation: Some(Operation::Send(message)),
        };

        self.subscribe_tx.send(bcast).unwrap();

        Ok(Response::new(MessageSendResponse {
            status: SendStatus::Success as i32,
        }))
    }

    type MessageSubscribeStream = ReceiverStream<Result<MessageBroadcast, Status>>;

    async fn message_subscribe(
        &self,
        request: Request<MessageSubscribeRequest>,
    ) -> Result<Response<Self::MessageSubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let mut subscribe_rx = self.subscribe_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(update) = subscribe_rx.recv().await {
                if let Some(operation) = &update.operation {
                    match operation {
                        Operation::Send(message) => {
                            if let Some(codomain) = &message.codomain {
                                let mut pass = true;

                                for token in codomain.keys.iter() {
                                    if !message_subscribe_filter(&request.get_ref(), &token) {
                                        pass = false;
                                    }
                                }

                                if pass {
                                    tx.send(Ok(update)).await.unwrap()
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        todo!()
    }

    type WatchStream = ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        todo!()
    }
}

#[async_trait]
impl<Db: TokenDb> CmToken for CmTokenService<Db> {
    async fn token_register(
        &self,
        request: Request<TokenRegisterRequest>,
    ) -> Result<Response<TokenRegisterResponse>, Status> {
        let token = request.into_inner().token.unwrap();

        let bcast = TokenBroadcast {
            operation: Some(token_broadcast::Operation::Addition(token)),
        };

        self.subscribe_tx.send(bcast).unwrap();
        Ok(Response::new(TokenRegisterResponse {
            status: token_register_response::Status::Success as i32,
        }))
    }

    async fn token_update(
        &self,
        request: Request<TokenUpdateRequest>,
    ) -> Result<Response<TokenUpdateResponse>, Status> {
        todo!()
    }

    type TokenSubscribeStream = ReceiverStream<Result<TokenBroadcast, Status>>;

    async fn token_subscribe(
        &self,
        request: Request<TokenSubscribeRequest>,
    ) -> Result<Response<Self::TokenSubscribeStream>, Status> {
        todo!()
    }

    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        todo!()
    }

    type WatchStream = ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:10000".parse().unwrap();

    println!("Service listening on: {}", addr);

    let broadcast = broadcast::channel(16);

    let message = CmMessageService {
        subscribe_tx: broadcast.0,
    };

    let message_svc = CmMessageServer::new(message);

    Server::builder()
        .add_service(message_svc)
        .serve(addr)
        .await?;

    Ok(())
}
