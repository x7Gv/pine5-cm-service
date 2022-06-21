use cm::cm_message_server::{CmMessage, CmMessageServer};
use cm::cm_token_server::{CmToken, CmTokenServer};
use cm::message_broadcast::Operation;
use cm::message_send_response::{SendStatus, self};
use cm::token_broadcast::{self};
use cm::{
    token_register_response, HealthCheckRequest, HealthCheckResponse, MessageBroadcast,
    MessageSendRequest, MessageSendResponse, MessageSubscribeRequest, TokenBroadcast, TokenKey,
    TokenRegisterRequest, TokenRegisterResponse, TokenSubscribeRequest, TokenUpdate,
    TokenUpdateRequest, TokenUpdateResponse,
};

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::async_trait;

pub mod model;

pub mod cm {
    tonic::include_proto!("cm");
}

#[async_trait]
pub trait TokenDb: Send + Sync + 'static {
    async fn insert(&self, token: model::TokenKey) -> Result<model::Token, TokenDbError>;
    async fn update(&self, token: model::TokenKey) -> Result<model::TokenUpdate, TokenDbError>;
    async fn invalidate(&self, token: model::TokenKey) -> Result<(), TokenDbError>;
}

pub struct TokenDbInMemory {
    db: Arc<Mutex<HashMap<model::TokenKey, model::Token>>>,
}

impl TokenDbInMemory {
    pub fn new() -> Self {
        Self {
            db: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for TokenDbInMemory {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub enum TokenDbError {
    TokenNotPresent(model::TokenKey),
    Unknown,
}

#[async_trait]
impl TokenDb for TokenDbInMemory {
    async fn insert(&self, token: model::TokenKey) -> Result<model::Token, TokenDbError> {
        let t = model::Token::from(token.clone());

        {
            let mut locked = self.db.lock().await;

            println!("{:?}", *locked);

            locked.insert(token.clone(), t.clone());
        }

        return Ok(t);
    }

    async fn update(&self, token: model::TokenKey) -> Result<model::TokenUpdate, TokenDbError> {

        let mut original: Option<model::Token> = None;

        {
            let locked = self.db.lock().await;
            let tok = locked.get(&token).clone();

            original = match tok {
                Some(tkn) => Some(tkn.clone()),
                None => return Err(TokenDbError::TokenNotPresent(token.clone())),
            }
        }

        let mut locked = self.db.lock().await;

        if locked.contains_key(&token) {
            let delta = model::Token::from(token.clone());
            locked.insert(token.clone(), delta.clone());

            match original {
                Some(tok) => return Ok(model::TokenUpdate {
                    original: tok,
                    delta,
                }),
                None => return Err(TokenDbError::TokenNotPresent(token.clone())),
            };
        }

        return Err(TokenDbError::TokenNotPresent(token.clone()));
    }

    async fn invalidate(&self, token: model::TokenKey) -> Result<(), TokenDbError> {
        let mut locked = self.db.lock().await;
        locked.remove(&token);
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
                    return !complement.keys.contains(key)
                }
                cm::message_subscribe_filter::Predicate::Intersection(intersection) => {
                    return intersection.keys.contains(key);
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
        let message = match request.into_inner().inner {
            Some(message) => message,
            None => return Ok(Response::new(MessageSendResponse { status: message_send_response::SendStatus::Failure as i32 })),
        };

        let bcast = MessageBroadcast {
            operation: Some(Operation::Send(message)),
        };

        match self.subscribe_tx.send(bcast) {
            Ok(_) => {},
            Err(_) => return Ok(Response::new(MessageSendResponse { status: message_send_response::SendStatus::Failure as i32 })),
        };

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
                                    if !message_subscribe_filter(request.get_ref(), token) {
                                        pass = false;
                                    }
                                }

                                if pass {
                                    tx.send(Ok(update)).await.unwrap();
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

fn token_subscribe_filter(request: &TokenSubscribeRequest, key: &TokenKey) -> bool {
    if let Some(filter) = request.filter.as_ref() {
        if let Some(predicate) = &filter.predicate {
            match predicate {
                cm::token_subscribe_filter::Predicate::Complement(complement) => {
                    return !complement.keys.contains(key);
                }
                cm::token_subscribe_filter::Predicate::Intersection(intersection) => {
                    return intersection.keys.contains(key);
                }
                cm::token_subscribe_filter::Predicate::Union(_) => {
                    return true;
                }
            }
        }
    }

    false
}

#[async_trait]
impl<Db: TokenDb> CmToken for CmTokenService<Db> {
    async fn token_register(
        &self,
        request: Request<TokenRegisterRequest>,
    ) -> Result<Response<TokenRegisterResponse>, Status> {
        let token = match request.into_inner().token {
            Some(tok) => tok,
            None => return Ok(Response::new(TokenRegisterResponse { status: token_register_response::Status::Invalid as i32, token: None })),
        };

        let token = match self.db.insert(token.into()).await {
            Ok(tok) => tok,
            Err(_) => return Ok(Response::new(TokenRegisterResponse { status: token_register_response::Status::Invalid as i32, token: None })),
        };

        let bcast = TokenBroadcast {
            operation: Some(token_broadcast::Operation::Addition(token.clone().into())),
        };

        self.subscribe_tx.send(bcast).unwrap();
        Ok(Response::new(TokenRegisterResponse {
            status: token_register_response::Status::Success as i32,
            token: Some(token.into()),
        }))
    }

    async fn token_update(
        &self,
        request: Request<TokenUpdateRequest>,
    ) -> Result<Response<TokenUpdateResponse>, Status> {
        let original_key = match request.into_inner().key {
            Some(key) => key,
            None => return Ok(Response::new(TokenUpdateResponse {
                token: None,
                timestamp: None
            })),
        }.into();

        let token_update = match self.db.update(original_key).await {
            Ok(tok) => tok,
            Err(_) => return Ok(Response::new(TokenUpdateResponse { token: None, timestamp: None })),
        };

        let bcast = TokenBroadcast {
            operation: Some(token_broadcast::Operation::Update(TokenUpdate {
                original: Some(token_update.original.clone().into()),
                delta: Some(token_update.delta.clone().into()),
            })),
        };

        println!("testeronis");

        self.subscribe_tx.send(bcast).unwrap();

        let timestamp = cm::Token::from(token_update.delta.clone()).timestamp;
        Ok(Response::new(TokenUpdateResponse {
            token: Some(token_update.delta.into()),
            timestamp,
        }))
    }

    type TokenSubscribeStream = ReceiverStream<Result<TokenBroadcast, Status>>;

    async fn token_subscribe(
        &self,
        request: Request<TokenSubscribeRequest>,
    ) -> Result<Response<Self::TokenSubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let mut subscribe_rx = self.subscribe_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(update) = subscribe_rx.recv().await {
                if let Some(operation) = &update.operation {

                    let pass = match operation {
                        token_broadcast::Operation::Addition(addition) => {
                            addition.key.as_ref().map_or(false, |key| {
                                token_subscribe_filter(request.get_ref(), key)
                            })
                        }
                        token_broadcast::Operation::Invalidation(invalidation) => {
                            invalidation.key.as_ref().map_or(false, |key| {
                                token_subscribe_filter(request.get_ref(), key)
                            })
                        }
                        token_broadcast::Operation::Update(update) => {
                            update.original.as_ref().map_or(false, |key| {
                                token_subscribe_filter(
                                    request.get_ref(),
                                    key.key.as_ref().unwrap(),
                                )
                            })
                        }
                    };

                    if pass {
                        tx.send(Ok(update)).await.unwrap();
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:10000".parse().unwrap();

    println!("Service listening on: {}", addr);

    let message_broadcast = broadcast::channel(16);

    let message = CmMessageService {
        subscribe_tx: message_broadcast.0,
    };

    let token_broadcast = broadcast::channel(16);

    let token = CmTokenService {
        db: Arc::new(TokenDbInMemory::new()),
        subscribe_tx: token_broadcast.0,
    };

    let message_svc = CmMessageServer::new(message);
    let token_svc = CmTokenServer::new(token);

    Server::builder()
        .add_service(message_svc)
        .add_service(token_svc)
        .serve(addr)
        .await?;

    Ok(())
}
