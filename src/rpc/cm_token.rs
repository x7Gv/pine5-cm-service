use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Request, Response, Status};

use crate::{
    database::{self, TokenDb, TokenDbError, TokenDbInMemory},
    rpc::cm::TokenUpdate,
};

use super::cm::{
    self, cm_token_server::CmToken, token_broadcast, HealthCheckRequest, HealthCheckResponse,
    TokenBroadcast, TokenKey, TokenRegisterRequest, TokenRegisterResponse, TokenSubscribeRequest,
    TokenUpdateRequest, TokenUpdateResponse,
};

#[derive(Debug)]
pub struct CmTokenService<Db: TokenDb> {
    subscribe_tx: broadcast::Sender<TokenBroadcast>,
    subscribe_rx: broadcast::Receiver<TokenBroadcast>,
    db: Arc<Db>,
}

impl<Db: TokenDb> CmTokenService<Db> {
    pub fn new(
        ch: (
            broadcast::Sender<TokenBroadcast>,
            broadcast::Receiver<TokenBroadcast>,
        ),
        db: Db,
    ) -> Self {
        Self {
            subscribe_tx: ch.0,
            subscribe_rx: ch.1,
            db: Arc::new(db),
        }
    }

    pub fn new_with_db(db: Db) -> Self {
        let ch = broadcast::channel(16);
        Self {
            subscribe_tx: ch.0,
            subscribe_rx: ch.1,
            db: Arc::new(db),
        }
    }
}

impl Default for CmTokenService<TokenDbInMemory> {
    fn default() -> Self {
        CmTokenService::new_with_db(database::TokenDbInMemory::default())
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
            None => {
                let status = Status::invalid_argument("token not present");
                return Err(status);
            }
        };

        let token = match self.db.insert(token.into()).await {
            Ok(tok) => tok,
            Err(error) => {
                let status = match error {
                    TokenDbError::TokenNotPresent(tok) => {
                        Status::invalid_argument(format!("token `{}` not existing", tok.key))
                    }
                    TokenDbError::Unknown => Status::internal("database failed"),
                };
                return Err(status);
            }
        };

        let bcast = TokenBroadcast {
            operation: Some(token_broadcast::Operation::Addition(token.clone().into())),
        };

        self.subscribe_tx.send(bcast).unwrap();
        Ok(Response::new(TokenRegisterResponse {
            token: Some(token.into()),
        }))
    }

    async fn token_update(
        &self,
        request: Request<TokenUpdateRequest>,
    ) -> Result<Response<TokenUpdateResponse>, Status> {
        let original_key = match request.into_inner().key {
            Some(key) => key,
            None => {
                let status = Status::invalid_argument("token not present");
                return Err(status);
            }
        }
        .into();

        let token_update = match self.db.update(original_key).await {
            Ok(tok) => tok,
            Err(error) => {
                let status = match error {
                    TokenDbError::TokenNotPresent(tok) => {
                        Status::invalid_argument(format!("token `{}` not existing", tok.key))
                    }
                    TokenDbError::Unknown => Status::internal("database failed"),
                };
                return Err(status);
            }
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
                        token_broadcast::Operation::Addition(addition) => addition
                            .key
                            .as_ref()
                            .map_or(false, |key| token_subscribe_filter(request.get_ref(), key)),
                        token_broadcast::Operation::Invalidation(invalidation) => invalidation
                            .key
                            .as_ref()
                            .map_or(false, |key| token_subscribe_filter(request.get_ref(), key)),
                        token_broadcast::Operation::Update(update) => {
                            update.original.as_ref().map_or(false, |key| {
                                token_subscribe_filter(request.get_ref(), key.key.as_ref().unwrap())
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
