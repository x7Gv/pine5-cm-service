use std::sync::Arc;

use futures::TryFutureExt;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Request, Response, Status};
use tracing::info;

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

    // TODO: Token validation
    /// Register a token to the database implementation.
    async fn token_register(
        &self,
        request: Request<TokenRegisterRequest>,
    ) -> Result<Response<TokenRegisterResponse>, Status> {

        let req0 = request.get_ref().clone();

        // Assert that there is a token present in the request.
        let token = match request.into_inner().token {
            Some(tok) => tok,
            None => {
                let status = Status::invalid_argument("token not present");
                info!(status = ?&status, "request failed");
                return Err(status);
            }
        };

        // Token is present. Attempt to insert the token to the database.
        let token = match self.db.insert(token.into()).await {
            Ok(tok) => tok,
            Err(error) => {
                let status = match error {
                    TokenDbError::TokenNotPresent(tok) => {
                        Status::invalid_argument(format!("token `{}` not existing", tok.key))
                    }
                    TokenDbError::Unknown => Status::internal("database failed"),
                };
                info!(status = ?&status, "request failed");
                return Err(status);
            }
        };

        // The insert was successful. Now construct a broadcastable object and send it to subscribers.
        let bcast = TokenBroadcast {
            operation: Some(token_broadcast::Operation::Addition(token.clone().into())),
        };

        match self.subscribe_tx.send(bcast) {
            Ok(_) => {},
            Err(_) => {
                let status = Status::internal("channel broken");
                info!(status = ?&status, "request failed");
                return Err(status);
            }
        }

        info!(
            "\nrpc::TokenRegister :: ({:?}) \n\n{:?}\n",
            &req0,
            &token,
        );

        // Ok, all things executed successfully. Send the response to finalize.
        Ok(Response::new(TokenRegisterResponse {
            token: Some(token.into()),
        }))
    }

    /// Update an exeting token. If the token is not present, throw an error status.
    async fn token_update(
        &self,
        request: Request<TokenUpdateRequest>,
    ) -> Result<Response<TokenUpdateResponse>, Status> {

        // Assert that the token in RPC is actually present.
        let original_key = match request.into_inner().key {
            Some(key) => key,
            None => {
                let status = Status::invalid_argument("token not present");
                info!(status = ?&status, "request failed");
                return Err(status);
            }
        }
        .into();

        // Token is present. Now update it.
        let token_update = match self.db.update(original_key).await {
            Ok(tok) => tok,
            Err(error) => {
                let status = match error {
                    TokenDbError::TokenNotPresent(tok) => {
                        Status::invalid_argument(format!("token `{}` not existing", tok.key))
                    }
                    TokenDbError::Unknown => Status::internal("database failed"),
                };
                info!(status = ?&status, "request failed");
                return Err(status);
            }
        };

        // The update was successful; Construct a broadcastable object and send it to subscribers.
        let bcast = TokenBroadcast {
            operation: Some(token_broadcast::Operation::Update(TokenUpdate {
                original: Some(token_update.original.clone().into()),
                delta: Some(token_update.delta.clone().into()),
            })),
        };

        match self.subscribe_tx.send(bcast) {
            Ok(_) => {},
            Err(_) => {
                let status = Status::internal("channel broken");
                info!(status = ?&status, "request failed");
                return Err(status);
            }
        }

        // Ok, all things executed successfully. Send the response to finalize.
        let timestamp = cm::Token::from(token_update.delta.clone()).timestamp;
        Ok(Response::new(TokenUpdateResponse {
            token: Some(token_update.delta.into()),
            timestamp,
        }))
    }

    type TokenSubscribeStream = ReceiverStream<Result<TokenBroadcast, Status>>;

    /// Mark an agreement to receive token updates as a unary stream.
    async fn token_subscribe(
        &self,
        request: Request<TokenSubscribeRequest>,
    ) -> Result<Response<Self::TokenSubscribeStream>, Status> {
        // spend up an internal mpsc channel for in process streaming.
        let (tx, rx) = mpsc::channel(4);

        let req = request.into_inner().clone();
        let req2 = req.clone();

        // take a new subscribtion for this instance of subscribe task.
        let mut subscribe_rx = self.subscribe_tx.subscribe();

        tokio::spawn(async move {
            while let Ok(update) = subscribe_rx.recv().await {

                // Match the defined operation and handle the set logic.
                if let Some(operation) = &update.operation {
                    // Determine whether or not the processed update is in the domain of the subscriber.
                    let pass = match operation {
                        token_broadcast::Operation::Addition(addition) => addition
                            .key
                            .as_ref()
                            .map_or(false, |key| token_subscribe_filter(&req, key)),
                        token_broadcast::Operation::Invalidation(invalidation) => invalidation
                            .key
                            .as_ref()
                            .map_or(false, |key| token_subscribe_filter(&req, key)),
                        token_broadcast::Operation::Update(update) => {
                            update.original.as_ref().map_or(false, |key| {
                                token_subscribe_filter(&req, key.key.as_ref().unwrap())
                            })
                        }
                    };

                    if pass {
                        // The update is in domain. Send it to the master process for the RPC stream.
                        match tx.send(Ok(update)).await {
                            Ok(_) => {},
                            Err(_) => {
                                info!("channel closed");
                                break;
                            },
                        };
                    }
                }
            }
        });

        info!(
            "\nrpc::TokenSubscribe :: ({:?})", &req2
        );

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
