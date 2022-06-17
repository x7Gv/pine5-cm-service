use cm::cm_message_server::{CmMessage, CmMessageServer};
use cm::cm_token_server::{CmToken, CmTokenServer};
use cm::message_broadcast::Operation;
use cm::message_send_response::SendStatus;
use cm::{
    HealthCheckRequest, HealthCheckResponse, MessageBroadcast, MessageSendRequest,
    MessageSendResponse, MessageSubscribeRequest, TokenBroadcast, TokenRegisterRequest,
    TokenRegisterResponse, TokenSubscribeRequest, TokenUpdateRequest, TokenUpdateResponse,
};

use tonic::{Request, Response, Status};

use std::{borrow::Borrow, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::async_trait;

pub mod cm {
    tonic::include_proto!("cm");
}

#[derive(Debug)]
pub struct CmMessageService {
    subscribe_tx: broadcast::Sender<MessageBroadcast>,
}

#[derive(Debug)]
pub struct CmTokenService {
    subscribe_tx: broadcast::Sender<TokenBroadcast>,
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

    fn subscribe_filter(request: &MessageSubscribeRequest, bcast: &MessageBroadcast) -> bool {
        if let Some(filter) = request.filter.as_ref() {
        }

        false
    }

    async fn message_subscribe(
        &self,
        request: Request<MessageSubscribeRequest>,
    ) -> Result<Response<Self::MessageSubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let mut subscribe_rx = self.subscribe_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(update) = subscribe_rx.recv().await {
                tx.send(Ok(update)).await.unwrap();
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
impl CmToken for CmTokenService {
    async fn token_register(
        &self,
        request: Request<TokenRegisterRequest>,
    ) -> Result<Response<TokenRegisterResponse>, Status> {
        todo!()
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

fn main() {
    println!("Hello, world!");
}
