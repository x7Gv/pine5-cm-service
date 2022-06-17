use cm::cm_message_server::{CmMessage, CmMessageServer};
use cm::cm_token_server::{CmToken, CmTokenServer};
use cm::message_broadcast::Operation;
use cm::message_send_response::SendStatus;
use cm::{
    HealthCheckRequest, HealthCheckResponse, MessageBroadcast, MessageSendResponse, TokenBroadcast,
};

use std::{borrow::Borrow, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Response, Status};

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
        request: tonic::Request<cm::MessageSendRequest>,
    ) -> Result<tonic::Response<cm::MessageSendResponse>, tonic::Status> {
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
        request: tonic::Request<cm::MessageSubscribeRequest>,
    ) -> Result<tonic::Response<Self::MessageSubscribeStream>, tonic::Status> {
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
        request: tonic::Request<cm::HealthCheckRequest>,
    ) -> Result<tonic::Response<cm::HealthCheckResponse>, tonic::Status> {
        todo!()
    }

    type WatchStream = ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn watch(
        &self,
        request: tonic::Request<cm::HealthCheckRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        todo!()
    }
}

#[async_trait]
impl CmToken for CmTokenService {
    async fn token_register(
        &self,
        request: tonic::Request<cm::TokenRegisterRequest>,
    ) -> Result<tonic::Response<cm::TokenRegisterResponse>, tonic::Status> {
        todo!()
    }

    async fn token_update(
        &self,
        request: tonic::Request<cm::TokenUpdateRequest>,
    ) -> Result<tonic::Response<cm::TokenUpdateResponse>, tonic::Status> {
        todo!()
    }

    type TokenSubscribeStream = ReceiverStream<Result<TokenBroadcast, Status>>;

    async fn token_subscribe(
        &self,
        request: tonic::Request<cm::TokenSubscribeRequest>,
    ) -> Result<tonic::Response<Self::TokenSubscribeStream>, tonic::Status> {
        todo!()
    }

    async fn check(
        &self,
        request: tonic::Request<cm::HealthCheckRequest>,
    ) -> Result<tonic::Response<cm::HealthCheckResponse>, tonic::Status> {
        todo!()
    }

    type WatchStream = ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn watch(
        &self,
        request: tonic::Request<cm::HealthCheckRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        todo!()
    }
}

fn main() {
    println!("Hello, world!");
}
