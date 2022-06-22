use super::cm;
use super::cm::MessageBroadcast;
use super::cm::MessageSubscribeRequest;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::async_trait;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use super::cm::cm_message_server::CmMessage;
use super::cm::message_broadcast::Operation;
use super::cm::HealthCheckRequest;
use super::cm::HealthCheckResponse;
use super::cm::MessageSendRequest;
use super::cm::MessageSendResponse;
use super::cm::TokenKey;

#[derive(Debug)]
pub struct CmMessageService {
    subscribe_tx: broadcast::Sender<MessageBroadcast>,
    subscribe_rx: broadcast::Receiver<MessageBroadcast>,
}

impl CmMessageService {
    pub fn new(
        ch: (
            broadcast::Sender<MessageBroadcast>,
            broadcast::Receiver<MessageBroadcast>,
        ),
    ) -> Self {
        Self {
            subscribe_tx: ch.0,
            subscribe_rx: ch.1,
        }
    }
}

impl Default for CmMessageService {
    fn default() -> Self {
        CmMessageService::new(broadcast::channel(16))
    }
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
            None => {
                let status = Status::invalid_argument("inner message not present");
                return Err(status);
            }
        };

        let bcast = MessageBroadcast {
            operation: Some(Operation::Send(message.clone())),
        };

        match self.subscribe_tx.send(bcast) {
            Ok(_) => {}
            Err(_) => {
                let status = Status::internal("channel broken");
                return Err(status);
            }
        };

        Ok(Response::new(MessageSendResponse {
            sent: Some(message),
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
