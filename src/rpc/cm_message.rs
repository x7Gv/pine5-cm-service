use std::sync::Arc;

use crate::model::message::Message;
use crate::model::token::TokenKey;

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
use tracing::debug;
use tracing::info;

use super::cm::cm_message_server::CmMessage;
use super::cm::message_broadcast::Operation;
use super::cm::HealthCheckRequest;
use super::cm::HealthCheckResponse;
use super::cm::MessageSendRequest;
use super::cm::MessageSendResponse;

#[derive(Debug)]
pub struct CmMessageService {
    fcm: Arc<crate::fcm::FCMService>,
    subscribe_tx: broadcast::Sender<MessageBroadcast>,
    subscribe_rx: broadcast::Receiver<MessageBroadcast>,
}

impl CmMessageService {
    pub fn new(
        fcm: Arc<crate::fcm::FCMService>,
        ch: (
            broadcast::Sender<MessageBroadcast>,
            broadcast::Receiver<MessageBroadcast>,
        ),
    ) -> Self {
        Self {
            fcm,
            subscribe_tx: ch.0,
            subscribe_rx: ch.1,
        }
    }
}

fn message_subscribe_filter(request: &MessageSubscribeRequest, key: &cm::TokenKey) -> bool {
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

fn messages_subscribe_filter(request: &MessageSubscribeRequest, keys: Vec<cm::TokenKey>) -> bool {
    if request.filter.as_ref().is_none() {
        return false;
    }

    let filter = request.filter.as_ref().unwrap();

    if let Some(predicate) = &filter.predicate {
        match predicate {
            cm::message_subscribe_filter::Predicate::Complement(_) => {
                for key in keys.iter() {
                    if !message_subscribe_filter(request, key) {
                        return false;
                    }
                }
                return true;
            }
            cm::message_subscribe_filter::Predicate::Intersection(_) => {
                for key in keys.iter() {
                    if !message_subscribe_filter(request, key) {
                        return false;
                    }
                }
                return true;
            }
            cm::message_subscribe_filter::Predicate::Union(_) => {
                return true;
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
        // Assert that there is an inner message present in the request.
        let message = match &request.get_ref().inner {
            Some(message) => message,
            None => {
                let status = Status::invalid_argument("inner message not present");
                info!(status = ?&status, "request failed");
                return Err(status);
            }
        };

        // Message is present. Now construct a broadcastable object and send it to the subscribers.
        let bcast = MessageBroadcast {
            operation: Some(Operation::Send(message.clone())),
        };

        // Send through the broadcast channel.
        match self.subscribe_tx.send(bcast) {
            Ok(_) => {}
            Err(_) => {
                let status = Status::internal("channel broken");
                info!(status = ?&status, "request failed");
                return Err(status);
            }
        };

        info!(
            "\nrpc#MessageSend :: ({:?}) \n\n{:?}\n",
            &request.get_ref(),
            &message
        );

        let fcm_message: Message = message.clone().into();
        let tokens: Vec<TokenKey> = message.clone().codomain.unwrap().into();

        self.fcm.send(&fcm_message, tokens).await.unwrap();

        // Ok, all things executed successfully. Send the response to finalize.
        Ok(Response::new(MessageSendResponse {
            sent: Some(message.clone()),
        }))
    }

    type MessageSubscribeStream = ReceiverStream<Result<MessageBroadcast, Status>>;

    async fn message_subscribe(
        &self,
        request: Request<MessageSubscribeRequest>,
    ) -> Result<Response<Self::MessageSubscribeStream>, Status> {
        // Spend up an internal mpsc channel for in process streaming.
        let (tx, rx) = mpsc::channel(4);

        let req = request.into_inner().clone();
        let req2 = req.clone();

        // Take a new subscription for this instance of subscribe task.
        let mut subscribe_rx = self.subscribe_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(update) = subscribe_rx.recv().await {
                info!("message recv");

                // Match the defined operation and handle the set logic.
                if let Some(operation) = update.clone().operation {
                    info!("{:?}", operation);

                    match operation {
                        Operation::Send(message) => {
                            if let Some(codomain) = message.codomain {
                                // Determine whether or not the the processed update is in the domain of the subscriber.
                                println!("{:?}", codomain.keys);
                                if messages_subscribe_filter(&req, codomain.keys) {
                                    match tx.send(Ok(update)).await {
                                        Ok(_) => {}
                                        Err(_) => {
                                            info!("channel closed");
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        info!("\nrpc#MessageSubscribe :: ({:?})", &req2);

        // Subscribed successfully, begin streaming.
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
