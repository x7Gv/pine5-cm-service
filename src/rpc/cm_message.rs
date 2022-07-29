use super::cm;
use super::cm::NotificationBroadcast;
use super::cm::NotificationSubscribeRequest;
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
use super::cm::notification_broadcast::Operation;
use super::cm::HealthCheckRequest;
use super::cm::HealthCheckResponse;
use super::cm::NotificationSendRequest;
use super::cm::NotificationSendResponse;
use super::cm::TokenKey;

#[derive(Debug)]
pub struct CmMessageService {
    subscribe_tx: broadcast::Sender<NotificationBroadcast>,
    subscribe_rx: broadcast::Receiver<NotificationBroadcast>,
}

impl CmMessageService {
    pub fn new(
        ch: (
            broadcast::Sender<NotificationBroadcast>,
            broadcast::Receiver<NotificationBroadcast>,
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

fn notification_subscribe_filter(request: &NotificationSubscribeRequest, key: &TokenKey) -> bool {
    if let Some(filter) = request.filter.as_ref() {
        if let Some(predicate) = &filter.predicate {
            match predicate {
                cm::notification_subscribe_filter::Predicate::Complement(complement) => {
                    return !complement.keys.contains(key)
                }
                cm::notification_subscribe_filter::Predicate::Intersection(intersection) => {
                    return intersection.keys.contains(key);
                }
                cm::notification_subscribe_filter::Predicate::Union(_) => {
                    return true;
                }
            }
        }
    }

    false
}

fn notifications_subscribe_filter(request: &NotificationSubscribeRequest, keys: Vec<TokenKey>) -> bool {

    if request.filter.as_ref().is_none() {
        return false;
    }

    let filter = request.filter.as_ref().unwrap();

    if let Some(predicate) = &filter.predicate {
        match predicate {
            cm::notification_subscribe_filter::Predicate::Complement(_) => {
                for key in keys.iter() {
                    if !notification_subscribe_filter(request, key) {
                        return false;
                    }
                }
                return true
            },
            cm::notification_subscribe_filter::Predicate::Intersection(_) => {
                for key in keys.iter() {
                    if !notification_subscribe_filter(request, key) {
                        return false;
                    }
                }
                return true
            },
            cm::notification_subscribe_filter::Predicate::Union(_) => {
                return true;
            },
        }
    }

    false
}

#[async_trait]
impl CmMessage for CmMessageService {
    async fn notification_send(
        &self,
        request: Request<NotificationSendRequest>,
    ) -> Result<Response<NotificationSendResponse>, Status> {
        // Assert that there is an inner notification present in the request.
        let notification = match &request.get_ref().inner {
            Some(notification) => notification,
            None => {
                let status = Status::invalid_argument("inner notification not present");
                info!(status = ?&status, "request failed");
                return Err(status);
            }
        };

        // Notification is present. Now construct a broadcastable object and send it to the subscribers.
        let bcast = NotificationBroadcast {
            operation: Some(Operation::Send(notification.clone())),
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
            "\nrpc#NotificationSend :: ({:?}) \n\n{:?}\n",
            &request.get_ref(),
            &notification
        );

        // Ok, all things executed successfully. Send the response to finalize.
        Ok(Response::new(NotificationSendResponse {
            sent: Some(notification.clone()),
        }))
    }

    type NotificationSubscribeStream = ReceiverStream<Result<NotificationBroadcast, Status>>;

    async fn notification_subscribe(
        &self,
        request: Request<NotificationSubscribeRequest>,
    ) -> Result<Response<Self::NotificationSubscribeStream>, Status> {
        // Spend up an internal mpsc channel for in process streaming.
        let (tx, rx) = mpsc::channel(4);

        let req = request.into_inner().clone();
        let req2 = req.clone();

        // Take a new subscription for this instance of subscribe task.
        let mut subscribe_rx = self.subscribe_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(update) = subscribe_rx.recv().await {

                info!("notification recv");

                // Match the defined operation and handle the set logic.
                if let Some(operation) = update.clone().operation {

                    info!("{:?}", operation);

                    match operation {
                        Operation::Send(notification) => {
                            if let Some(codomain) = notification.codomain {
                                // Determine whether or not the the processed update is in the domain of the subscriber.
                                println!("{:?}", codomain.keys);
                                if notifications_subscribe_filter(&req, codomain.keys) {
                                    match tx.send(Ok(update)).await {
                                        Ok(_) => {},
                                        Err(_) => {
                                            info!("channel closed");
                                            break;
                                        },
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        info!("\nrpc#NotificationSubscribe :: ({:?})", &req2);

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
