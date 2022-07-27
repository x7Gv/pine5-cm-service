use std::{sync::Arc, fmt::Debug};

use fcm::{Client, MessageBuilder, FcmResponse};

use crate::{database::{TokenDb, TokenDbInMemory}, model::{message::Message, token::{Token, TokenKey}}};

pub struct FCMService {
    api_key: String,
    client: Client,
}

impl Debug for FCMService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FCMService: {}", self.api_key)
    }
}

impl FCMService {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            client: Client::new(),
        }
    }

    pub async fn send(&self, message: &Message, target: Vec<TokenKey>) -> anyhow::Result<()> {
        for token in target {
            let mut builder = MessageBuilder::new(&self.api_key, &token.key);
            builder.data(&message.content);
            let resp = self.client.send(builder.finalize()).await?;
        }

        Ok(())
    }
}
