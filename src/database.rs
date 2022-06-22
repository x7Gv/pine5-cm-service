use crate::model;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::async_trait;
use tracing::{debug, info, trace};

#[async_trait]
pub trait TokenDb: Send + Sync + 'static {
    async fn insert(&self, token: model::TokenKey) -> Result<model::Token, TokenDbError>;
    async fn update(&self, token: model::TokenKey) -> Result<model::TokenUpdate, TokenDbError>;
    async fn invalidate(&self, token: model::TokenKey) -> Result<(), TokenDbError>;
}

#[derive(Debug)]
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
    #[tracing::instrument]
    async fn insert(&self, token: model::TokenKey) -> Result<model::Token, TokenDbError> {
        let t = model::Token::from(token.clone());

        {
            debug!("preparing to lock database");
            let mut locked = self.db.lock().await;
            debug!("database locked");

            locked.insert(token.clone(), t.clone());
            info!("inserting to database");
        }
        debug!("database unlocked");

        return Ok(t);
    }

    #[tracing::instrument]
    async fn update(&self, token: model::TokenKey) -> Result<model::TokenUpdate, TokenDbError> {
        let mut original: Option<model::Token> = None;

        {
            debug!("preparing to lock database");
            let locked = self.db.lock().await;
            debug!("database locked");
            let tok = locked.get(&token).clone();

            original = match tok {
                Some(tkn) => {
                    debug!("value to update selected");
                    Some(tkn.clone())
                }
                None => {
                    debug!("value to update not selected");
                    return Err(TokenDbError::TokenNotPresent(token.clone()));
                }
            }
        }
        debug!("database unlocked");

        debug!("preparing to lock database");
        let mut locked = self.db.lock().await;
        debug!("database locked");

        if locked.contains_key(&token) {
            let delta = model::Token::from(token.clone());
            locked.insert(token.clone(), delta.clone());
            info!("updating to database");

            match original {
                Some(tok) => {
                    return Ok(model::TokenUpdate {
                        original: tok,
                        delta,
                    })
                }
                None => return Err(TokenDbError::TokenNotPresent(token.clone())),
            };
        }

        return Err(TokenDbError::TokenNotPresent(token.clone()));
    }

    #[tracing::instrument]
    async fn invalidate(&self, token: model::TokenKey) -> Result<(), TokenDbError> {
        debug!("preparing to lock database");
        let mut locked = self.db.lock().await;
        debug!("database locked");
        locked.remove(&token);
        info!("removed from database");
        Ok(())
    }
}
