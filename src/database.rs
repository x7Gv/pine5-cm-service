use crate::model;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::async_trait;

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

    async fn invalidate(&self, token: model::TokenKey) -> Result<(), TokenDbError> {
        let mut locked = self.db.lock().await;
        locked.remove(&token);
        Ok(())
    }
}
