use chrono::NaiveDateTime;
use prost_types::Timestamp;
use crate::cm::{self, Tokens};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TokenKey {
    pub key: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Token {
    pub key: TokenKey,
    pub timestamp: chrono::NaiveDateTime,
}

impl From<TokenKey> for cm::TokenKey {
    fn from(source: TokenKey) -> Self {
        Self { key: source.key }
    }
}

impl From<cm::TokenKey> for TokenKey {
    fn from(source: cm::TokenKey) -> Self {
        Self { key: source.key }
    }
}

impl From<Token> for cm::Token {
    fn from(source: Token) -> Self {
        Self {
            key: Some(source.key.into()),
            timestamp: Some(Timestamp {
                seconds: source.timestamp.timestamp(),
                nanos: 0,
            }),
        }
    }
}

impl From<TokenKey> for Token {
    fn from(source: TokenKey) -> Self {
        Token::new(source)
    }
}

impl TokenKey {
    pub fn new(key: &str) -> Self {
        Self {
            key: key.to_string(),
        }
    }
}

impl Token {
    pub fn new(key: TokenKey) -> Self {
        Self {
            key,
            timestamp: chrono::Utc::now().naive_utc(),
        }
    }
}

impl From<cm::Token> for Token {
    fn from(source: cm::Token) -> Self {
        Self {
            key: source.key.unwrap().into(),
            timestamp: NaiveDateTime::from_timestamp(source.timestamp.unwrap().seconds, 0),
        }
    }
}

impl From<cm::TokenKeys> for Vec<TokenKey> {
    fn from(source: cm::TokenKeys) -> Self {
        source.keys.into_iter().map(|key| TokenKey::from(key)).collect()
    }
}

impl From<Vec<TokenKey>> for cm::TokenKeys {
    fn from(source: Vec<TokenKey>) -> Self {
        Self {
            keys: source.into_iter()
                .map(|key| cm::TokenKey::from(key))
                .collect()
        }
    }
}

impl From<cm::Tokens> for Vec<Token> {
    fn from(source: cm::Tokens) -> Self {
        source.tokens.into_iter().map(|token| Token::from(token)).collect()
    }
}

impl From<Vec<Token>> for Tokens {
    fn from(source: Vec<Token>) -> Self {
        Self {
            tokens: source.into_iter()
                .map(|token| cm::Token::from(token))
                .collect()
        }
    }
}
