pub mod token {
    use super::*;

    use std::sync::Arc;

    use crate::cm::{self, Tokens};
    use chrono::NaiveDateTime;
    use prost_types::Timestamp;

    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    pub struct TokenKey {
        pub key: Arc<str>,
    }

    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    pub struct Token {
        pub key: TokenKey,
        pub timestamp: chrono::NaiveDateTime,
    }

    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    pub struct TokenUpdate {
        pub original: Token,
        pub delta: Token,
    }

    impl From<TokenKey> for cm::TokenKey {
        fn from(source: TokenKey) -> Self {
            Self {
                key: source.key.to_string(),
            }
        }
    }

    impl From<&TokenKey> for cm::TokenKey {
        fn from(source: &TokenKey) -> Self {
            Self {
                key: source.key.to_string(),
            }
        }
    }

    impl From<cm::TokenKey> for TokenKey {
        fn from(source: cm::TokenKey) -> Self {
            Self {
                key: Arc::from(source.key),
            }
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

    impl From<&Token> for cm::Token {
        fn from(source: &Token) -> Self {
            Self {
                key: Some(source.key.clone().into()),
                timestamp: Some(Timestamp {
                    seconds: source.timestamp.timestamp(),
                    nanos: 0,
                }),
            }
        }
    }

    impl From<TokenUpdate> for cm::TokenUpdate {
        fn from(source: TokenUpdate) -> Self {
            Self {
                original: Some(source.original.into()),
                delta: Some(source.delta.into()),
            }
        }
    }

    impl From<cm::TokenUpdate> for TokenUpdate {
        fn from(source: cm::TokenUpdate) -> Self {
            Self {
                original: source.original.unwrap().into(),
                delta: source.delta.unwrap().into(),
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
                key: Arc::from(key),
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
            source.keys.into_iter().map(TokenKey::from).collect()
        }
    }

    impl From<&[TokenKey]> for cm::TokenKeys {
        fn from(source: &[TokenKey]) -> Self {
            Self {
                keys: source.iter().map(cm::TokenKey::from).collect(),
            }
        }
    }

    impl From<cm::Tokens> for Vec<Token> {
        fn from(source: cm::Tokens) -> Self {
            source.tokens.into_iter().map(Token::from).collect()
        }
    }

    impl From<&[Token]> for Tokens {
        fn from(source: &[Token]) -> Self {
            Self {
                tokens: source.iter().map(cm::Token::from).collect(),
            }
        }
    }
}

pub mod message {
    use chrono::NaiveDateTime;
    use prost_types::Timestamp;
    use std::collections::HashMap;

    use crate::rpc::cm::{self, TokenKeys};

    use super::token::TokenKey;

    pub struct Message {
        pub content: HashMap<String, String>,
        pub codomain: Vec<TokenKey>,
        pub timestamp: chrono::NaiveDateTime,
    }

     impl From<Message> for cm::Message {
        fn from(source: Message) -> Self {
            let codomain: TokenKeys = source.codomain.as_slice().into();

            Self {
                content: source.content,
                codomain: Some(codomain),
                timestamp: Some(Timestamp {
                    seconds: source.timestamp.timestamp(),
                    nanos: 0,
                }),
            }
        }
    }

    impl From<cm::Message> for Message {
        fn from(source: cm::Message) -> Self {
            Self {
                content: source.content,
                codomain: source.codomain.unwrap().into(),
                timestamp: NaiveDateTime::from_timestamp(source.timestamp.unwrap().seconds, 0),
            }
        }
    }

    impl From<&Message> for cm::Message {
        fn from(source: &Message) -> Self {
            let codomain: TokenKeys = source.codomain.as_slice().into();

            Self {
                content: source.content.clone(),
                codomain: Some(codomain),
                timestamp: Some(Timestamp {
                    seconds: source.timestamp.timestamp(),
                    nanos: 0,
                }),
            }
        }
    }

    impl From<&cm::Message> for Message {
        fn from(source: &cm::Message) -> Self {
            Self {
                content: source.content.clone(),
                codomain: source.codomain.clone().unwrap().into(),
                timestamp: NaiveDateTime::from_timestamp(source.timestamp.clone().unwrap().seconds, 0),
            }
        }
    }
}
