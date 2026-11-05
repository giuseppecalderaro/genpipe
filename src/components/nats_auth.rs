//! NATS authentication and credential-safe logging helpers.
//!
//! Secrets never need to live in a config file: the built-in `nats`
//! components resolve them from environment variables (or a `.creds`
//! file path) at construction time.  URLs are always passed through
//! [`redact_url`] before being logged or embedded in error messages.

use std::fmt;
use std::path::PathBuf;

use async_nats::ConnectOptions;

use crate::config::ComponentConfig;
use crate::error::Error;
use crate::Result;

/// How a NATS client authenticates to the server.
///
/// The [`Debug`] implementation never prints secret material.
#[derive(Clone, Default)]
pub enum NatsAuth {
    /// No explicit credentials (credentials embedded in the URL, if any,
    /// are still honoured by the client).
    #[default]
    None,
    /// Username / password authentication.
    UserPassword { user: String, password: String },
    /// Token authentication.
    Token(String),
    /// NKey authentication using the given seed.
    NKey(String),
    /// JWT + NKey credentials loaded from a `.creds` file.
    CredentialsFile(PathBuf),
}

impl fmt::Debug for NatsAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::UserPassword { user, .. } => f
                .debug_struct("UserPassword")
                .field("user", user)
                .field("password", &"***")
                .finish(),
            Self::Token(_) => f.write_str("Token(***)"),
            Self::NKey(_) => f.write_str("NKey(***)"),
            Self::CredentialsFile(p) => f.debug_tuple("CredentialsFile").field(p).finish(),
        }
    }
}

impl NatsAuth {
    /// Build [`ConnectOptions`] for this authentication method.
    async fn connect_options(&self) -> Result<ConnectOptions> {
        Ok(match self {
            Self::None => ConnectOptions::new(),
            Self::UserPassword { user, password } => {
                ConnectOptions::with_user_and_password(user.clone(), password.clone())
            }
            Self::Token(t) => ConnectOptions::with_token(t.clone()),
            Self::NKey(seed) => ConnectOptions::with_nkey(seed.clone()),
            Self::CredentialsFile(path) => ConnectOptions::with_credentials_file(path)
                .await
                .map_err(|e| {
                    Error::Nats(format!("load credentials file {}: {e}", path.display()))
                })?,
        })
    }

    /// Resolve authentication from a component's `settings`.
    ///
    /// Supported settings (at most one method may be configured):
    ///
    /// | Setting | Meaning |
    /// |-|-|
    /// | `user` + `password_env` | username (inline) and name of the env var holding the password |
    /// | `token_env` | name of the env var holding the token |
    /// | `nkey_seed_env` | name of the env var holding the NKey seed |
    /// | `credentials_file` | path to a NATS `.creds` file |
    ///
    /// Inline secrets (`password`, `token`, `nkey`, `nkey_seed`, `seed`,
    /// `credentials`) are rejected so they cannot be committed in a config
    /// file by accident.
    pub fn from_settings(cfg: &ComponentConfig) -> Result<Self> {
        Self::from_settings_with(cfg, |var| std::env::var(var).ok())
    }

    fn from_settings_with(
        cfg: &ComponentConfig,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let name = cfg.resolved_name();

        const INLINE_SECRETS: &[(&str, &str)] = &[
            ("password", "password_env"),
            ("token", "token_env"),
            ("nkey", "nkey_seed_env"),
            ("nkey_seed", "nkey_seed_env"),
            ("seed", "nkey_seed_env"),
            ("credentials", "credentials_file"),
        ];
        for (key, alternative) in INLINE_SECRETS {
            if cfg.settings.contains_key(*key) {
                return Err(Error::Config(format!(
                    "nats component `{name}`: inline secret setting `{key}` is not allowed; \
                     use `{alternative}` instead"
                )));
            }
        }

        let read_env = |setting: &str| -> Result<Option<String>> {
            match cfg.get_str(setting) {
                None => Ok(None),
                Some(var) => env(var).map(Some).ok_or_else(|| {
                    Error::Config(format!(
                        "nats component `{name}`: environment variable `{var}` \
                         (from `{setting}`) is not set"
                    ))
                }),
            }
        };

        let mut methods = Vec::new();

        let user = cfg.get_str("user");
        let password = read_env("password_env")?;
        match (user, password) {
            (Some(user), Some(password)) => methods.push(Self::UserPassword {
                user: user.to_owned(),
                password,
            }),
            (None, None) => {}
            _ => {
                return Err(Error::Config(format!(
                    "nats component `{name}`: `user` and `password_env` must be set together"
                )));
            }
        }
        if let Some(token) = read_env("token_env")? {
            methods.push(Self::Token(token));
        }
        if let Some(seed) = read_env("nkey_seed_env")? {
            methods.push(Self::NKey(seed));
        }
        if let Some(path) = cfg.get_str("credentials_file") {
            methods.push(Self::CredentialsFile(PathBuf::from(path)));
        }

        match methods.len() {
            0 => Ok(Self::None),
            1 => Ok(methods.pop().expect("one method")),
            _ => Err(Error::Config(format!(
                "nats component `{name}`: configure only one of `user`/`password_env`, \
                 `token_env`, `nkey_seed_env` or `credentials_file`"
            ))),
        }
    }
}

/// Connect to `url` with the given authentication.
///
/// Errors never contain the raw URL, only its [redacted](redact_url) form.
pub(crate) async fn connect(url: &str, auth: &NatsAuth) -> Result<async_nats::Client> {
    auth.connect_options()
        .await?
        .connect(url)
        .await
        .map_err(|e| Error::Nats(format!("connect to {}: {e}", redact_url(url))))
}

/// Returns `true` if `url` carries credentials in its userinfo section.
pub fn url_has_credentials(url: &str) -> bool {
    url.split(',').any(|u| userinfo_span(u).is_some())
}

/// Replace any credentials in a (possibly comma-separated) server URL
/// with `***`, keeping the username when present.
///
/// ```
/// use genpipe::components::nats_auth::redact_url;
/// assert_eq!(redact_url("nats://alice:s3cret@host:4222"), "nats://alice:***@host:4222");
/// assert_eq!(redact_url("nats://mytoken@host:4222"), "nats://***@host:4222");
/// assert_eq!(redact_url("nats://host:4222"), "nats://host:4222");
/// ```
pub fn redact_url(url: &str) -> String {
    url.split(',').map(redact_single).collect::<Vec<_>>().join(",")
}

/// Byte range of the userinfo (the part before `@` in the authority).
fn userinfo_span(url: &str) -> Option<(usize, usize)> {
    let start = url.find("://").map(|i| i + 3).unwrap_or(0);
    let authority_len = url[start..]
        .find(['/', '?', '#'])
        .unwrap_or(url.len() - start);
    let authority = &url[start..start + authority_len];
    authority.rfind('@').map(|at| (start, start + at))
}

fn redact_single(url: &str) -> String {
    match userinfo_span(url) {
        None => url.to_owned(),
        Some((start, end)) => {
            let userinfo = &url[start..end];
            let redacted = match userinfo.split_once(':') {
                Some((user, _)) => format!("{user}:***"),
                None => "***".to_owned(),
            };
            format!("{}{}{}", &url[..start], redacted, &url[end..])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg(settings: serde_json::Value) -> ComponentConfig {
        ComponentConfig {
            component_type: "nats".into(),
            name: Some("n".into()),
            from: Vec::new(),
            settings: serde_json::from_value(settings).unwrap(),
        }
    }

    fn env(var: &str) -> Option<String> {
        match var {
            "PW" => Some("hunter2".into()),
            "TOK" => Some("tok123".into()),
            _ => None,
        }
    }

    #[test]
    fn redacts_password_and_token() {
        assert_eq!(redact_url("nats://u:p@h:4222"), "nats://u:***@h:4222");
        assert_eq!(redact_url("tls://tok@h"), "tls://***@h");
        assert_eq!(redact_url("u:p@h:4222"), "u:***@h:4222");
        assert_eq!(redact_url("nats://h:4222/x@y"), "nats://h:4222/x@y");
        assert_eq!(
            redact_url("nats://a:b@h1,nats://h2"),
            "nats://a:***@h1,nats://h2"
        );
        assert!(url_has_credentials("nats://u:p@h"));
        assert!(!url_has_credentials("nats://localhost:4222"));
    }

    #[test]
    fn debug_hides_secrets() {
        let auth = NatsAuth::UserPassword { user: "u".into(), password: "hunter2".into() };
        assert!(!format!("{auth:?}").contains("hunter2"));
        assert!(!format!("{:?}", NatsAuth::Token("tok123".into())).contains("tok123"));
        assert!(!format!("{:?}", NatsAuth::NKey("SUAX".into())).contains("SUAX"));
    }

    #[test]
    fn resolves_from_env() {
        let a = NatsAuth::from_settings_with(&cfg(json!({"user": "u", "password_env": "PW"})), env)
            .unwrap();
        assert!(matches!(a, NatsAuth::UserPassword { ref password, .. } if password == "hunter2"));

        let a = NatsAuth::from_settings_with(&cfg(json!({"token_env": "TOK"})), env).unwrap();
        assert!(matches!(a, NatsAuth::Token(ref t) if t == "tok123"));

        let a = NatsAuth::from_settings_with(&cfg(json!({})), env).unwrap();
        assert!(matches!(a, NatsAuth::None));
    }

    #[test]
    fn rejects_inline_and_ambiguous_settings() {
        for bad in [
            json!({"password": "x"}),
            json!({"token": "x"}),
            json!({"user": "u"}),
            json!({"token_env": "MISSING"}),
            json!({"token_env": "TOK", "credentials_file": "/x.creds"}),
        ] {
            let err = NatsAuth::from_settings_with(&cfg(bad), env).unwrap_err().to_string();
            assert!(!err.contains("hunter2") && !err.contains("tok123"), "{err}");
        }
    }
}
