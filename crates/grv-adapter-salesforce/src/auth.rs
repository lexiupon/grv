use crate::config::Connection;
use crate::{Error, Result, integrity, invalid};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OrgId(String);

impl OrgId {
    pub fn parse(value: &str) -> Result<Self> {
        if !value.starts_with("00D")
            || !matches!(value.len(), 15 | 18)
            || !value.bytes().all(|c| c.is_ascii_alphanumeric())
        {
            return Err(invalid("invalid Salesforce org ID"));
        }
        // Fifteen-character IDs are case-sensitive. Canonical coordinates use
        // their equivalent 18-character checksum form without lowercasing.
        let canonical = if value.len() == 15 {
            let mut canonical = value.to_owned();
            const CHECKSUM: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ012345";
            for block in value.as_bytes().as_chunks::<5>().0 {
                let mut bits = 0;
                for (index, byte) in block.iter().enumerate() {
                    if byte.is_ascii_uppercase() {
                        bits |= 1 << index;
                    }
                }
                canonical.push(char::from(CHECKSUM[bits]));
            }
            canonical
        } else {
            value.into()
        };
        Ok(Self(canonical))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn identity(&self) -> String {
        format!("salesforce:{}", self.0)
    }
}
impl TryFrom<String> for OrgId {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        Self::parse(&value)
    }
}
impl From<OrgId> for String {
    fn from(value: OrgId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone)]
pub struct OfflineMetadata {
    pub username: String,
    pub org_id: Option<OrgId>,
}

/// Implementations read local non-secret alias metadata only. They must not
/// invoke `sf org display`, refresh tokens, or perform HTTP requests.
pub trait OfflineMetadataStore {
    fn lookup(&mut self, alias_or_username: &str) -> Result<Option<OfflineMetadata>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionLocator {
    pub canonical_connection: Connection,
    pub identity: Option<String>,
    pub engine_path: Option<String>,
    pub session_lock_path: Option<String>,
}

pub fn locate_connection(
    connection: &Connection,
    metadata: &mut impl OfflineMetadataStore,
) -> Result<ConnectionLocator> {
    connection.validate()?;
    let local = metadata.lookup(&connection.org)?;
    let (username, identity) = match local {
        Some(local) => {
            if local.username.is_empty() || local.username.contains('\0') {
                return Err(Error::new(
                    "INTEGRITY_FAILURE",
                    "invalid local alias metadata",
                ));
            }
            (local.username, local.org_id.map(|id| id.identity()))
        }
        None => (connection.org.clone(), None),
    };
    Ok(ConnectionLocator {
        canonical_connection: Connection {
            org: username,
            api_version: connection.api_version.clone(),
        },
        identity,
        engine_path: None,
        session_lock_path: None,
    })
}

/// Private authentication data intentionally implements neither Serialize nor
/// Debug. Public records expose only the verified org identity/API version.
pub struct AuthenticatedSession {
    org_id: OrgId,
    instance_url: String,
    access_token: String,
}

impl AuthenticatedSession {
    pub fn new(org_id: OrgId, instance_url: String, access_token: String) -> Result<Self> {
        let host = instance_url
            .strip_prefix("https://")
            .unwrap_or("")
            .trim_end_matches('/');
        if host.is_empty()
            || !host
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b':'))
            || access_token.is_empty()
            || access_token.len() > 64 * 1024
            || !access_token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(Error::new(
                "ADAPTER_FAILURE",
                "invalid authenticated connection coordinates",
            ));
        }
        Ok(Self {
            org_id,
            instance_url,
            access_token,
        })
    }
    pub fn org_id(&self) -> &OrgId {
        &self.org_id
    }
    /// Supply private credentials directly to an HTTP implementation. They must
    /// never be logged or put in request identity, journals, or public output.
    pub fn with_credentials<T>(&self, send: impl FnOnce(&str, &str) -> T) -> T {
        send(&self.instance_url, &self.access_token)
    }
}

pub trait AuthenticationBackend {
    /// Resolve/refresh credentials privately and independently verify org ID.
    fn authenticate_and_verify(&mut self, canonical: &Connection) -> Result<AuthenticatedSession>;
}

pub struct BoundConnection {
    locator: ConnectionLocator,
    authentication_attempted: bool,
    session: Option<std::sync::Arc<AuthenticatedSession>>,
}

impl BoundConnection {
    pub fn bind(locator: ConnectionLocator) -> Result<Self> {
        locator.canonical_connection.validate()?;
        if locator.engine_path.is_some() || locator.session_lock_path.is_some() {
            return Err(Error::new(
                "PROTOCOL_FAILURE",
                "Salesforce locator cannot name engine locks",
            ));
        }
        Ok(Self {
            locator,
            authentication_attempted: false,
            session: None,
        })
    }

    pub fn authenticate(
        &mut self,
        backend: &mut impl AuthenticationBackend,
        expected_identity: Option<&str>,
    ) -> Result<String> {
        if self.authentication_attempted {
            return Err(Error::new(
                "PROTOCOL_FAILURE",
                "authentication may be invoked at most once per handle",
            ));
        }
        self.authentication_attempted = true;
        let session = backend
            .authenticate_and_verify(&self.locator.canonical_connection)
            .map_err(|_| Error::new("ADAPTER_FAILURE", "Salesforce authentication failed"))?;
        let identity = session.org_id.identity();
        if expected_identity.is_some_and(|expected| expected != identity)
            || self
                .locator
                .identity
                .as_deref()
                .is_some_and(|expected| expected != identity)
        {
            return Err(Error::new(
                "REQUEST_MISMATCH",
                "authenticated org differs from fixed org identity",
            ));
        }
        self.session = Some(std::sync::Arc::new(session));
        Ok(identity)
    }

    pub fn session(&self) -> Result<&AuthenticatedSession> {
        self.session
            .as_deref()
            .ok_or_else(|| Error::new("ADAPTER_FAILURE", "connection has not authenticated"))
    }
    pub fn shared_session(&self) -> Result<std::sync::Arc<AuthenticatedSession>> {
        self.session
            .clone()
            .ok_or_else(|| Error::new("ADAPTER_FAILURE", "connection has not authenticated"))
    }
}

/// The host's supervised helper runner consumes this spec. No shell expansion
/// occurs, and automatic Salesforce CLI file logging is disabled. The helper
/// output is private credential-bearing input, never a public command result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelperCommand {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
}

pub fn authentication_helper(connection: &Connection) -> Result<HelperCommand> {
    connection.validate()?;
    Ok(HelperCommand {
        program: "sf".into(),
        args: vec![
            "org".into(),
            "display".into(),
            "--target-org".into(),
            connection.org.clone(),
            "--json".into(),
        ],
        env: vec![
            ("SF_DISABLE_LOG_FILE".into(), "true".into()),
            ("SFDX_DISABLE_LOG_FILE".into(), "true".into()),
            // CLI 2.152.14 redacts org display credentials by default, including
            // JSON. This private supervised pipe is the credential boundary.
            ("SF_TEMP_SHOW_SECRETS".into(), "true".into()),
            ("DEBUG".into(), "".into()),
            ("SF_LOG_LEVEL".into(), "error".into()),
            ("SFDX_LOG_LEVEL".into(), "error".into()),
            ("NODE_OPTIONS".into(), "".into()),
            ("NODE_DEBUG".into(), "".into()),
            ("NODE_PATH".into(), "".into()),
        ],
    })
}

pub trait OrgVerifier {
    fn verify_org(&mut self, session: &AuthenticatedSession, api_version: &str) -> Result<OrgId>;
}
impl<H: crate::http::HttpExecutor> OrgVerifier for crate::http::SalesforceHttp<H> {
    fn verify_org(&mut self, session: &AuthenticatedSession, api_version: &str) -> Result<OrgId> {
        self.verify_org(session, api_version)
    }
}

pub struct CliAuthentication<V> {
    pub program: std::ffi::OsString,
    pub supervisor: Option<std::ffi::OsString>,
    pub verifier: V,
    pub cancellation: crate::runtime::Cancellation,
}
impl<V: OrgVerifier> AuthenticationBackend for CliAuthentication<V> {
    fn authenticate_and_verify(&mut self, canonical: &Connection) -> Result<AuthenticatedSession> {
        let helper = authentication_helper(canonical)?;
        let spec = crate::runtime::ProcessSpec {
            program: self.program.clone(),
            supervisor: self.supervisor.clone(),
            args: helper.args.into_iter().map(Into::into).collect(),
            env: helper
                .env
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            stdin: vec![],
            stdout_limit: 1024 * 1024,
            timeout: std::time::Duration::from_secs(60),
        };
        let spec = crate::keychain::prepare(spec, &self.cancellation)?;
        let output = crate::runtime::run(spec, &self.cancellation)?;
        if !output.status.success() {
            return Err(Error::new(
                "ADAPTER_FAILURE",
                "Salesforce authentication helper failed",
            ));
        }
        let value = grv_adapter_wire::json::parse(&output.stdout).map_err(|_| {
            Error::new(
                "INTEGRITY_FAILURE",
                "Salesforce helper private response is invalid",
            )
        })?;
        if value.get("status").and_then(serde_json::Value::as_u64) != Some(0) {
            return Err(Error::new(
                "ADAPTER_FAILURE",
                "Salesforce helper authentication failed",
            ));
        }
        let result = value
            .get("result")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| integrity("Salesforce helper private session is missing"))?;
        let text = |key| {
            result
                .get(key)
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| integrity("Salesforce helper private session is incomplete"))
        };
        let id = OrgId::parse(text("id")?)?;
        let session = AuthenticatedSession::new(
            id.clone(),
            text("instanceUrl")?.into(),
            text("accessToken")?.into(),
        )?;
        let verified = self.verifier.verify_org(&session, &canonical.api_version)?;
        if id != verified {
            return Err(Error::new(
                "REQUEST_MISMATCH",
                "Salesforce helper org differs from authenticated org",
            ));
        }
        Ok(session)
    }
}
