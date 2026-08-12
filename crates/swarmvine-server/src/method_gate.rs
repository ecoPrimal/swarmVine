// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! Pre-dispatch capability gate (JH-0 / JH-2 ecosystem standard).
//!
//! Classifies every JSON-RPC method as Public or Protected and gates
//! dispatch based on the current mode. Ships in Permissive mode (all calls
//! allowed) per the ecoPrimals `METHOD_GATE_STANDARD`.

use serde::{Deserialize, Serialize};

/// Whether a method is freely callable or requires authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MethodVisibility {
    /// Callable by any peer without credentials.
    Public,
    /// Requires a valid token / caller identity when the gate is enforcing.
    Protected,
}

/// Gate operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GateMode {
    /// All calls allowed regardless of caller identity (JH-0 default).
    Permissive,
    /// Protected methods require valid authentication (JH-2 future).
    Enforcing,
}

/// Resource limits carried in an ionic token (JH-2 prep).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResourceEnvelope {
    /// Maximum memory in MB the token grants.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mem_mb: Option<u64>,
    /// Maximum CPU cores the token grants.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cpu_cores: Option<u32>,
    /// Maximum timeout in milliseconds per dispatch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_timeout_ms: Option<u64>,
    /// Methods this token may call. Empty = all allowed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) method_allowlist: Vec<String>,
}

impl ResourceEnvelope {
    /// Check whether the envelope allows calling `method`.
    pub(crate) fn allows_method(&self, method: &str) -> bool {
        self.method_allowlist.is_empty() || self.method_allowlist.iter().any(|m| m == method)
    }
}

/// Caller identity and resource context (JH-2 prep).
#[derive(Debug, Clone, Default)]
pub(crate) struct CallerContext {
    /// Caller identity (e.g. DID from ionic token).
    pub(crate) identity: Option<String>,
    /// Resource envelope from token.
    pub(crate) envelope: Option<ResourceEnvelope>,
}

impl CallerContext {
    /// Anonymous caller with no token (permissive-mode default).
    pub(crate) fn anonymous() -> Self {
        Self::default()
    }

    /// Whether this caller presented a token with an envelope.
    #[cfg(test)]
    pub(crate) const fn has_envelope(&self) -> bool {
        self.envelope.is_some()
    }
}

/// Gate denial — contains JSON-RPC error code and message.
#[derive(Debug, Clone)]
pub(crate) struct GateDenial {
    /// JSON-RPC error code.
    pub(crate) code: i32,
    /// Human-readable message.
    pub(crate) message: String,
}

/// Pre-dispatch capability gate.
pub(crate) struct MethodGate {
    mode: GateMode,
}

impl MethodGate {
    /// Create a new gate in the given mode.
    pub(crate) const fn new(mode: GateMode) -> Self {
        Self { mode }
    }

    /// Create a gate in permissive mode (JH-0 default).
    pub(crate) const fn permissive() -> Self {
        Self::new(GateMode::Permissive)
    }

    /// Check whether a method call should be allowed.
    pub(crate) fn check(&self, method: &str) -> Result<(), GateDenial> {
        self.check_with_context(method, &CallerContext::anonymous())
    }

    /// Check method access with full caller context (JH-2).
    pub(crate) fn check_with_context(
        &self,
        method: &str,
        ctx: &CallerContext,
    ) -> Result<(), GateDenial> {
        let visibility = classify_method(method);

        match self.mode {
            GateMode::Permissive => {
                if let Some(ref env) = ctx.envelope
                    && !env.allows_method(method)
                {
                    return Err(GateDenial {
                        code: -32001,
                        message: format!("Token does not permit method: {method}"),
                    });
                }
                Ok(())
            }
            GateMode::Enforcing => match visibility {
                MethodVisibility::Public => Ok(()),
                MethodVisibility::Protected => {
                    if ctx.identity.is_none() {
                        return Err(GateDenial {
                            code: -32002,
                            message: "Authentication required for protected method".into(),
                        });
                    }
                    if let Some(ref env) = ctx.envelope
                        && !env.allows_method(method)
                    {
                        return Err(GateDenial {
                            code: -32001,
                            message: format!("Token does not permit method: {method}"),
                        });
                    }
                    Ok(())
                }
            },
        }
    }
}

/// Classify a method name into its visibility tier.
///
/// Public: health probes, identity, capabilities, auth, lifecycle status,
/// BTSP negotiation. Everything else: protected.
pub(crate) fn classify_method(method: &str) -> MethodVisibility {
    match method {
        "health.liveness" | "health.readiness" | "health.check" | "identity.get"
        | "capabilities.list" | "capability.list" | "lifecycle.status" | "btsp.negotiate"
        | "primal.announce" => MethodVisibility::Public,
        m if m.starts_with("auth.") => MethodVisibility::Public,
        _ => MethodVisibility::Protected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permissive_allows_all() {
        let gate = MethodGate::permissive();
        assert!(gate.check("health.liveness").is_ok());
        assert!(gate.check("some.protected.method").is_ok());
        assert!(gate.check("custom.domain.verb").is_ok());
    }

    #[test]
    fn enforcing_allows_public() {
        let gate = MethodGate::new(GateMode::Enforcing);
        assert!(gate.check("health.liveness").is_ok());
        assert!(gate.check("health.readiness").is_ok());
        assert!(gate.check("health.check").is_ok());
        assert!(gate.check("identity.get").is_ok());
        assert!(gate.check("capabilities.list").is_ok());
        assert!(gate.check("capability.list").is_ok());
        assert!(gate.check("lifecycle.status").is_ok());
        assert!(gate.check("btsp.negotiate").is_ok());
        assert!(gate.check("auth.check").is_ok());
        assert!(gate.check("auth.mode").is_ok());
    }

    #[test]
    fn enforcing_denies_anonymous_on_protected() {
        let gate = MethodGate::new(GateMode::Enforcing);
        let err = gate.check("custom.method").unwrap_err();
        assert_eq!(err.code, -32002);
    }

    #[test]
    fn enforcing_allows_authenticated() {
        let gate = MethodGate::new(GateMode::Enforcing);
        let ctx = CallerContext {
            identity: Some("did:key:z6Mk_test".into()),
            envelope: Some(ResourceEnvelope::default()),
        };
        assert!(gate.check_with_context("custom.method", &ctx).is_ok());
    }

    #[test]
    fn permissive_enforces_token_allowlist() {
        let gate = MethodGate::permissive();
        let ctx = CallerContext {
            identity: Some("did:key:z6Mk_test".into()),
            envelope: Some(ResourceEnvelope {
                method_allowlist: vec!["health.liveness".into()],
                ..ResourceEnvelope::default()
            }),
        };
        assert!(gate.check_with_context("health.liveness", &ctx).is_ok());
        let err = gate.check_with_context("custom.method", &ctx).unwrap_err();
        assert_eq!(err.code, -32001);
    }

    #[test]
    fn enforcing_denies_method_not_in_allowlist() {
        let gate = MethodGate::new(GateMode::Enforcing);
        let ctx = CallerContext {
            identity: Some("did:key:z6Mk_test".into()),
            envelope: Some(ResourceEnvelope {
                method_allowlist: vec!["health.liveness".into()],
                ..ResourceEnvelope::default()
            }),
        };
        let err = gate.check_with_context("custom.method", &ctx).unwrap_err();
        assert_eq!(err.code, -32001);
    }

    #[test]
    fn classify_public_methods() {
        let public = [
            "health.liveness",
            "health.readiness",
            "health.check",
            "identity.get",
            "capabilities.list",
            "capability.list",
            "lifecycle.status",
            "btsp.negotiate",
            "primal.announce",
            "auth.check",
            "auth.mode",
            "auth.peer_info",
        ];
        for m in &public {
            assert_eq!(classify_method(m), MethodVisibility::Public, "{m}");
        }
    }

    #[test]
    fn classify_protected_methods() {
        let protected = ["custom.method", "data.store", "compute.run", "unknown"];
        for m in &protected {
            assert_eq!(classify_method(m), MethodVisibility::Protected, "{m}");
        }
    }

    #[test]
    fn resource_envelope_empty_allows_all() {
        let env = ResourceEnvelope::default();
        assert!(env.allows_method("anything"));
    }

    #[test]
    fn resource_envelope_restricts_to_allowlist() {
        let env = ResourceEnvelope {
            method_allowlist: vec!["health.liveness".into()],
            ..ResourceEnvelope::default()
        };
        assert!(env.allows_method("health.liveness"));
        assert!(!env.allows_method("custom.method"));
    }

    #[test]
    fn caller_context_anonymous() {
        let ctx = CallerContext::anonymous();
        assert!(ctx.identity.is_none());
        assert!(!ctx.has_envelope());
    }

    #[test]
    fn gate_mode_serde() {
        assert_eq!(
            serde_json::to_string(&GateMode::Permissive).unwrap(),
            "\"permissive\""
        );
        assert_eq!(
            serde_json::to_string(&GateMode::Enforcing).unwrap(),
            "\"enforcing\""
        );
    }
}
