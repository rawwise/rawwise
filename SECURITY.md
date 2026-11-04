# Security Policy

## Security Status

RawWise is at **v0.0.1 (pre-release)**. The authentication and authorization
subsystems exist but are **incomplete**, and they are **disabled by default**
in the local startup scripts. Do not treat this project as production-hardened.

| Capability | Current status |
|---|---|
| Authentication (token) | Partial. `AuthFilter`, `AuthRoutes`, `DefaultAuthMetastore` exist; disabled by default |
| Authorization / RBAC | Partial. Role and grant model with audit logging exists, but is not enforced on every endpoint |
| Admin separation | `/api/admin/*` checks for super admin only when authentication is enabled |
| TLS | Disabled by default (`rawwise.backend.tls.enabled=false`) |
| Multi-tenant isolation | Not implemented |
| Security audit trail | Auth grant/revoke/role operations are logged; not a complete security audit log |

When authentication is disabled while `context.enabled=true` (the default local
profile), the request filter injects an **anonymous ADMIN principal** and emits
a warning banner. In that state every API is reachable without credentials,
and the HTTP server binds to all interfaces by default.

**Do not expose RawWise to a public network or an untrusted LAN in this state.**
Bind it to loopback, restrict it with a firewall, or place it behind a reverse
proxy that enforces authentication.

## Supported Versions

| Version | Supported |
|---|---|
| 0.0.1 (pre-release) | Security fixes only, best effort |

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Report privately via GitHub [@rawwise](https://github.com/rawwise), or enable
**Private vulnerability reporting** in the repository's Security tab
(recommended). Do not open a public issue for security reports.

Contact email: **352270501+rawwise@users.noreply.github.com** (GitHub noreply
address; it identifies the maintainer but does not receive inbound mail — use
GitHub to reach the maintainer).

Include:

- Affected component, version, and configuration (especially whether
  authentication was enabled)
- Reproduction steps or proof of concept
- Impact assessment (data exposure, RCE, DoS, privilege escalation, etc.)
- Any suggested mitigation

You will receive an acknowledgement. Because this is a pre-release project,
fixes are provided on a best-effort basis with no response-time commitment.

## Scope

In scope:

- Remote code execution, command or SQL injection, SSRF, path traversal
- Authentication bypass or privilege escalation **when authentication is enabled**
- Unauthorized access to stored data, credentials, or tokens
- Denial of service reachable by an unauthenticated network client
- Vulnerabilities in the build, packaging, or deployment scripts in this repository

Known limitations — **not** treated as vulnerabilities unless they cause impact
beyond the documented state:

- Missing fine-grained RBAC enforcement or multi-tenant isolation
- Unauthenticated operation when the operator explicitly disables authentication
  (`rawwise.auth.explicitly-unsafe-disable=true`)
- TLS being disabled by default in local profiles
- Vulnerabilities in third-party dependencies without a demonstrated impact on
  RawWise (please report those upstream)

## Deployment Hardening

For any non-local deployment:

1. Enable authentication (`-Drawwise.auth.enabled=true`) and provision tokens
   through a token directory; never leave `explicitly-unsafe-disable=true`.
2. Bind the HTTP server to loopback or place it behind an authenticating proxy.
3. Enable TLS for frontend, backend, and gRPC endpoints.
4. Move data paths off `/tmp` and restrict filesystem permissions.
5. Do not commit `.env` or `deploy/deploy.conf`; use the provided templates.
