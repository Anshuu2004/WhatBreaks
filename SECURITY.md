# Security Policy

## 1. Security Overview

WhatBreaks is designed with security and safety at its core:
- **Strictly Read-Only Operations**: WhatBreaks passively observes infrastructure; it never mutates, scales, patches, or deletes cluster resources.
- **Zero Secret Custody**: WhatBreaks discards all Kubernetes Secret payload values (`.data`, `.stringData`, private keys) at the JSON unmarshaling layer, preserving only metadata and map key names.
- **Tenant Isolation**: All state, evidence, and topology records are strictly partitioned by tenant workspace UUID.
- **Anti-Credential Leakage**: Raw API tokens and kubeconfig files are prohibited in environment variables.

For complete architectural details, review the [Security Model](docs/security.md).

---

## 2. Reporting a Vulnerability

We take the security of WhatBreaks seriously. If you believe you have discovered a security vulnerability, **please do not disclose it in a public GitHub issue, pull request, or discussion**.

### Types of Security Issues to Report Privately
Please report any vulnerability that compromises the safety or confidentiality of WhatBreaks or monitored infrastructure, including:
- Any condition where Kubernetes Secret values, private keys, or credentials could be decoded, retained, logged, or stored (breaking Zero Secret Custody).
- Workspace tenant isolation bypasses in PostgreSQL or the Rust Core Engine.
- Authentication, CSRF token validation, or access-control bypasses in the Go REST API.
- Remote code execution, memory safety issues, or denial-of-service vulnerabilities in the Rust Core Engine or Go platform.
- Unintended mutation or execution capabilities against Kubernetes clusters.

### How to Report Privately
WhatBreaks is maintained as an open-source project by community maintainers. We do not operate a commercial 24/7 security operations center, but we investigate verified security reports as capacity permits.

1. **Preferred Route (GitHub Private Vulnerability Reporting)**:
   If enabled on the repository, navigate to the [Security Advisories page](https://github.com/WhatBreaks/WhatBreaks/security/advisories) on GitHub and click **"Report a vulnerability"**. This creates an encrypted, confidential advisory visible only to maintainers.
2. **Alternative Contact**:
   If private vulnerability reporting is not yet available, reach out directly to the repository maintainer through GitHub ([@aaradhychinche-alt](https://github.com/aaradhychinche-alt)) without posting technical details in public channels.

> **Maintainer Notice / Recommended Action**:
> To enable GitHub's built-in private vulnerability reporting channel:
> 1. Go to repository **Settings** on GitHub.
> 2. Under the "Security" section, select **Code security and analysis**.
> 3. Under "Private vulnerability reporting", click **Enable**.
> 4. Once enabled, external security researchers can privately submit advisories directly through GitHub.

---

## 3. What to Include in a Report

A high-quality, actionable vulnerability report should contain:
- **Component Affected**: Specify whether the issue affects the Go Platform API (`cmd/wb`, `internal/api`), Kubernetes Collector (`internal/collector/k8s`), Rust Core Engine (`core-engine/`, `core-engine-server/`), PostgreSQL state store, or Web UI (`web/`).
- **Description & Impact**: A clear description of the vulnerability and the potential threat scenario (e.g. data exposure, unauthorized access, denial of service).
- **Reproduction Steps**: Step-by-step instructions or a minimal proof-of-concept (PoC) demonstrating the behavior.
- **Environment Details**: Operating System, WhatBreaks version/commit SHA, Go/Rust versions, and Kubernetes distribution.
- **Suggested Remediation**: Any proposed patches or mitigations, if known.

---

## 4. Safe Handling of Sensitive Data

When preparing a report:
- **Never include real production secrets, passwords, cloud access keys, private keys, or sensitive customer data.**
- Use synthetic or sanitized values (e.g. `<REDACTED>`, `testsecret`, `example.com`).
- Synthetic test fixtures are available in the repository (such as in `internal/collector/k8s/secret_safety_test.go`).

---

## 5. Review & Disclosure Process

- **Acknowledgment**: Maintainers will review incoming reports and acknowledge receipt as capacity permits.
- **Assessment**: We will evaluate the report, verify the vulnerability in a local environment, and assess severity.
- **Coordinated Disclosure**: We will work with the reporter to develop and test a fix. Once a patch is available, we will release an update and credit the reporter (unless anonymity is requested).
