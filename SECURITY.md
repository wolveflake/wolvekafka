# Security policy

WolveKafka is early-stage software. We investigate security reports on a
best-effort basis and do not currently promise a response or fix deadline.
Operators should use Kafka authentication/TLS and keep configuration files
containing credentials out of public repositories and ConfigMaps. See the
[security and delivery guide](docs/delivery-and-security.md).

## Supported versions

Security fixes are targeted at the current `main` branch and, once public
releases exist, the latest release line. Older versions are not guaranteed
backports. Check [releases](https://github.com/dguptawolveflake/wolvekafka/releases)
for available updates.

## Report privately

Do not post exploit details, credentials, or affected customer data in a
public issue or pull request.

Email [contacts@wolveflake.io](mailto:contacts@wolveflake.io) with the subject
**WolveKafka security report**. This is the project's current private
reporting contact. If the repository's [Security advisories](https://github.com/dguptawolveflake/wolvekafka/security/advisories)
page offers **Report a vulnerability**, that private form is also suitable.

The repository is currently private, and GitHub's external private
vulnerability-reporting feature is available only to public repositories.
Before public launch, maintainers should verify that this mailbox is monitored
and consider enabling GitHub private vulnerability reporting as an additional
route.

In a private report, include affected versions/commits, impact, reproduction
steps or a proof of concept, and any suggested mitigation. We will assess the
report, coordinate a fix and advisory where appropriate, and discuss a
disclosure date with the reporter. Please allow time for remediation before
public disclosure. Do not test against systems or data you do not own or have
permission to test.
