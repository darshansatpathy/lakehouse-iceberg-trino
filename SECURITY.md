# Security policy

## Reporting a vulnerability

Please do not open a public issue for security problems. Use GitHub's
[private vulnerability reporting](https://github.com/darshansatpathy/lakehouse-iceberg-trino/security/advisories/new)
for this repository. You should get an acknowledgement within three working days.

## Scope

This is a local and CI stack. Its defaults are not suitable for anything else:

- The storage keys, the catalog password and Superset's admin account and secret key in
  `docker-compose.yml` are fixed development values. Replace them, and pass them from a secrets
  manager, before running the stack anywhere shared.
- Trino runs without authentication and over plain HTTP. Configure TLS and an authenticator
  before exposing port 8080.
- The JDBC catalog has no access control. Anyone with its credentials and the storage keys can
  change any table. Use a REST catalog with credential vending for shared deployments.
