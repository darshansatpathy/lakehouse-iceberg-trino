# ADR-0005: Any S3-compatible store; MinIO from a community build

**Status:** accepted

## Context

MinIO stopped publishing community container images and binaries in 2025, and the
`minio/minio` repository is no longer available on Docker Hub.

## Decision

Depend only on the S3 API. The Compose stack runs `pgsty/minio`, a community-maintained build of
the MinIO server, pinned to a release and updated by Dependabot. The Trino catalog and the
Python settings take the endpoint from the environment.

## Consequences

- Switching to AWS S3, Ceph, SeaweedFS or another MinIO distribution is a configuration change.
- The image comes from a third party; it is pinned, and the stack is verified end to end on each
  change.
