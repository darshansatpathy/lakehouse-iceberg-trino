# ADR-0001: An Iceberg JDBC catalog in PostgreSQL

**Status:** accepted

## Context

Iceberg tables need a catalog that maps a table name to its current metadata file and commits
changes atomically. Options: a Hive metastore, a JDBC catalog, a REST catalog (Apache Polaris,
Lakekeeper, Nessie), or a cloud catalog.

## Decision

Use the JDBC catalog on PostgreSQL. Trino and pyiceberg both implement it against the same two
tables, so one catalog serves both engines with no extra service.

## Consequences

- The commit is a conditional UPDATE in PostgreSQL: simple, transactional and easy to inspect.
- There is no catalog-level access control and no credential vending: every engine holds storage
  credentials. For a shared platform, a REST catalog is the next step, and the move does not
  touch the data files.
