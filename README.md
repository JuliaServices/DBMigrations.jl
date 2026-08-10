# DBMigrations.jl

***A Julia package for managing database migrations***

GitHub Actions : [![Build Status](https://github.com/JuliaServices/DBMigrations.jl/workflows/CI/badge.svg)](https://github.com/JuliaServices/DBMigrations.jl/actions?query=workflow%3ACI+branch%3Amain)

[![codecov.io](http://codecov.io/github/JuliaServices/DBMigrations.jl/coverage.svg?branch=main)](http://codecov.io/github/JuliaServices/DBMigrations.jl?branch=main)

## Usage

DBMigrations.jl tries to be simple, transparent, and flexible. It relies on minimal required structure to work, while allowing for more complex setups.

It aims to be compatible with all database packages that support the interfaces in [DBInterface.jl](https://github.com/JuliaDatabases/DBInterface.jl).
Currently that includes SQLite.jl, MySQL.jl, LibPQ.jl, and ODBC.jl.

The primary interface is calling `DBMigrations.runmigrations(conn::DBInterface.Connection, dir::String)`.

    DBMigrations.runmigrations(conn::DBInterface.Connection, dir::String)

Using an established database connection `conn` (which should have the appropriate schema already
selected), search the directory `dir` for migration files and apply them to the database. Migration
files should be named like `V1__baseline.sql`, `V2__latlong.sql`, etc. where they _must_ start with
a capital `V` followed by a number, followed by two underscores, followed by a description of the
migration. The number must be unique across all migrations. The description can be anything, but
should be descriptive of the migration. The file extension currently must be `.sql`.

Migration files found in `dir` will be checked against a special `flyway_schema_history` table that
the DBMigrations.jl package manages in the database connection for tracking which migrations have
already been applied (the table name and layout are compatible with [Flyway](https://flywaydb.org/),
including its line-ending-independent CRC32 checksums, so a history table previously managed by
Flyway can be picked up by DBMigrations.jl). If a migration file is found in `dir` that has not been
applied, it will be applied to the database. If a migration file is found in `dir` that has already
been applied, it will be skipped. If a migration file is found in `dir` that has been applied but
has changed since it was applied, a `ChecksumMismatch` will be thrown (migrations should be
immutable once applied).

Migration files may contain multiple SQL statements, separated by semicolons. Each statement will
be executed in order. Semicolons inside single-quoted strings, quoted identifiers, `--`/`/* */`
comments, and Postgres dollar-quoted blocks are handled correctly. Each migration file is applied
in a transaction: if any statement fails, the migration is rolled back and the error rethrown.

Supported keyword arguments to `runmigrations`:

  * `silent::Bool=false`: suppress informational logging while applying migrations
  * `splitstatements::Bool=true`: split each migration file on semicolons and execute each
    statement separately; pass `false` to send each file's contents to the database driver as-is
    (useful for constructs the splitter doesn't understand, like SQLite `CREATE TRIGGER` bodies
    with embedded semicolons — note some drivers, e.g. SQLite, only execute the first statement
    of a multi-statement string, so such constructs should live in their own file)
  * `allowoutoforder::Bool=false`: by default an `OutOfOrderMigrationError` is thrown if a pending
    migration has a version lower than the highest already-applied version (e.g. `V2` shows up
    after `V3` was already applied); pass `true` to apply such migrations anyway

Other error conditions: duplicate migration versions (in the directory or the history table) throw
a `DuplicateMigrationError`; a migration recorded as failed in the history table (possible when
sharing the table with Flyway) throws a `FailedMigrationError` and requires manual repair.

`DBMigrations.clean!(conn; confirm=true)` drops and recreates the history table, forgetting all
record of applied migrations (it does *not* undo the migrations themselves).

## Limitations

  * No locking is performed: if multiple processes run migrations against the same database
    concurrently, they may race. Coordinate deployments externally.
  * Transactional rollback of failed migrations depends on the database supporting transactional
    DDL (SQLite and PostgreSQL do; MySQL auto-commits DDL statements, so a failed multi-statement
    migration may leave earlier DDL statements applied).
  * Only versioned migrations are supported (no Flyway-style repeatable `R__` migrations or undo
    migrations).

## Example

```julia
using DBMigrations, SQLite

db = SQLite.DB("test.db")

DBMigrations.runmigrations(db, "migrations")
```
