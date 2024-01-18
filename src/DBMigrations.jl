module DBMigrations

using DBInterface, SHA

import Base: ==

const MIGRATIONS_TABLE = "__migrations__"

const MIGRATIONS_TABLE_SCHEMA = """
CREATE TABLE $MIGRATIONS_TABLE (
    filename VARCHAR(255) NOT NULL,
    sha256 VARCHAR(64) NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (filename)
);
"""

struct Sha256Mismatch <: Exception
    filename::String
    sha256::String
    applied_sha256::String
end

Base.showerror(io::IO, e::Sha256Mismatch) = print(io, "Migration file $(e.filename) has changed since it was applied to the database. Expected sha256 $(e.applied_sha256), got $(e.sha256)")

struct Migration
    filename::String
    sha256::String
    applied_at::String
    # non-db-stored fields
    statements::Vector{SubString{String}}
end

function Migration(filename::String)
    contents = read(filename, String)
    no_comments = join(filter!(x -> !startswith(x, "--"), map(strip, split(contents, '\n'))), '\n')
    statements = filter!(!isempty, [strip(x) for x in split(no_comments, ';'; keepempty=false)])
    return Migration(basename(filename), bytes2hex(sha256(contents)), "", statements)
end

Migration(filename::String, sha256::String, applied_at) = Migration(filename, sha256, string(applied_at), SubString{String}[])
==(m1::Migration, m2::Migration) = m1.filename == m2.filename && m1.sha256 == m2.sha256

struct DuplicateMigrationError <: Exception
    migrations::Vector{String}
end

Base.showerror(io::IO, e::DuplicateMigrationError) = print(io, "Duplicate migration version numbers detected: $(e.migrations)")

prefix(filename) = match(r"V\d+", filename).match

function getmigrations(conn)
    results = DBInterface.execute(conn, "SELECT * FROM $MIGRATIONS_TABLE")
    return [Migration(row[1], row[2], row[3]) for row in results]
end

"""
    Migrations.runmigrations(conn::DBInterface.Connection, dir::String)

Using an established database connection `conn` (which should have the appropriate schema already
selected), search the directory `dir` for migration files and apply them to the database. Migration
files should be named like `V1__baseline.sql`, `V2__latlong.sql`, etc. where they _must_ start with
a capital `V` followed by a number, followed by two underscores, followed by a description of the
migration. The number must be unique across all migrations. The description can be anything, but
should be descriptive of the migration. The file extension currently must be `.sql`.

Migration files found in `dir` will be checked against a special `__migrations__` table that
the Migrations.jl package manages in the database connection for tracking which migrations have
already been applied. If a migration file is found in `dir` that has not been applied, it will be
applied to the database. If a migration file is found in `dir` that has already been applied, it
will be skipped. If a migration file is found in `dir` that has been applied but has changed since
it was applied, an error will be thrown (migrations should be immutable once applied).

Migration files may contain multiple SQL statements, separated by semicolons. Each statement will
be executed in order. If any statement fails, the entire migration will be rolled back and an error
will be thrown. If a migration file contains a syntax error, the migration will be rolled back and
an error will be thrown.
"""
function runmigrations(conn, dir::String; silent::Bool=false)
    # first fetch migrations already applied from the database
    local dbmigrations
    try
        dbmigrations = getmigrations(conn)
    catch e
        silent || @warn "Unable to query migrations table, attempting to create:" exception=e
        try
            DBInterface.execute(conn, MIGRATIONS_TABLE_SCHEMA)
            dbmigrations = getmigrations(conn)
        catch e
            @error "Unable to create migrations table" exception=e
            rethrow()
        end
    end
    files = filter!(x -> match(r"V\d+__\w+\.sql", x) !== nothing, readdir(dir; join=true))
    migrations = map(Migration, files)
    # filter out migrations that have already been applied
    migrations_to_run = Migration[]
    db_index = 1
    for m in migrations
        # find matching db migration
        while db_index <= length(dbmigrations) && prefix(dbmigrations[db_index].filename) != prefix(m.filename)
            db_index += 1
        end
        if db_index > length(dbmigrations)
            # we checked all applied migrations and didn't find `m`, so it needs to be run
            push!(migrations_to_run, m)
        elseif dbmigrations[db_index].sha256 != m.sha256
            throw(Sha256Mismatch(m.filename, m.sha256, dbmigrations[db_index].sha256))
        else
            # we found a matching migration, so we don't need to run it
            db_index += 1
        end
    end
    # check that resulting migrations are unique
    allunique(prefix(m.filename) for m in migrations_to_run) || throw(DuplicateMigrationError([m.filename for m in migrations_to_run]))
    # run migrations
    for m in migrations_to_run
        DBInterface.transaction(conn) do
            silent || @info "Applying migration $(m.filename)"
            for statement in m.statements
                silent || @info "Applying migration statement:\n$(statement)"
                DBInterface.execute(conn, statement)
            end
            DBInterface.execute(conn, "INSERT INTO $MIGRATIONS_TABLE (filename, sha256) VALUES ('$(m.filename)', '$(m.sha256)')")
            silent || @info "Applied migration $(m.filename)"
        end
    end
    return migrations_to_run
end

function clean!(conn::DBInterface.Connection; confirm::Bool=false)
    confirm || throw(ArgumentError("Are you sure you want to delete the record of all previously applied migrations? Database state may be in an inconsistent state for future migrations. Pass `confirm=true` to proceed"))
    DBInterface.execute(conn, "DROP TABLE $MIGRATIONS_TABLE")
    DBInterface.execute(conn, MIGRATIONS_TABLE_SCHEMA)
    return
end

end
