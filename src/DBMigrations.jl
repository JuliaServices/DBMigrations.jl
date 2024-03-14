module DBMigrations

using DBInterface, CRC32, Dates

import Base: ==

const MIGRATIONS_TABLE = "flyway_schema_history"

const MIGRATIONS_TABLE_SCHEMA = """
CREATE TABLE $MIGRATIONS_TABLE (
    installed_rank INTEGER NOT NULL,
    version VARCHAR(50),
    description VARCHAR(200) NOT NULL,
    type VARCHAR(20) NOT NULL,
    script VARCHAR(1000) NOT NULL,
    checksum INTEGER,
    installed_by VARCHAR(100) NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    execution_time INTEGER NOT NULL,
    success BOOLEAN NOT NULL
);
"""

function insertmigration!(conn, m, etime)
    DBInterface.execute(conn, "INSERT INTO $MIGRATIONS_TABLE (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES ($(m.installed_rank), '$(m.version)', '$(m.description)', '$(m.type)', '$(m.script)', $(m.checksum), '$(m.installed_by)', $(max(1, etime)), true)")
end

struct ChecksumMismatch <: Exception
    filename::String
    checksum::Int
    applied_checksum::Int
end

Base.showerror(io::IO, e::ChecksumMismatch) = print(io, "Migration file $(e.filename) has changed since it was applied to the database. Expected checksum $(e.applied_checksum), got $(e.checksum)")

struct Migration
    installed_rank::Int
    version::String
    description::String
    type::String
    script::String
    checksum::Int
    installed_by::String
    installed_on::Union{String, DateTime}
    execution_time::Int
    success::Bool
    # non-db-stored fields
    statements::String
end

Migration(rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success) = Migration(rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success, "")

# filename matches r"V\d+__\w+\.sql"
function Migration(filename::String)
    statements = read(filename, String)
    rank = parse(Int, match(r"V(\d+)", filename).captures[1])
    version = string(rank)
    description = match(r"V\d+__(\w+).sql", filename).captures[1]
    lines = split(statements, '\n')
    # calculated according to https://github.com/zaunerc/flyway-checksum-tool/blob/master/src/main/java/net/nllk/flywaychecksumtool/LoadableResource.java
    checksum = crc32(lines[1])
    for line in @view lines[2:end]
        checksum = crc32(line, checksum)
    end
    checksum = Base.bitcast(Int32, checksum)
    return Migration(rank, version, description, "SQL", basename(filename), checksum, "DBMigrations.jl", "", 0, false, statements)
end

==(m1::Migration, m2::Migration) = m1.installed_rank == m2.installed_rank && m1.description == m2.description && m1.script == m2.script && m1.checksum == m2.checksum

struct DuplicateMigrationError <: Exception
    migrations::Vector{String}
end

Base.showerror(io::IO, e::DuplicateMigrationError) = print(io, "Duplicate migration version numbers detected: $(e.migrations)")

prefix(filename) = match(r"V\d+", filename).match

function getmigrations(conn)
    results = DBInterface.execute(conn, "SELECT * FROM $MIGRATIONS_TABLE")
    return [Migration(row...) for row in results]
end

"""
    DBMigrations.runmigrations(conn::DBInterface.Connection, dir::String)

Using an established database connection `conn` (which should have the appropriate schema already
selected), search the directory `dir` for migration files and apply them to the database. Migration
files should be named like `V1__baseline.sql`, `V2__latlong.sql`, etc. where they _must_ start with
a capital `V` followed by a number, followed by two underscores, followed by a description of the
migration. The number must be unique across all migrations. The description can be anything, but
should be descriptive of the migration. The file extension currently must be `.sql`.

Migration files found in `dir` will be checked against a special `$MIGRATIONS_TABLE` table that
the DBMigrations.jl package manages in the database connection for tracking which migrations have
already been applied. If a migration file is found in `dir` that has not been applied, it will be
applied to the database. If a migration file is found in `dir` that has already been applied, it
will be skipped. If a migration file is found in `dir` that has been applied but has changed since
it was applied, an error will be thrown (migrations should be immutable once applied).

Migration files may contain multiple SQL statements, separated by semicolons. Each statement will
be executed in order. If any statement fails, the entire migration will be rolled back and an error
will be thrown. If a migration file contains a syntax error, the migration will be rolled back and
an error will be thrown.
"""
function runmigrations(conn, dir::String; silent::Bool=false, splitstatements::Bool=true)
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
        while db_index <= length(dbmigrations) && prefix(dbmigrations[db_index].script) != prefix(m.script)
            db_index += 1
        end
        if db_index > length(dbmigrations)
            # we checked all applied migrations and didn't find `m`, so it needs to be run
            push!(migrations_to_run, m)
        elseif dbmigrations[db_index].checksum != m.checksum
            throw(ChecksumMismatch(m.script, m.checksum, dbmigrations[db_index].checksum))
        else
            # we found a matching migration, so we don't need to run it
            db_index += 1
        end
    end
    # check that resulting migrations are unique
    allunique(prefix(m.script) for m in migrations_to_run) || throw(DuplicateMigrationError([m.script for m in migrations_to_run]))
    # run migrations
    for m in migrations_to_run
        DBInterface.transaction(conn) do
            start = time()
            silent || @info "Applying migrations from file: $(m.script)"
            if splitstatements
                for statement in split(m.statements, ';')
                    statement = strip(statement)
                    if !isempty(statement)
                        silent || @info "Applying migration statement:\n$statement"
                        DBInterface.execute(conn, statement)
                    end
                end
            else
                silent || @info "Applying migration statement:\n$(m.statements)"
                DBInterface.execute(conn, m.statements)
            end
            insertmigration!(conn, m, round(Int, time() - start))
            silent || @info "Applied migrations from file: $(m.script)"
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
