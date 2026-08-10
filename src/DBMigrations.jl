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
    success BOOLEAN NOT NULL,
    CONSTRAINT $(MIGRATIONS_TABLE)_pk PRIMARY KEY (installed_rank)
);
"""

# render a value as a SQL literal, escaping embedded single quotes
sqlliteral(s::AbstractString) = string('\'', replace(s, '\'' => "''"), '\'')
sqlliteral(::Missing) = "NULL"

function insertmigration!(conn, m, rank, etime)
    DBInterface.execute(conn, "INSERT INTO $MIGRATIONS_TABLE (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES ($rank, $(sqlliteral(m.version)), $(sqlliteral(m.description)), $(sqlliteral(m.type)), $(sqlliteral(m.script)), $(m.checksum), $(sqlliteral(m.installed_by)), $(max(0, etime)), true)")
end

struct ChecksumMismatch <: Exception
    filename::String
    checksum::Int
    applied_checksum::Int
end

Base.showerror(io::IO, e::ChecksumMismatch) = print(io, "Migration file $(e.filename) has changed since it was applied to the database. Expected checksum $(e.applied_checksum), got $(e.checksum)")

struct Migration
    installed_rank::Int
    version::Union{String, Missing}
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

Migration(rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success) = Migration(rank, version, description, type, script, coalesce(checksum, 0), installed_by, installed_on, execution_time, success, "")

const MIGRATION_FILE_REGEX = r"^V(\d+)__(.+)\.sql$"

# filename matches MIGRATION_FILE_REGEX
function Migration(filename::String)
    statements = read(filename, String)
    # strip a UTF-8 BOM before checksumming/executing, matching Flyway
    startswith(statements, '\ufeff') && (statements = statements[(1 + ncodeunits('\ufeff')):end])
    m = match(MIGRATION_FILE_REGEX, basename(filename))
    m === nothing && throw(ArgumentError("invalid migration filename: `$(basename(filename))`; must match `V<version>__<description>.sql`"))
    rank = parse(Int, m.captures[1])
    version = string(rank)
    description = String(m.captures[2])
    # split on \r\n, \r, or \n like Java's BufferedReader.readLine so checksums are
    # line-ending independent, matching Flyway
    # calculated according to https://github.com/zaunerc/flyway-checksum-tool/blob/master/src/main/java/net/nllk/flywaychecksumtool/LoadableResource.java
    lines = split(statements, r"\r\n|\r|\n")
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

struct OutOfOrderMigrationError <: Exception
    migrations::Vector{String}
    maxapplied::Int
end

Base.showerror(io::IO, e::OutOfOrderMigrationError) = print(io, "Out-of-order migrations detected: $(e.migrations) have versions lower than the highest already-applied migration version ($(e.maxapplied)). Pass `allowoutoforder=true` to apply them anyway")

struct FailedMigrationError <: Exception
    script::String
end

Base.showerror(io::IO, e::FailedMigrationError) = print(io, "Migration $(e.script) previously failed (success=false in $MIGRATIONS_TABLE). Manually repair the database and remove the failed row before re-running migrations")

# normalize integer-like version strings so e.g. '05' and '5' compare equal
versionkey(v::AbstractString) = (p = tryparse(Int, v); p === nothing ? String(v) : string(p))

# skip a quoted region starting at `i` (opening quote char `q`), where a doubled
# quote is an escape; returns the index just past the closing quote
function skipquoted(sql, n, i, q)
    j = nextind(sql, i)
    while j <= n
        if sql[j] == q
            k = nextind(sql, j)
            (k <= n && sql[k] == q) || return k
            j = nextind(sql, k)
        else
            j = nextind(sql, j)
        end
    end
    return j
end

# skip a `/* */` block comment starting at `i`, honoring nesting (Postgres);
# returns the index just past the closing `*/`
function skipblockcomment(sql, n, i)
    j = nextind(sql, nextind(sql, i))
    depth = 1
    while j <= n && depth > 0
        c = sql[j]
        k = nextind(sql, j)
        if c == '*' && k <= n && sql[k] == '/'
            depth -= 1
            j = nextind(sql, k)
        elseif c == '/' && k <= n && sql[k] == '*'
            depth += 1
            j = nextind(sql, k)
        else
            j = k
        end
    end
    return j
end

"""
    DBMigrations.splitsqlstatements(sql::AbstractString)

Split `sql` into individual statements on semicolons, ignoring semicolons that appear
inside single-quoted strings, double-quoted or backtick-quoted identifiers, line (`--`)
and block (`/* */`, nesting allowed) comments, and Postgres dollar-quoted (`\$tag\$`)
blocks. Chunks containing only whitespace/comments are dropped.

Quotes are escaped by doubling (`''`), per the SQL standard; non-standard
backslash-escaped quotes (e.g. MySQL's default `\\'`) are not recognized — use `''` or
`splitstatements=false` for such files.
"""
function splitsqlstatements(sql::AbstractString)
    statements = String[]
    n = lastindex(sql)
    stmtstart = firstindex(sql)
    hascontent = false
    i = firstindex(sql)
    while i <= n
        c = sql[i]
        if c == ';'
            hascontent && push!(statements, strip(SubString(sql, stmtstart, prevind(sql, i))))
            stmtstart = i = nextind(sql, i)
            hascontent = false
        elseif c == '\'' || c == '"' || c == '`'
            hascontent = true
            i = skipquoted(sql, n, i, c)
        elseif c == '-' && (k = nextind(sql, i); k <= n && sql[k] == '-')
            i = something(findnext(==('\n'), sql, k), n + 1)
        elseif c == '/' && (k = nextind(sql, i); k <= n && sql[k] == '*')
            i = skipblockcomment(sql, n, i)
        elseif c == '$' && (m = match(r"^\$[A-Za-z_][A-Za-z_0-9]*\$|^\$\$", SubString(sql, i)); m !== nothing)
            hascontent = true
            closing = findnext(m.match, sql, i + ncodeunits(m.match))
            i = closing === nothing ? n + 1 : nextind(sql, last(closing))
        else
            hascontent |= !isspace(c)
            i = nextind(sql, i)
        end
    end
    hascontent && push!(statements, strip(SubString(sql, stmtstart, n)))
    return statements
end

function getmigrations(conn)
    # select columns explicitly: the Migration constructor is positional, so we can't
    # depend on the physical column order of a pre-existing (e.g. Flyway-created) table
    results = DBInterface.execute(conn, "SELECT installed_rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success FROM $MIGRATIONS_TABLE ORDER BY installed_rank")
    return [Migration(row...) for row in results]
end

"""
    DBMigrations.runmigrations(conn::DBInterface.Connection, dir::String; silent=false, splitstatements=true, allowoutoforder=false)

Using an established database connection `conn` (which should have the appropriate schema already
selected), search the directory `dir` for migration files and apply them to the database. Migration
files should be named like `V1__baseline.sql`, `V2__latlong.sql`, etc. where they _must_ start with
a capital `V` followed by a number, followed by two underscores, followed by a description of the
migration. The number must be unique across all migrations. The description can be anything, but
should be descriptive of the migration. The file extension currently must be `.sql`.

Migration files found in `dir` will be checked against a special `$MIGRATIONS_TABLE` table that
the DBMigrations.jl package manages in the database connection for tracking which migrations have
already been applied. History rows are matched to local files by version; the `installed_rank`
column records application order (Flyway's semantics for the column, so a history table
previously managed by Flyway can be picked up). If a migration file is found in `dir` that has not been
applied to the database. If a migration file is found in `dir` that has already been applied, it
will be skipped. If a migration file is found in `dir` that has been applied but has changed since
it was applied, an error will be thrown (migrations should be immutable once applied).

Migration files may contain multiple SQL statements, separated by semicolons. Each statement will
be executed in order. If any statement fails, the entire migration will be rolled back and an error
will be thrown. If a migration file contains a syntax error, the migration will be rolled back and
an error will be thrown. Semicolons inside single-quoted strings, quoted identifiers, `--` and
`/* */` comments, and Postgres dollar-quoted blocks are not treated as statement separators.

Supported keyword arguments:
  * `silent::Bool=false`: suppress informational logging while applying migrations
  * `splitstatements::Bool=true`: split each migration file on semicolons and execute each
    statement separately; pass `false` to pass each file's contents to the database driver
    as-is, e.g. for constructs the splitter doesn't understand such as SQLite `CREATE TRIGGER`
    bodies with embedded semicolons (note some drivers, e.g. SQLite, only execute the first
    statement of a multi-statement string, so such constructs should live in their own file)
  * `allowoutoforder::Bool=false`: by default, an `OutOfOrderMigrationError` is thrown if a
    pending migration has a version lower than the highest already-applied version (e.g. `V2`
    shows up after `V3` was already applied); pass `true` to apply such migrations anyway

Duplicate migration versions (in the local directory or in the database history table) throw a
`DuplicateMigrationError`. A migration recorded as failed in the history table (possible when the
table is shared with other tools like Flyway; this package rolls failed migrations back without
recording them) throws a `FailedMigrationError` and requires manual repair.
"""
function runmigrations(conn, dir::String; silent::Bool=false, splitstatements::Bool=true, allowoutoforder::Bool=false)
    isdir(dir) || throw(ArgumentError("migrations directory does not exist: `$dir`"))
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
    allfiles = filter(isfile, readdir(dir; join=true))
    files = filter(x -> match(MIGRATION_FILE_REGEX, basename(x)) !== nothing, allfiles)
    if !silent
        # .sql files that don't match the migration naming pattern are easy to mistake
        # for migrations, so call them out instead of silently ignoring them
        skipped = [basename(x) for x in allfiles if endswith(lowercase(x), ".sql") && match(MIGRATION_FILE_REGEX, basename(x)) === nothing]
        isempty(skipped) || @warn "Ignoring .sql files that don't match the `V<version>__<description>.sql` migration naming pattern: $skipped"
    end
    migrations = sort!(map(Migration, files), by=x->x.installed_rank)
    # check that all local migration versions are unique before comparing against the db
    if !allunique(m.installed_rank for m in migrations)
        counts = Dict{Int, Int}()
        for m in migrations
            counts[m.installed_rank] = get(counts, m.installed_rank, 0) + 1
        end
        throw(DuplicateMigrationError([m.script for m in migrations if counts[m.installed_rank] > 1]))
    end
    # index applied migrations by *version*, not installed_rank: Flyway's
    # installed_rank is an application-order counter, so on a Flyway-written history
    # it need not equal the version number (rank-based matching silently re-ran
    # already-applied migrations there). Rows with no version (e.g. Flyway repeatable
    # migrations) can't correspond to a local versioned file and are ignored.
    # Duplicate versions in the db mean the history table is corrupt.
    dbbyversion = Dict{String, Migration}()
    for dbm in dbmigrations
        dbm.version === missing && continue
        k = versionkey(dbm.version)
        haskey(dbbyversion, k) && throw(DuplicateMigrationError([x.script for x in dbmigrations if x.version !== missing && versionkey(x.version) == k]))
        dbbyversion[k] = dbm
    end
    # filter out migrations that have already been applied
    migrations_to_run = Migration[]
    for m in migrations
        dbm = get(dbbyversion, versionkey(m.version), nothing)
        if dbm === nothing
            # not applied yet, so it needs to be run
            push!(migrations_to_run, m)
        elseif !dbm.success
            throw(FailedMigrationError(m.script))
        elseif dbm.checksum != 0 && dbm.checksum != m.checksum
            throw(ChecksumMismatch(m.script, m.checksum, dbm.checksum))
        end
    end
    # by default, refuse to apply migrations with versions lower than the highest
    # already-applied version; the old scan-based matching silently *re-ran*
    # already-applied migrations in this situation
    appliedversions = Int[]
    for dbm in dbmigrations
        dbm.version === missing && continue
        p = tryparse(Int, dbm.version)
        p === nothing || push!(appliedversions, p)
    end
    if !allowoutoforder && !isempty(appliedversions)
        maxapplied = maximum(appliedversions)
        outoforder = [m.script for m in migrations_to_run if m.installed_rank < maxapplied]
        isempty(outoforder) || throw(OutOfOrderMigrationError(outoforder, maxapplied))
    end
    # run migrations; installed_rank records application order (max existing rank + 1
    # onwards), matching Flyway's semantics for the column
    nextrank = (isempty(dbmigrations) ? 0 : maximum(dbm.installed_rank for dbm in dbmigrations)) + 1
    for m in migrations_to_run
        DBInterface.transaction(conn) do
            start = time()
            silent || @info "Applying migrations from file: $(m.script)"
            if splitstatements
                for statement in splitsqlstatements(m.statements)
                    silent || @info "Applying migration statement:\n$statement"
                    DBInterface.execute(conn, statement)
                end
            else
                silent || @info "Applying migration statement:\n$(m.statements)"
                DBInterface.execute(conn, m.statements)
            end
            insertmigration!(conn, m, nextrank, round(Int, (time() - start) * 1000))
            silent || @info "Applied migrations from file: $(m.script)"
        end
        nextrank += 1
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
