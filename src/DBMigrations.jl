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

# These packages cannot be hard dependencies, so identify the drivers whose
# DBInterface placeholder and transaction behavior differs from the other supported
# drivers. LibPQ.DBConnection is the public DBInterface connection wrapper.
function islibpqconnection(conn)
    T = typeof(conn)
    return nameof(T) === :DBConnection && nameof(parentmodule(T)) === :LibPQ
end

# Postgres.jl implements the full DBInterface prepared-statement contract (so it
# takes the generic prepare/execute/close! path), but like LibPQ it speaks native
# PostgreSQL `$1` placeholders rather than `?`.
function ispostgresconnection(conn)
    T = typeof(conn)
    return nameof(T) === :Connection && nameof(parentmodule(T)) === :Postgres
end

usesdollarmarkers(conn) = islibpqconnection(conn) || ispostgresconnection(conn)

function ismysqlconnection(conn)
    T = typeof(conn)
    return nameof(T) === :Connection && nameof(parentmodule(T)) === :MySQL
end

function issqliteconnection(conn)
    T = typeof(conn)
    return nameof(T) === :DB && nameof(parentmodule(T)) === :SQLite
end

function closecursor(cursor)
    if applicable(DBInterface.close!, cursor)
        DBInterface.close!(cursor)
    elseif applicable(close, cursor)
        close(cursor)
    end
    return
end

function closelibpqresult(conn, sql, params=nothing)
    result = params === nothing ? DBInterface.execute(conn, sql) : DBInterface.execute(conn, sql, params)
    try
        return nothing
    finally
        closecursor(result)
    end
end

function executewithcursor(f, conn, sql, params=nothing)
    if islibpqconnection(conn)
        result = params === nothing ? DBInterface.execute(conn, sql) : DBInterface.execute(conn, sql, params)
        try
            return f(result)
        finally
            closecursor(result)
        end
    end
    actualparams = params === nothing ? () : params
    statement = DBInterface.prepare(conn, sql)
    try
        cursor = DBInterface.execute(statement, actualparams)
        try
            return f(cursor)
        finally
            # ODBC.Cursor has no close! method. Closing its statement below releases
            # the handle after the callback has consumed the rows.
            closecursor(cursor)
        end
    finally
        closecursor(statement)
    end
end

function executecommand(conn, sql)
    if issqliteconnection(conn)
        # SQLite's direct DBInterface fallback owns a prepared statement that remains
        # registered until GC. The callback form closes it deterministically.
        return executewithcursor(_ -> nothing, conn, sql)
    end

    # Preserve direct execution for arbitrary migration SQL on MySQL, LibPQ, ODBC,
    # and other drivers. Close the returned cursor when that driver exposes a method.
    cursor = DBInterface.execute(conn, sql)
    try
        return nothing
    finally
        closecursor(cursor)
    end
end

function executebound(conn, sql, params)
    # LibPQ uses $1 placeholders and its Statement is not a DBInterface.Statement;
    # executewithcursor uses direct parameter execution there. SQLite, MySQL, and
    # ODBC use `?` and the callback form closes their one-shot prepared statements.
    executewithcursor(_ -> nothing, conn, sql, params)
end

function insertmigration!(conn, m, rank, etime)
    markers = usesdollarmarkers(conn) ? join(("\$$i" for i = 1:9), ", ") : join(fill("?", 9), ", ")
    sql = "INSERT INTO $MIGRATIONS_TABLE (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES ($markers)"
    # MySQL.jl does not bind Bool correctly on all supported releases. Int8(1)
    # round-trips as true through SQLite/MySQL/Postgres/ODBC boolean columns.
    params = (rank, m.version, m.description, m.type, m.script, m.checksum, m.installed_by, max(0, etime), Int8(1))
    executebound(conn, sql, params)
end

function migrationtransaction(f, conn)
    islibpqconnection(conn) || return DBInterface.transaction(f, conn)

    # DBInterface 2.7 prepares transaction-control statements. LibPQ.Statement does
    # not implement DBInterface.Statement/close!, so that generic path fails before
    # BEGIN. Direct execution also works on DBInterface 2.6, the Julia 1.6 minimum.
    closelibpqresult(conn, "BEGIN;")
    try
        result = f()
        closelibpqresult(conn, "COMMIT;")
        return result
    catch transaction_error
        transaction_backtrace = catch_backtrace()
        try
            closelibpqresult(conn, "ROLLBACK;")
        catch rollback_error
            rollback_backtrace = catch_backtrace()
            throw(CompositeException([
                CapturedException(transaction_error, transaction_backtrace),
                CapturedException(rollback_error, rollback_backtrace),
            ]))
        end
        rethrow()
    end
end

struct ChecksumMismatch <: Exception
    filename::String
    checksum::Int
    applied_checksum::Int
end

Base.showerror(io::IO, e::ChecksumMismatch) = print(io, "Migration file $(e.filename) has changed since it was applied to the database. Expected checksum $(e.applied_checksum), got $(e.checksum)")

struct DescriptionMismatch <: Exception
    filename::String
    description::String
    applied_description::String
end

Base.showerror(io::IO, e::DescriptionMismatch) = print(io, "Migration file $(e.filename) has a different description from the migration applied to the database. Expected description $(repr(e.applied_description)), got $(repr(e.description))")

struct TypeMismatch <: Exception
    filename::String
    type::String
    applied_type::String
end

Base.showerror(io::IO, e::TypeMismatch) = print(io, "Migration file $(e.filename) has a different type from the migration applied to the database. Expected type $(repr(e.applied_type)), got $(repr(e.type))")

struct Migration
    installed_rank::Int
    version::Union{String, Missing}
    description::String
    type::String
    script::String
    checksum::Union{Int, Missing}
    installed_by::String
    installed_on::Union{String, DateTime}
    execution_time::Int
    success::Bool
    # non-db-stored fields
    statements::String
end

Migration(rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success) = Migration(rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success, "")

const MIGRATION_FILE_REGEX = r"^V(\d+)__(.+)\.sql$"

function abbreviatedescription(description::String)
    length(description) <= 200 && return description
    return first(description, 197) * "..."
end

# filename matches MIGRATION_FILE_REGEX
function Migration(filename::String)
    statements = read(filename, String)
    # strip a UTF-8 BOM before checksumming/executing, matching Flyway
    startswith(statements, '\ufeff') && (statements = statements[(1 + ncodeunits('\ufeff')):end])
    m = match(MIGRATION_FILE_REGEX, basename(filename))
    m === nothing && throw(ArgumentError("invalid migration filename: `$(basename(filename))`; must match `V<version>__<description>.sql`"))
    rank = parse(Int, m.captures[1])
    version = string(rank)
    # Flyway stores descriptions with underscores replaced by spaces.
    description = abbreviatedescription(replace(String(m.captures[2]), '_' => ' '))
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

==(m1::Migration, m2::Migration) = m1.installed_rank == m2.installed_rank && isequal(m1.version, m2.version) && m1.description == m2.description && m1.type == m2.type && m1.script == m2.script && isequal(m1.checksum, m2.checksum)

Base.hash(m::Migration, h::UInt) = hash((m.installed_rank, m.version, m.description, m.type, m.script, m.checksum), h)

struct DuplicateMigrationError <: Exception
    migrations::Vector{String}
end

Base.showerror(io::IO, e::DuplicateMigrationError) = print(io, "Duplicate migration version numbers detected: $(e.migrations)")

struct DuplicateInstalledRankError <: Exception
    ranks::Vector{Int}
end

Base.showerror(io::IO, e::DuplicateInstalledRankError) = print(io, "Duplicate installed_rank values detected in $MIGRATIONS_TABLE: $(e.ranks)")

struct InvalidDeleteMarkerError <: Exception
    version::String
end

Base.showerror(io::IO, e::InvalidDeleteMarkerError) = print(io, "Flyway DELETE marker for version $(e.version) has no active migration row to delete")

struct OutOfOrderMigrationError <: Exception
    migrations::Vector{String}
    maxapplied::Union{Int, String}
end

Base.showerror(io::IO, e::OutOfOrderMigrationError) = print(io, "Out-of-order migrations detected: $(e.migrations) have versions lower than the highest already-applied migration version ($(e.maxapplied)). Pass `allowoutoforder=true` to apply them anyway")

struct FailedMigrationError <: Exception
    script::String
end

Base.showerror(io::IO, e::FailedMigrationError) = print(io, "Migration $(e.script) previously failed (success=false in $MIGRATIONS_TABLE). Manually repair the database and remove the failed row before re-running migrations")

# Flyway versions contain numeric components separated by dots (underscores are
# normalized to dots). Trailing zero components do not affect equality.
function flywayversionparts(v::AbstractString)
    parts = BigInt[]
    for part in split(replace(v, '_' => '.'), '.'; keepempty=true)
        parsed = tryparse(BigInt, part)
        parsed === nothing && return nothing
        push!(parts, parsed)
    end
    while length(parts) > 1 && last(parts) == 0
        pop!(parts)
    end
    return parts
end

function compareversions(a::Vector{BigInt}, b::Vector{BigInt})
    for i = 1:max(length(a), length(b))
        avalue = i <= length(a) ? a[i] : BigInt(0)
        bvalue = i <= length(b) ? b[i] : BigInt(0)
        avalue < bvalue && return -1
        avalue > bvalue && return 1
    end
    return 0
end

function versionkey(v::AbstractString)
    parts = flywayversionparts(v)
    return parts === nothing ? String(v) : join(parts, '.')
end

localversionparts(m::Migration) = BigInt[BigInt(m.installed_rank)]

function versiondisplay(parts, original)
    if length(parts) == 1 && typemin(Int) <= parts[1] <= typemax(Int)
        return Int(parts[1])
    end
    return String(original)
end

function activehistory(dbmigrations)
    active = trues(length(dbmigrations))
    for (i, dbm) in enumerate(dbmigrations)
        uppercase(dbm.type) == "DELETE" || continue
        active[i] = false
        dbm.version === missing && continue

        key = versionkey(dbm.version)
        target = nothing
        for j = (i - 1):-1:1
            active[j] || continue
            candidate = dbmigrations[j]
            candidate.version === missing && continue
            versionkey(candidate.version) == key || continue
            type = uppercase(candidate.type)
            (type == "BASELINE" || type == "SCHEMA" || type == "DELETE" || startswith(type, "UNDO")) && continue
            target = j
            break
        end
        target === nothing && throw(InvalidDeleteMarkerError(dbm.version))
        active[target] = false
    end
    return dbmigrations[active]
end

# DBMigrations versions before 2.2 stored the raw filename description. Accept that
# legacy spelling while writing Flyway's space-normalized spelling for new rows.
function legacydescription(m::Migration)
    match_ = match(MIGRATION_FILE_REGEX, m.script)
    return String(match_.captures[2])
end

# skip a quoted region starting at `i`, where a doubled closing character is an
# escape; returns the index just past the closing character
function skipquoted(sql, n, i, closing)
    j = nextind(sql, i)
    while j <= n
        if sql[j] == closing
            k = nextind(sql, j)
            (k <= n && sql[k] == closing) || return k
            j = nextind(sql, k)
        else
            j = nextind(sql, j)
        end
    end
    return j
end

function islinecommentstart(sql, n, i, mysqlcomments)
    c = sql[i]
    c == '#' && return mysqlcomments
    c == '-' || return false
    second = nextind(sql, i)
    second <= n && sql[second] == '-' || return false
    mysqlcomments || return true
    after = nextind(sql, second)
    return after > n || isspace(sql[after])
end

function statementtext(sql, start, stop, lonecommentcrs)
    relevantcrs = [i for i in lonecommentcrs if start <= i <= stop]
    isempty(relevantcrs) && return String(strip(SubString(sql, start, stop)))

    io = IOBuffer()
    position = start
    for cr in relevantcrs
        position < cr && write(io, SubString(sql, position, prevind(sql, cr)))
        write(io, '\n')
        position = nextind(sql, cr)
    end
    position <= stop && write(io, SubString(sql, position, stop))
    return String(strip(String(take!(io))))
end

# skip a `/* */` block comment starting at `i`, honoring nesting (Postgres);
# returns the index just past the closing `*/`
function skipblockcomment(sql, n, i, nestedcomments)
    j = nextind(sql, nextind(sql, i))
    depth = 1
    while j <= n && depth > 0
        c = sql[j]
        k = nextind(sql, j)
        if c == '*' && k <= n && sql[k] == '/'
            depth -= 1
            j = nextind(sql, k)
        elseif nestedcomments && c == '/' && k <= n && sql[k] == '*'
            depth += 1
            j = nextind(sql, k)
        else
            j = k
        end
    end
    return j
end

function candollarquote(sql, i)
    i == firstindex(sql) && return true
    previous = SubString(sql, prevind(sql, i), prevind(sql, i))
    # PostgreSQL permits dollar signs in unquoted identifiers. A dollar-quote
    # delimiter must therefore be separated from a preceding identifier.
    return !occursin(r"^[\p{L}\p{M}\p{Nd}_\$]$", previous)
end

"""
    DBMigrations.splitsqlstatements(sql::AbstractString)

Split `sql` into individual statements on semicolons, ignoring semicolons that appear
inside single-quoted strings, double-quoted, backtick-quoted, or bracket-quoted
identifiers, line (`--`) and block (`/* */`, nesting allowed) comments, and Postgres
dollar-quoted (`\$tag\$`) blocks. Chunks containing only whitespace/comments are dropped.

Quotes are escaped by doubling (`''`), per the SQL standard; non-standard
backslash-escaped quotes (e.g. MySQL's default `\\'`) are not recognized — use `''` or
`splitstatements=false` for such files.
"""
splitsqlstatements(sql::AbstractString) = splitsqlstatements(sql, false)

splitsqlstatements(sql::AbstractString, mysqlcomments::Bool) = splitsqlstatements(sql, mysqlcomments, !mysqlcomments)

function splitsqlstatements(sql::AbstractString, mysqlcomments::Bool, nestedcomments::Bool)
    statements = String[]
    n = lastindex(sql)
    stmtstart = firstindex(sql)
    hascontent = false
    lonecommentcrs = Int[]
    i = firstindex(sql)
    while i <= n
        c = sql[i]
        if c == ';'
            hascontent && push!(statements, statementtext(sql, stmtstart, prevind(sql, i), lonecommentcrs))
            stmtstart = i = nextind(sql, i)
            hascontent = false
            empty!(lonecommentcrs)
        elseif c == '\'' || c == '"' || c == '`'
            hascontent = true
            i = skipquoted(sql, n, i, c)
        elseif c == '['
            hascontent = true
            i = skipquoted(sql, n, i, ']')
        elseif islinecommentstart(sql, n, i, mysqlcomments)
            # a lone \r is a line ending too (consistent with the checksum algorithm)
            lineend = something(findnext(x -> x == '\n' || x == '\r', sql, nextind(sql, i)), n + 1)
            if lineend <= n && sql[lineend] == '\r'
                after = nextind(sql, lineend)
                (after > n || sql[after] != '\n') && push!(lonecommentcrs, lineend)
            end
            # Preserve line-form optimizer hints, but trim ordinary leading comments.
            second = c == '-' ? nextind(sql, i) : i
            after = nextind(sql, second)
            hint = c == '-' && after <= n && sql[after] == '+'
            i = lineend
            !hascontent && !hint && (stmtstart = i)
        elseif c == '/' && (k = nextind(sql, i); k <= n && sql[k] == '*')
            after = nextind(sql, k)
            marker = after <= n ? sql[after] : '\0'
            executable = marker == '!'
            hint = marker == '+'
            i = skipblockcomment(sql, n, i, nestedcomments)
            executable && (hascontent = true)
            !hascontent && !hint && (stmtstart = i)
        elseif c == '$' && candollarquote(sql, i) && (m = match(r"^\$[\p{L}\p{M}_][\p{L}\p{M}\p{Nd}_]*\$|^\$\$", SubString(sql, i)); m !== nothing)
            hascontent = true
            closing = findnext(m.match, sql, i + ncodeunits(m.match))
            i = closing === nothing ? n + 1 : nextind(sql, last(closing))
        else
            hascontent |= !isspace(c)
            i = nextind(sql, i)
        end
    end
    hascontent && push!(statements, statementtext(sql, stmtstart, n, lonecommentcrs))
    return statements
end

function getmigrations(conn)
    # select columns explicitly: the Migration constructor is positional, so we can't
    # depend on the physical column order of a pre-existing (e.g. Flyway-created) table
    sql = "SELECT installed_rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success FROM $MIGRATIONS_TABLE ORDER BY installed_rank"
    return executewithcursor(conn, sql) do results
        [Migration(row...) for row in results]
    end
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
previously managed by Flyway can be picked up). If a migration file is found in `dir` that has not
been applied, it will be applied to the database. If a migration file is found in `dir` that has already been applied, it
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
            executecommand(conn, MIGRATIONS_TABLE_SCHEMA)
            dbmigrations = getmigrations(conn)
        catch e
            @error "Unable to create migrations table" exception=e
            rethrow()
        end
    end
    # Any unresolved failed row means the database may contain partial changes. It
    # must block later migrations even when its version/file is absent locally.
    for dbm in dbmigrations
        dbm.success || throw(FailedMigrationError(dbm.script))
    end
    if !allunique(dbm.installed_rank for dbm in dbmigrations)
        counts = Dict{Int, Int}()
        for dbm in dbmigrations
            counts[dbm.installed_rank] = get(counts, dbm.installed_rank, 0) + 1
        end
        throw(DuplicateInstalledRankError(sort!([rank for (rank, count) in counts if count > 1])))
    end
    effectivemigrations = activehistory(dbmigrations)
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
    for dbm in effectivemigrations
        dbm.version === missing && continue
        k = versionkey(dbm.version)
        haskey(dbbyversion, k) && throw(DuplicateMigrationError([x.script for x in effectivemigrations if x.version !== missing && versionkey(x.version) == k]))
        dbbyversion[k] = dbm
    end
    # A Flyway baseline declares every lower version already represented by the
    # database state even though those versions have no individual history rows.
    baselineversion = nothing
    for dbm in effectivemigrations
        dbm.version === missing && continue
        uppercase(dbm.type) == "BASELINE" || continue
        parts = flywayversionparts(dbm.version)
        parts === nothing && continue
        if baselineversion === nothing || compareversions(parts, baselineversion[1]) > 0
            baselineversion = (parts, dbm.version)
        end
    end
    # filter out migrations that have already been applied
    migrations_to_run = Migration[]
    for m in migrations
        dbm = get(dbbyversion, versionkey(m.version), nothing)
        if dbm === nothing
            # Versions at or below a Flyway baseline are represented by that baseline
            # row and must not be run against the existing database state.
            (baselineversion === nothing || compareversions(localversionparts(m), baselineversion[1]) > 0) && push!(migrations_to_run, m)
        elseif uppercase(dbm.type) == "BASELINE"
            # A baseline is synthetic. It represents the database state rather than
            # the local SQL file at the same version, so Flyway does not validate it.
            continue
        elseif uppercase(dbm.type) != uppercase(m.type)
            throw(TypeMismatch(m.script, m.type, dbm.type))
        elseif dbm.description != m.description && dbm.description != legacydescription(m)
            throw(DescriptionMismatch(m.script, m.description, dbm.description))
        elseif dbm.checksum !== missing && dbm.checksum != m.checksum
            throw(ChecksumMismatch(m.script, m.checksum, dbm.checksum))
        end
    end
    # by default, refuse to apply migrations with versions lower than the highest
    # already-applied version; the old scan-based matching silently *re-ran*
    # already-applied migrations in this situation
    maxapplied = nothing
    for dbm in effectivemigrations
        dbm.version === missing && continue
        parts = flywayversionparts(dbm.version)
        parts === nothing && continue
        if maxapplied === nothing || compareversions(parts, maxapplied[1]) > 0
            maxapplied = (parts, dbm.version)
        end
    end
    if !allowoutoforder && maxapplied !== nothing
        outoforder = [m.script for m in migrations_to_run if compareversions(localversionparts(m), maxapplied[1]) < 0]
        isempty(outoforder) || throw(OutOfOrderMigrationError(outoforder, versiondisplay(maxapplied...)))
    end
    # run migrations; installed_rank records application order (max existing rank + 1
    # onwards), matching Flyway's semantics for the column
    nextrank = (isempty(dbmigrations) ? 0 : maximum(dbm.installed_rank for dbm in dbmigrations)) + 1
    for m in migrations_to_run
        migrationtransaction(conn) do
            start = time()
            silent || @info "Applying migrations from file: $(m.script)"
            if splitstatements
                mysqlcomments = ismysqlconnection(conn)
                nestedcomments = !mysqlcomments && !issqliteconnection(conn)
                for statement in splitsqlstatements(m.statements, mysqlcomments, nestedcomments)
                    executecommand(conn, statement)
                end
            else
                executecommand(conn, m.statements)
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
    executecommand(conn, "DROP TABLE $MIGRATIONS_TABLE")
    executecommand(conn, MIGRATIONS_TABLE_SCHEMA)
    return
end

end
