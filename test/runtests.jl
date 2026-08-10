using Test, DBMigrations, DBInterface
using SQLite

# A minimal stand-in for LibPQ's DBInterface wrapper. LibPQ is intentionally not a
# test dependency, but its public type/module names select DBMigrations' compatibility
# path and let the transaction/placeholder behavior be regression-tested.
module LibPQ
import DBInterface

mutable struct DBConnection <: DBInterface.Connection
    executions::Vector{Tuple{String, Any}}
    closed::Int
end

struct Result
    conn::DBConnection
end

Base.close(result::Result) = (result.conn.closed += 1)

function DBInterface.execute(conn::DBConnection, sql::AbstractString, params=())
    push!(conn.executions, (String(sql), params))
    return Result(conn)
end
end

module MySQL
import DBInterface
struct Connection <: DBInterface.Connection end
end

@testset "DBMigrations" begin
    @testset "SQLite" begin
        db = SQLite.DB()

        dir = "test1"
        migrations = DBMigrations.runmigrations(db, abspath(joinpath(dirname(pathof(DBMigrations)), "..", "test/sqlite", dir)))
        @test length(migrations) == 3
        @test migrations[1].script == "V1__baseline.sql"
        @test migrations[2].script == "V2__latlong.sql"
        @test migrations[3].script == "V3__modify.sql"
        dbmigrations = [DBMigrations.Migration(m...) for m in DBInterface.execute(db, "SELECT * FROM $(DBMigrations.MIGRATIONS_TABLE) ORDER BY installed_rank ASC")]
        @test migrations == dbmigrations

        # re-running same directory with no changes should not apply any migrations
        migrations = DBMigrations.runmigrations(db, abspath(joinpath(dirname(pathof(DBMigrations)), "..", "test/sqlite", dir)))
        @test length(migrations) == 0

        # run a new migration
        dir = "test2"
        migrations = DBMigrations.runmigrations(db, abspath(joinpath(dirname(pathof(DBMigrations)), "..", "test/sqlite", dir)))
        @test length(migrations) == 1
        @test migrations[1].script == "V4__modify_back.sql"
        # test table was successfully renamed back to original name
        @test isempty(DBInterface.execute(db, "SELECT * FROM latlong"))
        @test_throws SQLiteException DBInterface.execute(db, "SELECT * FROM latlongs")

        dir = "test3"
        migrations = DBMigrations.runmigrations(db, abspath(joinpath(dirname(pathof(DBMigrations)), "..", "test/sqlite", dir)))
        @test length(migrations) == 1
        @test migrations[1].script == "V5__multiple.sql"
        for i = 1:5
            @test isempty(DBInterface.execute(db, "SELECT * FROM points$i"))
        end

        # Error Handling Scenarios
        # Syntax Error in Migration: Introduce a syntax error in a migration file.
        nmigrations = length(collect(DBInterface.execute(db, "SELECT * FROM $(DBMigrations.MIGRATIONS_TABLE)")))
        dir = "error1"
        @test_throws SQLiteException DBMigrations.runmigrations(db, abspath(joinpath(dirname(pathof(DBMigrations)), "..", "test/sqlite", dir)))
        @test length(collect(DBInterface.execute(db, "SELECT * FROM $(DBMigrations.MIGRATIONS_TABLE)"))) == nmigrations
        @test_throws SQLiteException DBInterface.execute(db, "SELECT * FROM invalid")

        dir = "error2"
        @test_throws DBMigrations.DuplicateMigrationError DBMigrations.runmigrations(db, abspath(joinpath(dirname(pathof(DBMigrations)), "..", "test/sqlite", dir)))
    end

    @testset "directory path containing V<digits>" begin
        # regression test: rank/prefix used to be parsed from the full path, so a
        # path segment like `V2` corrupted every migration's version
        mktempdir() do tmp
            dir = joinpath(tmp, "appV2", "migrations")
            mkpath(dir)
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT);")
            write(joinpath(dir, "V3__third.sql"), "CREATE TABLE t3 (x INT);")
            db = SQLite.DB()
            migrations = DBMigrations.runmigrations(db, dir; silent=true)
            @test length(migrations) == 2
            @test migrations[1].installed_rank == 1
            @test migrations[2].installed_rank == 3
            @test isempty(DBMigrations.runmigrations(db, dir; silent=true))
        end
    end

    @testset "nonexistent migrations directory" begin
        db = SQLite.DB()
        @test_throws ArgumentError DBMigrations.runmigrations(db, joinpath(@__DIR__, "does_not_exist"))
    end

    @testset "out-of-order migrations" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT);")
            write(joinpath(dir, "V3__third.sql"), "CREATE TABLE t3 (x INT);")
            db = SQLite.DB()
            @test length(DBMigrations.runmigrations(db, dir; silent=true)) == 2
            # V2 shows up after V3 was already applied
            write(joinpath(dir, "V2__second.sql"), "CREATE TABLE t2 (x INT);")
            @test_throws DBMigrations.OutOfOrderMigrationError DBMigrations.runmigrations(db, dir; silent=true)
            # regression test: the old matching logic re-ran already-applied V3 here
            migrations = DBMigrations.runmigrations(db, dir; silent=true, allowoutoforder=true)
            @test length(migrations) == 1
            @test migrations[1].script == "V2__second.sql"
            @test isempty(DBMigrations.runmigrations(db, dir; silent=true))
        end
    end

    @testset "duplicate version of an already-applied migration" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__original.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            @test length(DBMigrations.runmigrations(db, dir; silent=true)) == 1
            # regression test: a duplicate of an already-applied version used to be
            # silently run because uniqueness was only checked on pending migrations
            write(joinpath(dir, "V1__sneaky_duplicate.sql"), "CREATE TABLE t1b (x INT);")
            @test_throws DBMigrations.DuplicateMigrationError DBMigrations.runmigrations(db, dir; silent=true)
            @test_throws SQLiteException DBInterface.execute(db, "SELECT * FROM t1b")
        end
    end

    @testset "checksum mismatch on modified migration" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            @test length(DBMigrations.runmigrations(db, dir; silent=true)) == 1
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT, y INT);")
            @test_throws DBMigrations.ChecksumMismatch DBMigrations.runmigrations(db, dir; silent=true)
        end
    end

    @testset "checksum zero is distinct from NULL" begin
        mktempdir() do dir
            path = joinpath(dir, "V1__empty.sql")
            write(path, "")
            db = SQLite.DB()
            @test length(DBMigrations.runmigrations(db, dir; silent=true)) == 1
            @test only(DBMigrations.getmigrations(db)).checksum == 0
            write(path, "CREATE TABLE changed_after_apply (x INT);")
            @test_throws DBMigrations.ChecksumMismatch DBMigrations.runmigrations(db, dir; silent=true)
        end
    end

    @testset "failed migration recorded in history table" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, '1', 'first', 'SQL', 'V1__first.sql', 0, 'flyway', 10, false)")
            @test_throws DBMigrations.FailedMigrationError DBMigrations.runmigrations(db, dir; silent=true)
        end
        # A failed row must also block when no local file has that version. Otherwise
        # later migrations can run against a database with partial prior changes.
        mktempdir() do dir
            write(joinpath(dir, "V2__second.sql"), "CREATE TABLE t2 (x INT);")
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, '1', 'failed', 'SQL', 'V1__failed.sql', 123, 'flyway', 10, false)")
            @test_throws DBMigrations.FailedMigrationError DBMigrations.runmigrations(db, dir; silent=true)
            @test_throws SQLiteException DBInterface.execute(db, "SELECT * FROM t2")
        end
        # Failed repeatable rows have no version, but still represent unresolved
        # partial database changes and must block versioned migrations.
        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, NULL, 'views', 'SQL', 'R__views.sql', 123, 'flyway', 10, false)")
            @test_throws DBMigrations.FailedMigrationError DBMigrations.runmigrations(db, dir; silent=true)
        end
    end

    @testset "Flyway description normalization and validation" begin
        mktempdir() do dir
            path = joinpath(dir, "V1__create_users.sql")
            write(path, "CREATE TABLE users (id INT);")
            db = SQLite.DB()
            DBMigrations.runmigrations(db, dir; silent=true)
            @test only(DBMigrations.getmigrations(db)).description == "create users"

            # Flyway validates description changes even when the SQL checksum is the
            # same, so renaming an applied migration must not pass silently.
            renamed = joinpath(dir, "V1__renamed.sql")
            mv(path, renamed)
            @test_throws DBMigrations.DescriptionMismatch DBMigrations.runmigrations(db, dir; silent=true)
        end

        # Rows written by older DBMigrations releases used raw underscores. Keep
        # those histories readable while new rows use Flyway's spelling.
        mktempdir() do dir
            path = joinpath(dir, "V1__create_users.sql")
            write(path, "CREATE TABLE users (id INT);")
            m = DBMigrations.Migration(path)
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, '1', 'create_users', 'SQL', 'V1__create_users.sql', $(m.checksum), 'DBMigrations.jl', 10, true)")
            @test isempty(DBMigrations.runmigrations(db, dir; silent=true))
        end
    end

    @testset "duplicate versions in history table" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            # a table created without the primary key (e.g. by older versions of this
            # package) can hold duplicate ranks; runmigrations must detect them
            DBInterface.execute(db, replace(DBMigrations.MIGRATIONS_TABLE_SCHEMA, r",\s*CONSTRAINT[^)]*\)" => ""))
            for script in ("V1__first.sql", "V1__other.sql")
                DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, '1', 'first', 'SQL', '$script', 0, 'flyway', 10, true)")
            end
            @test_throws DBMigrations.DuplicateMigrationError DBMigrations.runmigrations(db, dir; silent=true)
        end
    end

    @testset "history table primary key rejects duplicate ranks" begin
        db = SQLite.DB()
        DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
        insertsql = "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, '1', 'first', 'SQL', 'V1__first.sql', 0, 'x', 10, true)"
        DBInterface.execute(db, insertsql)
        @test_throws SQLiteException DBInterface.execute(db, insertsql)
    end

    @testset "history table with different physical column order" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            # simulate a pre-existing history table whose columns are laid out in a
            # different physical order than the schema this package creates
            DBInterface.execute(db, """
                CREATE TABLE $(DBMigrations.MIGRATIONS_TABLE) (
                    success BOOLEAN NOT NULL,
                    version VARCHAR(50),
                    installed_rank INTEGER NOT NULL,
                    description VARCHAR(200) NOT NULL,
                    type VARCHAR(20) NOT NULL,
                    script VARCHAR(1000) NOT NULL,
                    checksum INTEGER,
                    installed_by VARCHAR(100) NOT NULL,
                    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    execution_time INTEGER NOT NULL
                );""")
            migrations = DBMigrations.runmigrations(db, dir; silent=true)
            @test length(migrations) == 1
            @test isempty(DBMigrations.runmigrations(db, dir; silent=true))
        end
    end

    @testset "one-shot SQLite statements are closed" begin
        db = SQLite.DB()
        activehandles() = count(wrapper -> wrapper[] != C_NULL, keys(db.stmt_wrappers))
        DBMigrations.executecommand(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
        @test activehandles() == 0
        for _ = 1:100
            @test isempty(DBMigrations.getmigrations(db))
        end
        @test activehandles() == 0

        mktempdir() do dir
            statements = join(("SELECT $i" for i = 1:100), ";")
            write(joinpath(dir, "V1__many_statements.sql"), statements)
            @test length(DBMigrations.runmigrations(db, dir; silent=true)) == 1
            @test activehandles() == 0
        end
    end

    @testset "checksums are line-ending and BOM independent" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__lf.sql"), "CREATE TABLE t1 (x INT);\nCREATE TABLE t2 (y INT);")
            write(joinpath(dir, "V2__crlf.sql"), "CREATE TABLE t1 (x INT);\r\nCREATE TABLE t2 (y INT);")
            write(joinpath(dir, "V3__cr.sql"), "CREATE TABLE t1 (x INT);\rCREATE TABLE t2 (y INT);")
            write(joinpath(dir, "V4__bom.sql"), "\ufeffCREATE TABLE t1 (x INT);\nCREATE TABLE t2 (y INT);")
            ms = [DBMigrations.Migration(joinpath(dir, f)) for f in ("V1__lf.sql", "V2__crlf.sql", "V3__cr.sql", "V4__bom.sql")]
            # reference value computed with Flyway's line-by-line CRC32 algorithm
            @test all(m.checksum == -1128550967 for m in ms)
            # BOM must also be stripped from the statements that get executed
            @test startswith(ms[4].statements, "CREATE")
        end
    end

    @testset "history values with embedded quotes are escaped" begin
        db = SQLite.DB()
        DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
        m = DBMigrations.Migration(1, "1", "it's got 'quotes'", "SQL", "V1__it's.sql", 123, "DBMigrations.jl", "", 0, false, "")
        DBMigrations.insertmigration!(db, m, 1, 5)
        stored = only(DBMigrations.getmigrations(db))
        @test stored.description == "it's got 'quotes'"
        @test stored.script == "V1__it's.sql"
        # missing version is stored as NULL, not the string "missing"
        m2 = DBMigrations.Migration(2, missing, "noversion", "SQL", "V2__noversion.sql", 456, "DBMigrations.jl", "", 0, false, "")
        DBMigrations.insertmigration!(db, m2, 2, 5)
        @test isequal([r.version for r in DBInterface.execute(db, "SELECT version FROM $(DBMigrations.MIGRATIONS_TABLE) WHERE installed_rank = 2")], [missing])
    end

    @testset "LibPQ parameter and transaction compatibility" begin
        conn = LibPQ.DBConnection(Tuple{String, Any}[], 0)
        result = DBMigrations.migrationtransaction(conn) do
            42
        end
        @test result == 42
        @test first.(conn.executions) == ["BEGIN;", "COMMIT;"]
        @test conn.closed == 2

        empty!(conn.executions)
        @test_throws ErrorException DBMigrations.migrationtransaction(conn) do
            error("migration failed")
        end
        @test first.(conn.executions) == ["BEGIN;", "ROLLBACK;"]
        @test conn.closed == 4

        empty!(conn.executions)
        m = DBMigrations.Migration(1, "1", "backslash\\'quote", "SQL", "V1__backslash\\'quote.sql", 123, "DBMigrations.jl", "", 0, false, "")
        DBMigrations.insertmigration!(conn, m, 7, 9)
        sql, params = only(conn.executions)
        @test occursin("VALUES (\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9)", sql)
        @test params == (7, "1", "backslash\\'quote", "SQL", "V1__backslash\\'quote.sql", 123, "DBMigrations.jl", 9, Int8(1))
        @test !occursin("backslash", sql)
        @test conn.closed == 5
    end

    @testset "splitsqlstatements" begin
        split_ = DBMigrations.splitsqlstatements
        @test split_("SELECT 1; SELECT 2") == ["SELECT 1", "SELECT 2"]
        @test split_("SELECT 1;;SELECT 2;") == ["SELECT 1", "SELECT 2"]
        # semicolons inside single-quoted strings, incl. '' escapes
        @test split_("INSERT INTO t VALUES ('a;b'); SELECT 1") == ["INSERT INTO t VALUES ('a;b')", "SELECT 1"]
        @test split_("INSERT INTO t VALUES ('it''s; fine'); SELECT 1") == ["INSERT INTO t VALUES ('it''s; fine')", "SELECT 1"]
        # semicolons inside quoted identifiers
        @test split_("CREATE TABLE \"we;ird\" (x INT); SELECT 1") == ["CREATE TABLE \"we;ird\" (x INT)", "SELECT 1"]
        @test split_("SELECT `a;b` FROM t; SELECT 1") == ["SELECT `a;b` FROM t", "SELECT 1"]
        @test split_("CREATE TABLE [we;ird] (x INT); SELECT 1") == ["CREATE TABLE [we;ird] (x INT)", "SELECT 1"]
        # semicolons inside comments
        @test split_("SELECT 1 -- not; a separator\n; SELECT 2") == ["SELECT 1 -- not; a separator", "SELECT 2"]
        @test split_("SELECT 1 /* not; a separator */; SELECT 2") == ["SELECT 1 /* not; a separator */", "SELECT 2"]
        # leading comments are trimmed from emitted statements (SQLite's tokenizer
        # ends -- comments only at \n, so a statement starting with a comment can be
        # parsed as all-comment and fail)
        @test split_("/* nested /* block; */ comment; */ SELECT 1") == ["SELECT 1"]
        @test split_("-- leading\nSELECT 1; -- inter\nSELECT 2") == ["SELECT 1", "SELECT 2"]
        # Executable MySQL comments and optimizer hints affect semantics and must not
        # be removed as ordinary leading comments.
        @test split_("/*!40101 SET @saved = 1 */; SELECT 2") == ["/*!40101 SET @saved = 1 */", "SELECT 2"]
        @test split_("/*+ INDEX(t idx) */ SELECT * FROM t; SELECT 2") == ["/*+ INDEX(t idx) */ SELECT * FROM t", "SELECT 2"]
        # Postgres dollar-quoted bodies
        @test split_("CREATE FUNCTION f() RETURNS void AS \$\$ BEGIN PERFORM 1; END; \$\$ LANGUAGE plpgsql; SELECT 1") ==
            ["CREATE FUNCTION f() RETURNS void AS \$\$ BEGIN PERFORM 1; END; \$\$ LANGUAGE plpgsql", "SELECT 1"]
        @test split_("SELECT \$tag\$ a; b \$tag\$; SELECT 1") == ["SELECT \$tag\$ a; b \$tag\$", "SELECT 1"]
        # comment-only/whitespace-only chunks are dropped
        @test split_("SELECT 1;\n-- done\n") == ["SELECT 1"]
        @test split_("-- nothing here\n/* at all */") == []
        @test split_("") == []
        # unterminated constructs don't hang or throw
        @test split_("SELECT 'abc") == ["SELECT 'abc"]
        @test split_("SELECT 1 /* unterminated") == ["SELECT 1 /* unterminated"]
        @test split_("SELECT \$\$ unterminated") == ["SELECT \$\$ unterminated"]
        # a lone $ isn't a dollar-quote
        @test split_("SELECT a\$b; SELECT 1") == ["SELECT a\$b", "SELECT 1"]
        # dollar-quote tags may contain non-ASCII identifier characters
        @test split_("SELECT \$é\$ a; b \$é\$; SELECT 1") == ["SELECT \$é\$ a; b \$é\$", "SELECT 1"]
        # lone \r ends a line comment (CR-only files); statements after the comment
        # used to be silently swallowed into it and never executed
        @test split_("CREATE TABLE t (x INT);\r-- seed\rINSERT INTO t VALUES (1);\r") ==
            ["CREATE TABLE t (x INT)", "INSERT INTO t VALUES (1)"]
        # SQLite does not treat lone CR as a line ending. Only comment terminators
        # are normalized; CR characters in SQL string literals remain unchanged.
        @test split_("CREATE TABLE t (x INT, -- keep y\ry INT); SELECT '\r';") ==
            ["CREATE TABLE t (x INT, -- keep y\ny INT)", "SELECT '\r'"]

        mysqlsplit = sql -> DBMigrations.splitsqlstatements(sql, true)
        @test DBMigrations.ismysqlconnection(MySQL.Connection())
        @test mysqlsplit("SELECT 1; # comment; still a comment\nSELECT 2;") == ["SELECT 1", "SELECT 2"]
        @test mysqlsplit("SELECT 2--1; SELECT 3") == ["SELECT 2--1", "SELECT 3"]
    end

    @testset "statement splitting during migration" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__seed.sql"), """
                CREATE TABLE notes (txt TEXT);
                INSERT INTO notes VALUES ('semi;colons; galore');
                -- trailing comment
                """)
            # a CR-only file must fully execute end-to-end (regression: everything
            # after the first -- comment was silently lost)
            write(joinpath(dir, "V2__cr_only.sql"), "CREATE TABLE crt (x INT);\r-- seed\rINSERT INTO crt VALUES (1);\r")
            write(joinpath(dir, "V3__inline_cr.sql"), "CREATE TABLE inline_cr (x INT, -- keep y\ry INT);")
            db = SQLite.DB()
            @test length(DBMigrations.runmigrations(db, dir; silent=true)) == 3
            @test [r.txt for r in DBInterface.execute(db, "SELECT txt FROM notes")] == ["semi;colons; galore"]
            @test [r.x for r in DBInterface.execute(db, "SELECT x FROM crt")] == [1]
            @test [r.name for r in DBInterface.execute(db, "PRAGMA table_info(inline_cr)")] == ["x", "y"]
        end
    end

    @testset "description charset and non-matching .sql files" begin
        mktempdir() do dir
            # hyphens/dots in descriptions are valid (Flyway allows them)
            write(joinpath(dir, "V1__add-index.v2.sql"), "CREATE TABLE t1 (x INT);")
            # doesn't match the naming pattern: warned about, not applied
            write(joinpath(dir, "v2__lowercase.sql"), "CREATE TABLE t2 (x INT);")
            db = SQLite.DB()
            migrations = @test_logs (:warn, r"Ignoring \.sql files") match_mode=:any DBMigrations.runmigrations(db, dir)
            @test length(migrations) == 1
            @test migrations[1].description == "add-index.v2"
            @test_throws SQLiteException DBInterface.execute(db, "SELECT * FROM t2")
        end
    end

    @testset "clean!" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            @test length(DBMigrations.runmigrations(db, dir; silent=true)) == 1
            @test_throws ArgumentError DBMigrations.clean!(db)
            DBMigrations.clean!(db; confirm=true)
            @test isempty(DBMigrations.getmigrations(db))
        end
    end

    @testset "Flyway history where installed_rank != version" begin
        # Flyway's installed_rank is an application-order counter, so e.g. versions
        # 1, 2, 5 occupy ranks 1, 2, 3; regression test: rank-based matching treated
        # version 5 as unapplied and silently re-executed it
        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE accounts (balance INT);")
            write(joinpath(dir, "V2__seed.sql"), "INSERT INTO accounts VALUES (100);")
            write(joinpath(dir, "V5__bonus.sql"), "UPDATE accounts SET balance = balance + 50;")
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            for (rank, file) in ((1, "V1__first.sql"), (2, "V2__seed.sql"), (3, "V5__bonus.sql"))
                m = DBMigrations.Migration(joinpath(dir, file))
                DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES ($rank, '$(m.version)', '$(m.description)', 'SQL', '$(m.script)', $(m.checksum), 'flyway', 10, true)")
            end
            DBInterface.execute(db, "CREATE TABLE accounts (balance INT)")
            DBInterface.execute(db, "INSERT INTO accounts VALUES (150)")
            @test isempty(DBMigrations.runmigrations(db, dir; silent=true))
            @test [r.balance for r in DBInterface.execute(db, "SELECT balance FROM accounts")] == [150]
            # a new local migration gets the next application rank (4), not its version (6)
            write(joinpath(dir, "V6__extra.sql"), "CREATE TABLE extra (x INT);")
            @test length(DBMigrations.runmigrations(db, dir; silent=true)) == 1
            @test [(r.installed_rank, r.version) for r in DBInterface.execute(db, "SELECT installed_rank, version FROM $(DBMigrations.MIGRATIONS_TABLE) ORDER BY installed_rank")] ==
                [(1, "1"), (2, "2"), (3, "5"), (4, "6")]
        end
    end

    @testset "Flyway repeatable migration row with NULL version" begin
        mktempdir() do dir
            write(joinpath(dir, "V2__second.sql"), "CREATE TABLE t2 (x INT);")
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            # a repeatable migration occupies rank 2 with no version; it must not be
            # confused with local V2 (rank-based matching threw ChecksumMismatch here)
            DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (2, NULL, 'views', 'SQL', 'R__views.sql', 12345, 'flyway', 10, true)")
            migrations = DBMigrations.runmigrations(db, dir; silent=true)
            @test length(migrations) == 1
            @test migrations[1].script == "V2__second.sql"
        end
    end

    @testset "Flyway baseline row with NULL checksum" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__baseline.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, '1', 'baseline', 'BASELINE', 'V1__baseline.sql', NULL, 'flyway', 10, true)")
            # baseline row means V1 counts as applied even though checksums and
            # descriptions can't be compared
            @test isempty(DBMigrations.runmigrations(db, dir; silent=true))
            @test only(DBMigrations.getmigrations(db)).checksum === missing
        end

        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE must_not_run_1 (x INT);")
            write(joinpath(dir, "V4__fourth.sql"), "CREATE TABLE must_not_run_4 (x INT);")
            write(joinpath(dir, "V6__after_baseline.sql"), "CREATE TABLE after_baseline (x INT);")
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, '5', '<< Flyway Baseline >>', 'BASELINE', '<< Flyway Baseline >>', NULL, 'flyway', 0, true)")
            migrations = DBMigrations.runmigrations(db, dir; silent=true)
            @test [m.script for m in migrations] == ["V6__after_baseline.sql"]
            @test_throws SQLiteException DBInterface.execute(db, "SELECT * FROM must_not_run_1")
            @test_throws SQLiteException DBInterface.execute(db, "SELECT * FROM must_not_run_4")
            @test isempty(DBInterface.execute(db, "SELECT * FROM after_baseline"))
            @test [(r.installed_rank, r.version) for r in DBInterface.execute(db, "SELECT installed_rank, version FROM $(DBMigrations.MIGRATIONS_TABLE) ORDER BY installed_rank")] ==
                [(1, "5"), (2, "6")]
        end
    end
end
