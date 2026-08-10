using Test, DBMigrations, DBInterface
using SQLite

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

    @testset "failed migration recorded in history table" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, '1', 'first', 'SQL', 'V1__first.sql', 0, 'flyway', 10, false)")
            @test_throws DBMigrations.FailedMigrationError DBMigrations.runmigrations(db, dir; silent=true)
        end
    end

    @testset "duplicate versions in history table" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__first.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            for script in ("V1__first.sql", "V1__other.sql")
                DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, '1', 'first', 'SQL', '$script', 0, 'flyway', 10, true)")
            end
            @test_throws DBMigrations.DuplicateMigrationError DBMigrations.runmigrations(db, dir; silent=true)
        end
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

    @testset "Flyway baseline row with NULL checksum" begin
        mktempdir() do dir
            write(joinpath(dir, "V1__baseline.sql"), "CREATE TABLE t1 (x INT);")
            db = SQLite.DB()
            DBInterface.execute(db, DBMigrations.MIGRATIONS_TABLE_SCHEMA)
            DBInterface.execute(db, "INSERT INTO $(DBMigrations.MIGRATIONS_TABLE) (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (1, '1', 'baseline', 'BASELINE', 'V1__baseline.sql', NULL, 'flyway', 10, true)")
            # baseline row means V1 counts as applied even though checksums can't be compared
            @test isempty(DBMigrations.runmigrations(db, dir; silent=true))
        end
    end
end
