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
end
