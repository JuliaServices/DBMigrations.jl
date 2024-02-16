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
end
