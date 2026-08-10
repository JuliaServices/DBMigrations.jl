# DBMigrations.jl

A Julia package for managing database migrations, compatible with any database package
supporting the [DBInterface.jl](https://github.com/JuliaDatabases/DBInterface.jl) interfaces.
The migration file format and schema history table are compatible with
[Flyway](https://flywaydb.org/).

```@docs
DBMigrations.runmigrations
DBMigrations.splitsqlstatements
```
