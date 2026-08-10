using Documenter, DBMigrations

makedocs(modules = [DBMigrations], sitename = "DBMigrations.jl")

deploydocs(repo = "github.com/JuliaServices/DBMigrations.jl.git", push_preview = true)
