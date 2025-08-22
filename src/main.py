#!/usr/bin/env python3
import duckdb

duckdb.install_extension("ducklake")
duckdb.load_extension("ducklake")

con = duckdb.connect("my_ducklake.db")
# Attach ducklake to the absolute path of my_ducklake.db & add a (DATA_PATH example/path)
# con.execute("ATTACH 'ducklake:my_ducklake.db' AS my_lake")

con.execute("CREATE SCHEMA IF NOT EXISTS BRONZE")
con.execute("CREATE SCHEMA IF NOT EXISTS SILVER")
con.execute("CREATE SCHEMA IF NOT EXISTS GOLD")

# example query
# with open ("sql/query.sql", "r") as f:
#     query = f.read()
# con.execute(query)

def main():
    print("Welcome to a duckdb pipeline!")

if __name__ == "__main__":
    main()
