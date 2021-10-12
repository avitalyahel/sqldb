# sqldb - Wrapper to sqlite3 & MySQL

## Mission

Provide simple python client for SQL Db.

## Design

1. Define table fields
2. `init()` & `fini()` for connecting and loading table info
3. Generic `create()`, `read()`, `update()` & `delete()`
4. Helpers `existing()`, `write()` & `dump()` (serialize)
5. Low-level `select()` & `select_join()`

## Examples

See [test.py]()