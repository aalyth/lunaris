# Lunaris

Lunaris is a relational database built from scratch in Rust. It implements the core components 
of an SQL database engine: parsing, compilation to bytecode, virtual machine execution, and 
persistent B+ tree storage.

## Structure 

- **server** -- The database engine. Accepts TCP connections, parses SQL, compiles it to 
  bytecode, and executes it against the storage layer.
- **client** -- CLI shell that connects to the server and displays query results as formatted tables.
- **common** -- Shared protocol definitions and value types.

### Execution pipeline

```
SQL text -> Parser -> Compiler -> Bytecode VM -> B-tree storage
```

SQL statements are parsed using `sqlparser`, compiled into a register-based bytecode program, 
and executed by a virtual machine that operates on B+ tree backed tables through a cursor interface.

### Storage engine

Data is stored in 4 KB pages managed by a pager. Tables are organized as B+ trees with automatic 
page splitting on insert. Rows are serialized with a null bitmap followed by fixed-size fields 
in little-endian byte order. A catalog (itself a B+ tree) persists table schemas to disk.

## Supported SQL

```sql
CREATE TABLE users (id INTEGER, name VARCHAR(64), active BOOLEAN);

INSERT INTO users VALUES (1, 'alice', true);
INSERT INTO users VALUES (2, 'bob', false);

SELECT id, name FROM users WHERE active = true;

DELETE FROM users WHERE id = 2;
```

### Statements

- `CREATE TABLE` with typed columns
- `INSERT INTO ... VALUES (...)`
- `SELECT` with column selection and `WHERE` filtering
- `DELETE FROM ... WHERE ...`

### Data types

- `INTEGER` (i64)
- `FLOAT` (f64)
- `BOOLEAN`
- `VARCHAR(n)`
- `NULL`

### Filter expressions

`WHERE` clauses support `=`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, and `OR`.

## Running

Start the server:

```
cargo run --bin lunaris-server
```

The server listens on port 7435 by default. Set `LUNARIS_PORT` to change it. Data is stored in 
`~/.lunaris` (override with `LUNARIS_DATA_DIR`).

Connect with the client:

```
cargo run --bin lunaris-client
```

## License

GNU GPL v3.0
>>>>>>> 5476fe0af4d02302d43477796f15625c9887baf7
