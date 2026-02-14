use crate::catalog::Catalog;
use crate::error::{LunarisError, LunarisResult};
use crate::storage::btree::BTreeTable;
use crate::storage::row::{serialize_row, TableSchema};
use crate::vm::compiler;
use crate::vm::parser;
use crate::vm::vm::{ExecutionResult, Lvm};
use lunaris_common::value::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

pub struct Database {
    db_dir: PathBuf,
    catalog: RwLock<Catalog>,
    tables: RwLock<HashMap<String, Mutex<BTreeTable>>>,
}

impl Database {
    pub fn open(db_dir: PathBuf) -> LunarisResult<Self> {
        std::fs::create_dir_all(&db_dir)?;

        let catalog = Catalog::open(&db_dir)?;
        let tables = HashMap::new();
        let db = Self {
            db_dir,
            catalog: RwLock::new(catalog),
            tables: RwLock::new(tables),
        };

        Ok(db)
    }

    pub fn execute_sql(&self, sql: &str) -> LunarisResult<ExecutionResult> {
        let stmt = parser::parse_sql(sql)?;
        let catalog = self.catalog.read().unwrap();
        let program = compiler::compile(&stmt, &catalog)?;
        drop(catalog);

        Lvm::new().execute(self, &program)
    }

    pub fn get_schema(&self, table_name: &str) -> LunarisResult<TableSchema> {
        let catalog = self.catalog.read().unwrap();
        catalog.get_schema(table_name)
    }

    pub fn create_table(&self, schema: &TableSchema) -> LunarisResult<()> {
        let mut catalog = self.catalog.write().unwrap();
        catalog.register_table(schema)?;

        let path = self.db_dir.join(format!("{}.db", schema.table_name));
        let btree = BTreeTable::open_or_create(&path)?;

        let mut tables = self.tables.write().unwrap();
        tables.insert(schema.table_name.clone(), Mutex::new(btree));
        Ok(())
    }

    pub fn insert_row(&self, table_name: &str, key: u64, values: &[Value]) -> LunarisResult<()> {
        let schema = self.get_schema(table_name)?;
        let data = serialize_row(&schema, values)?;

        self.with_table_mut(table_name, |tree| {
            tree.insert(key, &data)?;
            tree.flush()
        })
    }

    pub fn with_table_mut<F, R>(&self, table_name: &str, f: F) -> LunarisResult<R>
    where
        F: FnOnce(&mut BTreeTable) -> LunarisResult<R>,
    {
        {
            let tables = self.tables.read().unwrap();
            if let Some(table_mutex) = tables.get(table_name) {
                let mut tree = table_mutex.lock().unwrap();
                return f(&mut tree);
            }
        }

        let path = self.db_dir.join(format!("{table_name}.db"));
        if !path.exists() {
            return Err(LunarisError::TableNotFound(table_name.to_string()));
        }
        let btree = BTreeTable::open_or_create(&path)?;

        let mut tables = self.tables.write().unwrap();
        tables.insert(table_name.to_string(), Mutex::new(btree));
        let table_mutex = tables.get(table_name).unwrap();
        let mut tree = table_mutex.lock().unwrap();
        f(&mut tree)
    }
}
