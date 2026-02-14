use std::collections::HashMap;
use std::path::Path;

use crate::error::{LunarisError, LunarisResult};
use crate::storage::btree::BTreeTable;
use crate::storage::cursor::Cursor;
use crate::storage::row::TableSchema;

pub struct Catalog {
    schemas: HashMap<String, TableSchema>,
    btree: BTreeTable,
}

impl Catalog {
    pub fn open(db_dir: &Path) -> LunarisResult<Self> {
        let path = db_dir.join("catalog.db");
        let mut btree = BTreeTable::open_or_create(&path)?;
        let mut schemas = HashMap::new();

        let dummy_schema = TableSchema::new("_catalog".into(), vec![]);
        let mut cursor = Cursor::new(dummy_schema);
        if cursor.rewind(&mut btree)? {
            loop {
                let data = btree.get_cell_data_at(&cursor)?;
                if let Ok(schema) = serde_json::from_slice::<TableSchema>(data) {
                    schemas.insert(schema.table_name.clone(), schema);
                }

                if !cursor.next(&mut btree)? {
                    break;
                }
            }
        }

        Ok(Self { schemas, btree })
    }

    pub fn get_schema(&self, table_name: &str) -> LunarisResult<TableSchema> {
        self.schemas
            .get(table_name)
            .cloned()
            .ok_or_else(|| LunarisError::TableNotFound(table_name.to_string()))
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.schemas.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn table_exists(&self, table_name: &str) -> bool {
        self.schemas.contains_key(table_name)
    }

    pub fn register_table(&mut self, schema: &TableSchema) -> LunarisResult<()> {
        if self.schemas.contains_key(&schema.table_name) {
            return Err(LunarisError::TableAlreadyExists(schema.table_name.clone()));
        }

        let key = self.btree.next_row_id();
        let data = serde_json::to_vec(schema).map_err(|e| LunarisError::Storage(e.to_string()))?;
        self.btree.insert(key, &data)?;
        self.btree.flush()?;

        self.schemas
            .insert(schema.table_name.clone(), schema.clone());
        Ok(())
    }
}
