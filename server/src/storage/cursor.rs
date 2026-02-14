use crate::error::LunarisResult;
use crate::storage::btree::BTreeTable;
use crate::storage::page::{Page, PageKind};
use crate::storage::row::{deserialize_row, TableSchema};
use lunaris_common::value::Value;

pub struct Cursor {
    schema: TableSchema,
    current_page: u32,
    current_cell: u16,
    num_cells: u16,
    done: bool,
}

impl Cursor {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            current_page: 0,
            current_cell: 0,
            num_cells: 0,
            done: true,
        }
    }

    /// Position the cursor at the first row (leftmost leaf, cell 0).
    /// Returns false if the table is empty.
    pub fn rewind(&mut self, tree: &mut BTreeTable) -> LunarisResult<bool> {
        let mut page_id = tree.root_page_id();
        loop {
            let page = tree.pager.get_page(page_id)?;
            match page.kind {
                PageKind::Interior => {
                    if page.cells_count > 0 {
                        let cell = page.read_cell(0);
                        page_id = Page::interior_cell_left_child(cell);
                    } else {
                        page_id = page.right_pointer;
                    }
                }
                PageKind::Leaf => {
                    self.current_page = page_id;
                    self.current_cell = 0;
                    self.num_cells = page.cells_count;
                    self.done = page.cells_count == 0;
                    return Ok(!self.done);
                }
                PageKind::Invalid => {
                    self.done = true;
                    return Ok(false);
                }
            }
        }
    }

    /// Advance to the next row, returns false when there are no more rows.
    pub fn next(&mut self, tree: &mut BTreeTable) -> LunarisResult<bool> {
        if self.done {
            return Ok(false);
        }

        self.current_cell += 1;
        if self.current_cell < self.num_cells {
            return Ok(true);
        }

        let page = tree.pager.get_page(self.current_page)?;
        let next_page = page.right_pointer;
        if next_page == 0 {
            self.done = true;
            return Ok(false);
        }

        let next = tree.pager.get_page(next_page)?;
        self.current_page = next_page;
        self.current_cell = 0;
        self.num_cells = next.cells_count;
        self.done = next.cells_count == 0;
        Ok(!self.done)
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn current_page_id(&self) -> u32 {
        self.current_page
    }

    pub fn current_cell_index(&self) -> u16 {
        self.current_cell
    }

    /// Read the row_id (key) of the current cell.
    pub fn row_id(&self, tree: &mut BTreeTable) -> LunarisResult<u64> {
        let page = tree.pager.get_page(self.current_page)?;
        let cell = page.read_cell(self.current_cell);
        Ok(Page::leaf_get_cell_key(cell))
    }

    /// Read a single column from the current row.
    pub fn column(&self, tree: &mut BTreeTable, col_index: usize) -> LunarisResult<Value> {
        let row = self.read_row(tree)?;
        Ok(row.into_iter().nth(col_index).unwrap_or(Value::Null))
    }

    /// Deserialize the full current row.
    pub fn read_row(&self, tree: &mut BTreeTable) -> LunarisResult<Vec<Value>> {
        let page = tree.pager.get_page(self.current_page)?;
        let cell = page.read_cell(self.current_cell);
        let data = Page::leaf_get_cell_data(cell);
        deserialize_row(&self.schema, data)
    }

    /// Delete the current cell and reposition the cursor. Returns false if
    /// the cursor becomes invalid, i.e. no more rows.
    pub fn delete_current(&mut self, tree: &mut BTreeTable) -> LunarisResult<bool> {
        let page = tree.pager.get_page_mut(self.current_page)?;
        page.remove_cell(self.current_cell);
        self.num_cells -= 1;

        if self.current_cell < self.num_cells {
            return Ok(true);
        }

        let page = tree.pager.get_page(self.current_page)?;
        let next_page = page.right_pointer;
        if next_page == 0 {
            self.done = true;
            return Ok(false);
        }

        let next = tree.pager.get_page(next_page)?;
        self.current_page = next_page;
        self.current_cell = 0;
        self.num_cells = next.cells_count;
        self.done = next.cells_count == 0;
        Ok(!self.done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("lunaris_test");
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn test_cursor_scan() {
        let path = temp_path("cursor_test.db");
        let _ = std::fs::remove_file(&path);

        let mut tree = BTreeTable::open_or_create(&path).unwrap();
        for i in 1u64..=50 {
            let data = vec![0u8; 10];
            tree.insert(i, &data).unwrap();
        }

        let schema = TableSchema::new("test".into(), vec![]);
        let mut cursor = Cursor::new(schema);
        let has_data = cursor.rewind(&mut tree).unwrap();
        assert!(has_data);

        let mut count = 0u64;
        loop {
            let key = cursor.row_id(&mut tree).unwrap();
            count += 1;
            assert_eq!(key, count);
            if !cursor.next(&mut tree).unwrap() {
                break;
            }
        }
        assert_eq!(count, 50);

        let _ = std::fs::remove_file(&path);
    }
}
