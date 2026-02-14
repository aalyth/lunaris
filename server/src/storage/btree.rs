use std::path::Path;

use crate::error::{LunarisError, LunarisResult};
use crate::storage::cursor::Cursor;
use crate::storage::page::{Page, PageKind};
use crate::storage::pager::Pager;

pub struct BTreeTable {
    pub pager: Pager,
}

impl BTreeTable {
    pub fn open_or_create(path: &Path) -> LunarisResult<Self> {
        let pager = Pager::open_or_create(path)?;
        Ok(Self { pager })
    }

    pub fn root_page_id(&self) -> u32 {
        self.pager.meta.root_page_id
    }

    pub fn next_row_id(&mut self) -> u64 {
        let id = self.pager.meta.next_row_id;
        self.pager.meta.next_row_id += 1;
        id
    }

    pub fn flush(&mut self) -> LunarisResult<()> {
        self.pager.flush_all()
    }

    /// Find the leaf page containing `key` and the cell index (Ok = found, Err = insertion point).
    pub fn search(&mut self, key: u64) -> LunarisResult<(u32, Result<u16, u16>)> {
        let mut page_id = self.root_page_id();
        loop {
            let page = self.pager.get_page(page_id)?;
            match page.kind {
                PageKind::Leaf => {
                    let result = page.binary_search_leaf(key);
                    return Ok((page_id, result));
                }
                PageKind::Interior => {
                    let idx = page.binary_search_interior(key);
                    if idx < page.cells_count {
                        let cell = page.read_cell(idx);
                        page_id = Page::interior_cell_left_child(cell);
                    } else {
                        page_id = page.right_pointer;
                    }
                }
                PageKind::Invalid => {
                    return Err(LunarisError::Storage("hit free page during search".into()));
                }
            }
        }
    }

    /// Insert a row with the given key and data bytes.
    pub fn insert(&mut self, key: u64, data: &[u8]) -> LunarisResult<()> {
        let root_id = self.root_page_id();
        let cell = Page::make_leaf_cell(key, data);

        match self.insert_into_page(root_id, key, &cell)? {
            InsertResult::Done => Ok(()),
            InsertResult::Split {
                new_page_id,
                median_key,
            } => {
                // root was split — create a new root interior page
                let new_root_id = self.pager.allocate_page();
                let new_root = self.pager.get_page_mut(new_root_id)?;
                *new_root = Page::new_interior(new_root_id);

                let interior_cell = Page::make_interior_cell(root_id, median_key);
                new_root.insert_cell(0, &interior_cell)?;
                new_root.right_pointer = new_page_id;

                self.pager.meta.root_page_id = new_root_id;
                Ok(())
            }
        }
    }

    fn insert_into_page(
        &mut self,
        page_id: u32,
        key: u64,
        cell: &[u8],
    ) -> LunarisResult<InsertResult> {
        let page_type = self.pager.get_page(page_id)?.kind;

        match page_type {
            PageKind::Leaf => self.insert_into_leaf(page_id, key, cell),
            PageKind::Interior => self.insert_into_interior(page_id, key, cell),
            PageKind::Invalid => Err(LunarisError::Storage("insert hit free page".into())),
        }
    }

    fn insert_into_leaf(
        &mut self,
        page_id: u32,
        key: u64,
        cell: &[u8],
    ) -> LunarisResult<InsertResult> {
        let page = self.pager.get_page(page_id)?;
        let insert_pos = match page.binary_search_leaf(key) {
            Ok(_) => return Err(LunarisError::DuplicateKey(key)),
            Err(pos) => pos,
        };

        // try inserting directly
        let page = self.pager.get_page_mut(page_id)?;
        if page.insert_cell(insert_pos, cell).is_ok() {
            return Ok(InsertResult::Done);
        }

        // page is full — split
        self.split_leaf(page_id, key, cell)
    }

    fn insert_into_interior(
        &mut self,
        page_id: u32,
        key: u64,
        cell: &[u8],
    ) -> LunarisResult<InsertResult> {
        // find which child to descend into
        let (child_id, child_idx) = {
            let page = self.pager.get_page(page_id)?;
            let idx = page.binary_search_interior(key);
            let child = if idx < page.cells_count {
                let c = page.read_cell(idx);
                Page::interior_cell_left_child(c)
            } else {
                page.right_pointer
            };
            (child, idx)
        };

        match self.insert_into_page(child_id, key, cell)? {
            InsertResult::Done => Ok(InsertResult::Done),
            InsertResult::Split {
                new_page_id,
                median_key,
            } => {
                // child was split — insert the median key into this interior page
                let interior_cell = Page::make_interior_cell(child_id, median_key);
                let page = self.pager.get_page_mut(page_id)?;

                if child_idx < page.cells_count {
                    if page.insert_cell(child_idx, &interior_cell).is_ok() {
                        let next_cell = page.read_cell(child_idx + 1).to_vec();
                        let next_key = Page::interior_cell_key(&next_cell);
                        let replacement = Page::make_interior_cell(new_page_id, next_key);
                        page.remove_cell(child_idx + 1);
                        page.insert_cell(child_idx + 1, &replacement)?;
                        return Ok(InsertResult::Done);
                    }
                } else {
                    // child was the right_pointer
                    let interior_cell = Page::make_interior_cell(child_id, median_key);
                    if page.insert_cell(child_idx, &interior_cell).is_ok() {
                        page.right_pointer = new_page_id;
                        return Ok(InsertResult::Done);
                    }
                }

                // interior page is also full — split it
                self.split_interior(page_id, child_id, new_page_id, median_key)
            }
        }
    }

    fn split_leaf(
        &mut self,
        page_id: u32,
        new_key: u64,
        new_cell: &[u8],
    ) -> LunarisResult<InsertResult> {
        // collect all existing cells + the new one, sorted
        let page = self.pager.get_page(page_id)?;
        let old_right = page.right_pointer;
        let num = page.cells_count;

        let mut all_cells: Vec<(u64, Vec<u8>)> = Vec::with_capacity(num as usize + 1);
        for i in 0..num {
            let cell = page.read_cell(i);
            let key = Page::leaf_get_cell_key(cell);
            all_cells.push((key, cell.to_vec()));
        }

        // insert new cell in sorted position
        let pos = all_cells.partition_point(|(k, _)| *k < new_key);
        all_cells.insert(pos, (new_key, new_cell.to_vec()));

        let mid = all_cells.len() / 2;
        let median_key = all_cells[mid].0;

        // left half stays in the original page
        let new_right_id = self.pager.allocate_page();

        let left_page = self.pager.get_page_mut(page_id)?;
        *left_page = Page::new_leaf(page_id);
        for (_, cell) in &all_cells[..mid] {
            let k = Page::leaf_get_cell_key(cell);
            let pos = left_page.binary_search_leaf(k).unwrap_err();
            left_page.insert_cell(pos, cell)?;
        }
        left_page.right_pointer = new_right_id;

        // right half goes to the new page
        let right_page = self.pager.get_page_mut(new_right_id)?;
        *right_page = Page::new_leaf(new_right_id);
        for (_, cell) in &all_cells[mid..] {
            let k = Page::leaf_get_cell_key(cell);
            let pos = right_page.binary_search_leaf(k).unwrap_err();
            right_page.insert_cell(pos, cell)?;
        }
        right_page.right_pointer = old_right;

        Ok(InsertResult::Split {
            new_page_id: new_right_id,
            median_key,
        })
    }

    fn split_interior(
        &mut self,
        page_id: u32,
        new_child_left: u32,
        new_child_right: u32,
        new_key: u64,
    ) -> LunarisResult<InsertResult> {
        let page = self.pager.get_page(page_id)?;
        let old_right = page.right_pointer;
        let num = page.cells_count;

        // collect all interior cells + the new one
        let mut all_cells: Vec<(u64, u32)> = Vec::with_capacity(num as usize + 1);
        for i in 0..num {
            let cell = page.read_cell(i);
            let key = Page::interior_cell_key(cell);
            let left_child = Page::interior_cell_left_child(cell);
            all_cells.push((key, left_child));
        }

        // insert the new separator
        let pos = all_cells.partition_point(|(k, _)| *k < new_key);
        all_cells.insert(pos, (new_key, new_child_left));

        // fix up child pointers - the entry after the inserted one should
        // have its left_child changed to new_child_right
        if pos + 1 < all_cells.len() {
            all_cells[pos + 1].1 = new_child_right;
        }

        let mid = all_cells.len() / 2;
        let median_key = all_cells[mid].0;

        let left_right_ptr = all_cells[mid].1;
        let rightmost = if pos + 1 >= all_cells.len() {
            new_child_right
        } else {
            old_right
        };
        let right_right_ptr = rightmost;

        // rebuild left page
        let new_right_id = self.pager.allocate_page();
        let left_page = self.pager.get_page_mut(page_id)?;
        *left_page = Page::new_interior(page_id);
        for (i, (key, left_child)) in all_cells[..mid].iter().enumerate() {
            let cell = Page::make_interior_cell(*left_child, *key);
            left_page.insert_cell(i as u16, &cell)?;
        }
        left_page.right_pointer = left_right_ptr;

        // build right page
        let right_page = self.pager.get_page_mut(new_right_id)?;
        *right_page = Page::new_interior(new_right_id);
        for (i, (key, left_child)) in all_cells[mid + 1..].iter().enumerate() {
            let cell = Page::make_interior_cell(*left_child, *key);
            right_page.insert_cell(i as u16, &cell)?;
        }
        right_page.right_pointer = right_right_ptr;

        Ok(InsertResult::Split {
            new_page_id: new_right_id,
            median_key,
        })
    }

    /// Delete the row with the given key. Returns true if found and deleted.
    pub fn delete(&mut self, key: u64) -> LunarisResult<bool> {
        let (page_id, search_result) = self.search(key)?;
        match search_result {
            Ok(index) => {
                let page = self.pager.get_page_mut(page_id)?;
                page.remove_cell(index);
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    /// Read the row data for a given key (without the key prefix).
    pub fn get(&mut self, key: u64) -> LunarisResult<Option<Vec<u8>>> {
        let (page_id, search_result) = self.search(key)?;
        match search_result {
            Ok(index) => {
                let page = self.pager.get_page(page_id)?;
                let cell = page.read_cell(index);
                Ok(Some(Page::leaf_get_cell_data(cell).to_vec()))
            }
            Err(_) => Ok(None),
        }
    }

    /// Read the cell data at a given `Cursor` position.
    pub fn get_cell_data_at(&mut self, cursor: &Cursor) -> LunarisResult<&[u8]> {
        let page = self.pager.get_page(cursor.current_page_id())?;
        let cell = page.read_cell(cursor.current_cell_index());
        Ok(Page::leaf_get_cell_data(cell))
    }
}

enum InsertResult {
    Done,
    Split { new_page_id: u32, median_key: u64 },
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
    fn test_insert_and_search() {
        let path = temp_path("btree_basic.db");
        let _ = std::fs::remove_file(&path);

        let mut tree = BTreeTable::open_or_create(&path).unwrap();
        for i in 1u64..=10 {
            let data = format!("row_{i}");
            tree.insert(i, data.as_bytes()).unwrap();
        }

        for i in 1u64..=10 {
            let data = tree.get(i).unwrap().unwrap();
            assert_eq!(String::from_utf8(data).unwrap(), format!("row_{i}"));
        }

        assert!(tree.get(999).unwrap().is_none());

        tree.flush().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_insert_many_forces_splits() {
        let path = temp_path("btree_split.db");
        let _ = std::fs::remove_file(&path);

        let mut tree = BTreeTable::open_or_create(&path).unwrap();
        // Insert enough rows to force multiple leaf splits.
        // Each cell is ~110 bytes → ~37 cells per leaf → split around 37.
        for i in 1u64..=200 {
            let data = format!("data_{i:0>100}"); // 100-char payload
            tree.insert(i, data.as_bytes()).unwrap();
        }

        // Verify all rows
        for i in 1u64..=200 {
            let data = tree
                .get(i)
                .unwrap()
                .unwrap_or_else(|| panic!("key {i} not found"));
            let expected = format!("data_{i:0>100}");
            assert_eq!(String::from_utf8(data).unwrap(), expected);
        }

        tree.flush().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_delete() {
        let path = temp_path("btree_delete.db");
        let _ = std::fs::remove_file(&path);

        let mut tree = BTreeTable::open_or_create(&path).unwrap();
        for i in 1u64..=5 {
            tree.insert(i, b"x").unwrap();
        }

        assert!(tree.delete(3).unwrap());
        assert!(!tree.delete(3).unwrap()); // already gone
        assert!(tree.get(3).unwrap().is_none());
        assert!(tree.get(2).unwrap().is_some());
        assert!(tree.get(4).unwrap().is_some());

        let _ = std::fs::remove_file(&path);
    }
}
