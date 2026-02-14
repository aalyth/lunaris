use crate::error::{LunarisError, LunarisResult};

use crate::constants::{CELL_AREA_SIZE, CELL_POINTER_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageKind {
    Invalid = 0,
    Leaf = 1,
    Interior = 2,
}

impl PageKind {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => PageKind::Leaf,
            2 => PageKind::Interior,
            _ => PageKind::Invalid,
        }
    }
}

/// A single 4 KB page, represented in the following format:
/// [id | empty byte | num_cells | start | free space | next | ... | data]
#[derive(Clone)]
pub struct Page {
    pub id: u32,
    pub dirty: bool,

    pub kind: PageKind,
    pub cells_count: u16,

    pub cell_bodies_start: u16,
    pub free_space: u16,

    /// leaf -  page id of the next sibling leaf
    /// interior - page id of the rightmost child
    pub right_pointer: u32,

    pub data: [u8; CELL_AREA_SIZE],
}

impl Page {
    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        buf[0] = self.kind as u8;
        // byte 1 - reserved
        buf[2..4].copy_from_slice(&self.cells_count.to_le_bytes());
        buf[4..6].copy_from_slice(&self.cell_bodies_start.to_le_bytes());
        buf[6..8].copy_from_slice(&self.free_space.to_le_bytes());
        buf[8..12].copy_from_slice(&self.right_pointer.to_le_bytes());
        // bytes [12; 16] - reserved
        buf[PAGE_HEADER_SIZE..].copy_from_slice(&self.data);
        buf
    }

    pub fn from_bytes(id: u32, buf: &[u8; PAGE_SIZE]) -> Self {
        let mut data = [0u8; CELL_AREA_SIZE];
        data.copy_from_slice(&buf[PAGE_HEADER_SIZE..]);

        Self {
            id,
            dirty: false,
            kind: PageKind::from_u8(buf[0]),
            cells_count: u16::from_le_bytes([buf[2], buf[3]]),
            cell_bodies_start: u16::from_le_bytes([buf[4], buf[5]]),
            free_space: u16::from_le_bytes([buf[6], buf[7]]),
            right_pointer: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            data,
        }
    }

    pub fn new_leaf(id: u32) -> Self {
        Self {
            id,
            dirty: true,
            kind: PageKind::Leaf,
            cells_count: 0,
            cell_bodies_start: CELL_AREA_SIZE as u16,
            free_space: CELL_AREA_SIZE as u16,
            right_pointer: 0,
            data: [0u8; CELL_AREA_SIZE],
        }
    }

    pub fn new_interior(id: u32) -> Self {
        Self {
            id,
            dirty: true,
            kind: PageKind::Interior,
            cells_count: 0,
            cell_bodies_start: CELL_AREA_SIZE as u16,
            free_space: CELL_AREA_SIZE as u16,
            right_pointer: 0,
            data: [0u8; CELL_AREA_SIZE],
        }
    }

    fn cell_pointer_offset(index: u16) -> usize {
        (index as usize) * CELL_POINTER_SIZE
    }

    fn cell_pointers_end(&self) -> usize {
        (self.cells_count as usize) * CELL_POINTER_SIZE
    }

    pub fn get_cell_offset(&self, index: u16) -> u16 {
        let off = Self::cell_pointer_offset(index);
        u16::from_le_bytes([self.data[off], self.data[off + 1]])
    }

    fn set_cell_offset(&mut self, index: u16, offset: u16) {
        let off = Self::cell_pointer_offset(index);
        let bytes = offset.to_le_bytes();
        self.data[off] = bytes[0];
        self.data[off + 1] = bytes[1];
    }

    /// Free gap between the end of the pointer array and the start of cell bodies.
    pub fn usable_space(&self) -> usize {
        let content_start = self.cell_bodies_start as usize;
        let pointers_end = self.cell_pointers_end();
        content_start.saturating_sub(pointers_end)
    }

    pub fn read_cell(&self, index: u16) -> &[u8] {
        let offset = self.get_cell_offset(index) as usize;
        match self.kind {
            PageKind::Leaf => {
                let data_len =
                    u16::from_le_bytes([self.data[offset + 8], self.data[offset + 9]]) as usize;
                &self.data[offset..offset + 10 + data_len]
            }
            PageKind::Interior => &self.data[offset..offset + 12],
            PageKind::Invalid => &[],
        }
    }

    // -- Leaf cell accessors: [row_id: u64][data_len: u16][row bytes...] --

    pub fn leaf_get_cell_key(cell: &[u8]) -> u64 {
        u64::from_le_bytes(cell[0..8].try_into().unwrap())
    }

    pub fn leaf_get_cell_data(cell: &[u8]) -> &[u8] {
        let data_len = u16::from_le_bytes([cell[8], cell[9]]) as usize;
        &cell[10..10 + data_len]
    }

    // -- Interior cell accessors: [left_child: u32][separator_key: u64] --

    pub fn interior_cell_left_child(cell: &[u8]) -> u32 {
        u32::from_le_bytes(cell[0..4].try_into().unwrap())
    }

    pub fn interior_cell_key(cell: &[u8]) -> u64 {
        u64::from_le_bytes(cell[4..12].try_into().unwrap())
    }

    /// Insert `cell_data` at sorted position `index`, shifting later pointers.
    pub fn insert_cell(&mut self, sorted_index: u16, cell_data: &[u8]) -> LunarisResult<()> {
        let cell_size = cell_data.len();
        let needed = cell_size + CELL_POINTER_SIZE;
        let available = self.usable_space();

        if needed > available {
            return Err(LunarisError::PageFull { needed, available });
        }

        let new_content_start = self.cell_bodies_start as usize - cell_size;
        self.data[new_content_start..new_content_start + cell_size].copy_from_slice(cell_data);
        self.cell_bodies_start = new_content_start as u16;

        for i in (sorted_index..self.cells_count).rev() {
            let off = self.get_cell_offset(i);
            self.set_cell_offset(i + 1, off);
        }
        self.set_cell_offset(sorted_index, new_content_start as u16);

        self.cells_count += 1;
        self.free_space = self.usable_space() as u16;
        self.dirty = true;
        Ok(())
    }

    /// Remove the cell at `index`, shifting later pointers left.
    /// Does not reclaim the cell body bytes (fragmentation is tolerated).
    pub fn remove_cell(&mut self, index: u16) {
        for i in index..self.cells_count - 1 {
            let off = self.get_cell_offset(i + 1);
            self.set_cell_offset(i, off);
        }
        self.cells_count -= 1;
        self.free_space = self.usable_space() as u16;
        self.dirty = true;
    }

    /// `Ok(index)` if found, `Err(index)` for the sorted insertion point.
    pub fn binary_search_leaf(&self, key: u64) -> Result<u16, u16> {
        let mut left = 0u16;
        let mut right = self.cells_count;
        while left < right {
            let mid = left + (right - left) / 2;
            let cell_key = Self::leaf_get_cell_key(self.read_cell(mid));
            match cell_key.cmp(&key) {
                std::cmp::Ordering::Equal => return Ok(mid),
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
            }
        }
        Err(left)
    }

    /// Search an interior page for `key`.
    pub fn binary_search_interior(&self, key: u64) -> u16 {
        let mut left = 0u16;
        let mut right = self.cells_count;
        while left < right {
            let mid = left + (right - left) / 2;
            let cell_key = Self::interior_cell_key(self.read_cell(mid));
            if key < cell_key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        left
    }

    pub fn make_leaf_cell(row_id: u64, data: &[u8]) -> Vec<u8> {
        let mut cell = Vec::with_capacity(10 + data.len());
        cell.extend_from_slice(&row_id.to_le_bytes());
        cell.extend_from_slice(&(data.len() as u16).to_le_bytes());
        cell.extend_from_slice(data);
        cell
    }

    pub fn make_interior_cell(left_child: u32, separator_key: u64) -> Vec<u8> {
        let mut cell = Vec::with_capacity(12);
        cell.extend_from_slice(&left_child.to_le_bytes());
        cell.extend_from_slice(&separator_key.to_le_bytes());
        cell
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_page_insert_and_search() {
        let mut page = Page::new_leaf(1);
        for i in 0u64..5 {
            let data = format!("row_{i}");
            let cell = Page::make_leaf_cell(i * 10, data.as_bytes());
            let pos = page.binary_search_leaf(i * 10).unwrap_err();
            page.insert_cell(pos, &cell).unwrap();
        }

        assert_eq!(page.cells_count, 5);
        assert_eq!(page.binary_search_leaf(20), Ok(2));

        let pos = page.binary_search_leaf(15).unwrap_err();
        assert_eq!(pos, 2);
    }

    #[test]
    fn test_leaf_page_remove() {
        let mut page = Page::new_leaf(1);
        for i in 0u64..3 {
            let cell = Page::make_leaf_cell(i, &[0u8; 10]);
            let pos = page.binary_search_leaf(i).unwrap_err();
            page.insert_cell(pos, &cell).unwrap();
        }
        assert_eq!(page.cells_count, 3);
        page.remove_cell(1);
        assert_eq!(page.cells_count, 2);
        assert_eq!(Page::leaf_get_cell_key(page.read_cell(0)), 0);
        assert_eq!(Page::leaf_get_cell_key(page.read_cell(1)), 2);
    }

    #[test]
    fn test_interior_page_binary_search() {
        let mut page = Page::new_interior(1);
        for &key in &[10u64, 20, 30] {
            let cell = Page::make_interior_cell(0, key);
            let pos = page.binary_search_interior(key);
            page.insert_cell(pos, &cell).unwrap();
        }
        assert_eq!(page.binary_search_interior(5), 0);
        assert_eq!(page.binary_search_interior(10), 1);
        assert_eq!(page.binary_search_interior(15), 1);
        assert_eq!(page.binary_search_interior(35), 3);
    }

    #[test]
    fn test_roundtrip_to_bytes() {
        let mut page = Page::new_leaf(7);
        let cell = Page::make_leaf_cell(42, b"hello");
        page.insert_cell(0, &cell).unwrap();
        page.right_pointer = 99;

        let bytes = page.to_bytes();
        let restored = Page::from_bytes(7, &bytes);

        assert_eq!(restored.kind, PageKind::Leaf);
        assert_eq!(restored.cells_count, 1);
        assert_eq!(restored.right_pointer, 99);
        let c = restored.read_cell(0);
        assert_eq!(Page::leaf_get_cell_key(c), 42);
        assert_eq!(Page::leaf_get_cell_data(c), b"hello");
    }
}
