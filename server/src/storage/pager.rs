use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::constants::{MAGIC, META_PAGE_SIZE, PAGE_SIZE};
use crate::error::{LunarisError, LunarisResult};
use crate::storage::page::Page;

pub struct FileMetadata {
    pub root_page_id: u32,
    pub next_row_id: u64,
}

impl FileMetadata {
    pub fn to_bytes(&self) -> [u8; META_PAGE_SIZE] {
        let mut buf = [0u8; META_PAGE_SIZE];
        buf[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&self.root_page_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.next_row_id.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; META_PAGE_SIZE]) -> LunarisResult<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Err(LunarisError::Storage(format!(
                "bad magic: expected 0x{MAGIC:08X}, got 0x{magic:08X}"
            )));
        }
        let root_page_id = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let next_row_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        Ok(Self {
            root_page_id,
            next_row_id,
        })
    }
}

pub struct Pager {
    file: File,
    pub page_count: u32,
    cache: HashMap<u32, Page>,
    pub meta: FileMetadata,
}

impl Pager {
    pub fn open(path: &Path) -> LunarisResult<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut meta_buf = [0u8; META_PAGE_SIZE];
        file.read_exact(&mut meta_buf)?;
        let meta = FileMetadata::from_bytes(&meta_buf)?;

        let file_len = file.seek(SeekFrom::End(0))?;
        let page_count = ((file_len as usize - META_PAGE_SIZE) / PAGE_SIZE) as u32;

        Ok(Self {
            file,
            page_count,
            cache: HashMap::new(),
            meta,
        })
    }

    pub fn create(path: &Path) -> LunarisResult<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let meta = FileMetadata {
            root_page_id: 1,
            next_row_id: 1,
        };
        file.write_all(&meta.to_bytes())?;

        let root = Page::new_leaf(1);
        file.write_all(&root.to_bytes())?;
        file.sync_all()?;

        Ok(Self {
            file,
            page_count: 1,
            cache: HashMap::new(),
            meta,
        })
    }

    pub fn open_or_create(path: &Path) -> LunarisResult<Self> {
        if path.exists() {
            Self::open(path)
        } else {
            Self::create(path)
        }
    }

    pub fn get_page(&mut self, id: u32) -> LunarisResult<&Page> {
        if !self.cache.contains_key(&id) {
            let page = self.read_page_from_disk(id)?;
            self.cache.insert(id, page);
        }
        Ok(self.cache.get(&id).unwrap())
    }

    /// Get a mutable reference to a page (marks it dirty for later flush).
    pub fn get_page_mut(&mut self, id: u32) -> LunarisResult<&mut Page> {
        if !self.cache.contains_key(&id) {
            let page = self.read_page_from_disk(id)?;
            self.cache.insert(id, page);
        }
        let page = self.cache.get_mut(&id).unwrap();
        page.dirty = true;
        Ok(page)
    }

    /// Allocate a new zeroed page at the end of the file and return its id.
    pub fn allocate_page(&mut self) -> u32 {
        self.page_count += 1;
        let id = self.page_count;
        let page = Page::new_leaf(id);
        self.cache.insert(id, page);
        id
    }

    /// Write the meta header and all dirty pages to disk.
    pub fn flush_all(&mut self) -> LunarisResult<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&self.meta.to_bytes())?;

        for page in self.cache.values_mut() {
            if page.dirty {
                let offset = META_PAGE_SIZE as u64 + (page.id as u64 - 1) * PAGE_SIZE as u64;
                self.file.seek(SeekFrom::Start(offset))?;
                self.file.write_all(&page.to_bytes())?;
                page.dirty = false;
            }
        }

        self.file.sync_all()?;
        Ok(())
    }

    fn read_page_from_disk(&mut self, id: u32) -> LunarisResult<Page> {
        let offset = META_PAGE_SIZE as u64 + (id as u64 - 1) * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf)?;
        Ok(Page::from_bytes(id, &buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("lunaris_test");
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn test_create_and_reopen() {
        let path = temp_path("pager_test.db");
        let _ = std::fs::remove_file(&path);

        {
            let mut pager = Pager::create(&path).unwrap();
            let page = pager.get_page_mut(1).unwrap();
            let cell = Page::make_leaf_cell(1, b"hello");
            page.insert_cell(0, &cell).unwrap();
            pager.flush_all().unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            assert_eq!(pager.meta.root_page_id, 1);
            let page = pager.get_page(1).unwrap();
            assert_eq!(page.cells_count, 1);
            let cell = page.read_cell(0);
            assert_eq!(Page::leaf_get_cell_data(cell), b"hello");
        }

        let _ = std::fs::remove_file(&path);
    }
}
