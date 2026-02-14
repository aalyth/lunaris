pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 16;
pub const CELL_AREA_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;
pub const CELL_POINTER_SIZE: usize = 2;
pub const META_PAGE_SIZE: usize = 16;

pub const VM_STARTING_REGISTERS: usize = 64;

// "LUNA"
pub const MAGIC: u32 = 0x4C554E41;
