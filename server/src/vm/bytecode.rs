use crate::storage::row::TableSchema;

#[derive(Debug, Clone)]
pub enum Instruction {
    Init {
        target: usize,
    },
    Goto {
        target: usize,
    },
    Halt,

    OpenReadCursor {
        cursor: i32,
        table: String,
    },
    OpenReadWriteCursor {
        cursor: i32,
        table: String,
    },
    RewindCursor {
        cursor: i32,
        empty_target: usize,
    },
    CursorAdvance {
        cursor: i32,
        loop_target: usize,
    },
    CloseCursor {
        cursor: i32,
    },

    Integer {
        value: i64,
        reg: usize,
    },
    String {
        value: String,
        reg: usize,
    },
    Float {
        value: f64,
        reg: usize,
    },
    Bool {
        value: bool,
        reg: usize,
    },
    Null {
        reg: usize,
    },

    ReadColumn {
        cursor: i32,
        col_index: usize,
        reg: usize,
    },
    ReadRowId {
        cursor: i32,
        reg: usize,
    },
    WriteResultRow {
        start: usize,
        count: usize,
    },

    Jeq {
        left: usize,
        right: usize,
        target: usize,
    },
    Jne {
        left: usize,
        right: usize,
        target: usize,
    },
    Jlt {
        left: usize,
        right: usize,
        target: usize,
    },
    Jle {
        left: usize,
        right: usize,
        target: usize,
    },
    Jgt {
        left: usize,
        right: usize,
        target: usize,
    },
    Jge {
        left: usize,
        right: usize,
        target: usize,
    },

    CreateRecord {
        start: usize,
        count: usize,
    },
    InsertRecord {
        cursor: i32,
        key_reg: usize,
    },
    DeleteRow {
        cursor: i32,
    },

    CreateTable {
        schema: TableSchema,
    },
}

#[derive(Debug, Clone)]
pub struct Program {
    pub instructions: Vec<Instruction>,
    pub result_columns: Vec<String>,
}

impl Program {
    pub fn new() -> Self {
        Self {
            instructions: Vec::new(),
            result_columns: Vec::new(),
        }
    }

    pub fn emit(&mut self, inst: Instruction) -> usize {
        let addr = self.instructions.len();
        self.instructions.push(inst);
        addr
    }

    pub fn update_target(&mut self, addr: usize, new_target: usize) {
        match &mut self.instructions[addr] {
            Instruction::Init { target } => *target = new_target,
            Instruction::Goto { target } => *target = new_target,
            Instruction::RewindCursor { empty_target, .. } => *empty_target = new_target,
            Instruction::CursorAdvance { loop_target, .. } => *loop_target = new_target,
            Instruction::Jeq { target, .. } => *target = new_target,
            Instruction::Jne { target, .. } => *target = new_target,
            Instruction::Jlt { target, .. } => *target = new_target,
            Instruction::Jle { target, .. } => *target = new_target,
            Instruction::Jgt { target, .. } => *target = new_target,
            Instruction::Jge { target, .. } => *target = new_target,
            _ => panic!("patch_target called on non-jump instruction"),
        }
    }

    pub fn current_addr(&self) -> usize {
        self.instructions.len()
    }
}
