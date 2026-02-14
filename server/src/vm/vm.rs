use crate::constants::VM_STARTING_REGISTERS;
use crate::database::Database;
use crate::error::{LunarisError, LunarisResult};
use crate::storage::cursor::Cursor;
use crate::vm::bytecode::{Instruction, Program};
use lunaris_common::value;
use lunaris_common::value::Value;
use std::cmp::Ordering;
use std::collections::HashMap;

struct RuntimeCursor {
    table_name: String,
    cursor: Cursor,
}

/// Lunaris virtual machine - the core component, executing query logic.
pub struct Lvm {
    pc: usize,
    halted: bool,
    registers: Vec<Value>,

    // Built for future compatibility - currently all cursor indexes are
    // hardcoded to 0.
    cursors: HashMap<i32, RuntimeCursor>,

    result_rows: Vec<Vec<Value>>,
    record_buffer: Vec<Value>,
    rows_affected: u64,

    message: String,
}

impl Lvm {
    pub fn new() -> Self {
        Self {
            pc: 0,
            halted: false,
            registers: vec![Value::Null; VM_STARTING_REGISTERS],
            cursors: HashMap::new(),
            result_rows: Vec::new(),
            record_buffer: Vec::new(),
            rows_affected: 0,
            message: String::new(),
        }
    }

    pub fn execute(mut self, db: &Database, program: &Program) -> LunarisResult<ExecutionResult> {
        loop {
            if self.pc >= program.instructions.len() || self.halted {
                break;
            }

            let instr = &program.instructions[self.pc];
            self.pc += 1;
            self.execute_instr(instr, db)?;
        }

        if self.message.is_empty() {
            if !self.result_rows.is_empty() {
                self.message = format!("{} row(s) returned", self.result_rows.len());
            } else if self.rows_affected > 0 {
                self.message = format!("{} row(s) affected", self.rows_affected);
            } else {
                self.message = "OK".into();
            }
        }

        Ok(ExecutionResult {
            columns: program.result_columns.clone(),
            rows: self.result_rows,
            rows_affected: self.rows_affected,
            message: self.message,
        })
    }

    fn execute_instr(&mut self, instr: &Instruction, db: &Database) -> LunarisResult<()> {
        match instr {
            Instruction::Init { target } => self.pc = *target,
            Instruction::Goto { target } => self.pc = *target,
            Instruction::Halt => self.halted = true,

            Instruction::OpenReadCursor { cursor, table } => {
                self.open_cursor(*cursor, table, db)?
            }

            Instruction::OpenReadWriteCursor { cursor, table } => {
                self.open_cursor(*cursor, table, db)?
            }

            Instruction::RewindCursor {
                cursor,
                empty_target,
            } => {
                let oc = self.get_cursor_mut(cursor)?;
                let has_data = db.with_table_mut(&oc.table_name, |tree| oc.cursor.rewind(tree))?;
                if !has_data {
                    self.pc = *empty_target;
                }
            }

            Instruction::CursorAdvance {
                cursor,
                loop_target,
            } => {
                let open_cur = self.get_cursor_mut(cursor)?;
                let has_more =
                    db.with_table_mut(&open_cur.table_name, |tree| open_cur.cursor.next(tree))?;
                if has_more {
                    self.pc = *loop_target;
                }
            }
            Instruction::CloseCursor { cursor } => {
                self.cursors.remove(cursor);
            }

            Instruction::Integer { value, reg: dest } => {
                ensure_reg(&mut self.registers, *dest);
                self.registers[*dest] = Value::Integer(*value);
            }
            Instruction::String { value, reg: dest } => {
                ensure_reg(&mut self.registers, *dest);
                self.registers[*dest] = Value::Text(value.clone());
            }
            Instruction::Float { value, reg: dest } => {
                ensure_reg(&mut self.registers, *dest);
                self.registers[*dest] = Value::Float(*value);
            }
            Instruction::Bool { value, reg: dest } => {
                ensure_reg(&mut self.registers, *dest);
                self.registers[*dest] = Value::Boolean(*value);
            }
            Instruction::Null { reg: dest } => {
                ensure_reg(&mut self.registers, *dest);
                self.registers[*dest] = Value::Null;
            }
            Instruction::ReadColumn {
                cursor,
                col_index,
                reg: dest,
            } => {
                ensure_reg(&mut self.registers, *dest);
                let open_cur = self.get_cursor_mut(cursor)?;
                let val = db.with_table_mut(&open_cur.table_name, |tree| {
                    open_cur.cursor.column(tree, *col_index)
                })?;
                self.registers[*dest] = val;
            }
            Instruction::ReadRowId { cursor, reg: dest } => {
                ensure_reg(&mut self.registers, *dest);
                let oc = self.get_cursor_mut(cursor)?;
                let id = db.with_table_mut(&oc.table_name, |tree| oc.cursor.row_id(tree))?;
                self.registers[*dest] = Value::Integer(id as i64);
            }
            Instruction::WriteResultRow { start, count } => {
                let row: Vec<Value> = self.registers[*start..*start + *count].to_vec();
                self.result_rows.push(row);
            }

            Instruction::Jeq {
                left,
                right,
                target,
            } => {
                if value::compare(&self.registers[*left], &self.registers[*right])
                    == Some(Ordering::Equal)
                {
                    self.pc = *target;
                }
            }
            Instruction::Jne {
                left,
                right,
                target,
            } => {
                if value::compare(&self.registers[*left], &self.registers[*right])
                    != Some(Ordering::Equal)
                {
                    self.pc = *target;
                }
            }
            Instruction::Jlt {
                left,
                right,
                target,
            } => {
                if value::compare(&self.registers[*left], &self.registers[*right])
                    == Some(Ordering::Less)
                {
                    self.pc = *target;
                }
            }
            Instruction::Jle {
                left,
                right,
                target,
            } => {
                if matches!(
                    value::compare(&self.registers[*left], &self.registers[*right]),
                    Some(Ordering::Less | Ordering::Equal)
                ) {
                    self.pc = *target;
                }
            }
            Instruction::Jgt {
                left,
                right,
                target,
            } => {
                if value::compare(&self.registers[*left], &self.registers[*right])
                    == Some(Ordering::Greater)
                {
                    self.pc = *target;
                }
            }
            Instruction::Jge {
                left,
                right,
                target,
            } => {
                if matches!(
                    value::compare(&self.registers[*left], &self.registers[*right]),
                    Some(Ordering::Greater | Ordering::Equal)
                ) {
                    self.pc = *target;
                }
            }

            Instruction::CreateRecord { start, count } => {
                self.record_buffer = self.registers[*start..*start + *count].to_vec();
            }

            Instruction::InsertRecord { cursor, key_reg } => {
                let oc = self
                    .cursors
                    .get_mut(cursor)
                    .ok_or_else(|| LunarisError::Vm(format!("cursor {cursor} not open")))?;
                let key = match &self.registers[*key_reg] {
                    Value::Integer(k) => *k as u64,
                    other => {
                        return Err(LunarisError::Vm(format!(
                            "key register is not integer: {other:?}"
                        )));
                    }
                };
                let values = self.record_buffer.clone();
                db.insert_row(&oc.table_name, key, &values)?;
                self.rows_affected += 1;
            }

            Instruction::DeleteRow { cursor } => {
                let oc = self.get_cursor_mut(cursor)?;
                db.with_table_mut(&oc.table_name, |tree| {
                    oc.cursor.delete_current(tree)?;
                    tree.flush()
                })?;
                self.rows_affected += 1;
            }

            Instruction::CreateTable { schema } => {
                db.create_table(schema)?;
                self.message = format!("Table '{}' created", schema.table_name);
            }
        }

        Ok(())
    }

    fn open_cursor(&mut self, cursor: i32, table_name: &str, db: &Database) -> LunarisResult<()> {
        self.cursors.insert(
            cursor,
            RuntimeCursor {
                table_name: table_name.to_owned(),
                cursor: Cursor::new(db.get_schema(table_name)?),
            },
        );
        Ok(())
    }

    fn get_cursor_mut(&mut self, cursor: &i32) -> LunarisResult<&mut RuntimeCursor> {
        Ok(self
            .cursors
            .get_mut(cursor)
            .ok_or_else(|| LunarisError::Vm(format!("cursor {cursor} not open")))?)
    }
}

pub struct ExecutionResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: u64,
    pub message: String,
}

fn ensure_reg(regs: &mut Vec<Value>, index: usize) {
    if index >= regs.len() {
        regs.resize(index + 1, Value::Null);
    }
}
