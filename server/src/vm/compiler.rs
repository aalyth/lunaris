use ast::CharacterLength::IntegerLength;
use ast::{BinaryOperator, UnaryOperator};
use sqlparser::ast::{self, Expr, FromTable, SelectItem, SetExpr, Statement, Value as SqlValue};

use crate::catalog::Catalog;
use crate::constants::CELL_AREA_SIZE;
use crate::error::{LunarisError, LunarisResult};
use crate::storage::row::{ColumnDef, ColumnType, TableSchema};
use crate::vm::bytecode::{Instruction, Program};

pub fn compile(stmt: &Statement, catalog: &Catalog) -> LunarisResult<Program> {
    match stmt {
        Statement::CreateTable(ct) => compile_create_table(ct),
        Statement::Insert(insert) => compile_insert(insert, catalog),
        Statement::Query(query) => compile_select(query, catalog),
        Statement::Delete(delete) => compile_delete(delete, catalog),
        _ => Err(LunarisError::Compile(format!(
            "unsupported statement: {stmt}"
        ))),
    }
}

fn compile_create_table(ct: &ast::CreateTable) -> LunarisResult<Program> {
    let table_name = ct.name.to_string();
    let mut columns = Vec::new();

    for col_def in &ct.columns {
        let name = col_def.name.value.clone();
        let col_type = parse_column_type(&col_def.data_type)?;
        columns.push(ColumnDef { name, col_type });
    }

    let schema = TableSchema::new(table_name, columns);
    let mut prog = Program::new();
    prog.emit(Instruction::CreateTable { schema });
    prog.emit(Instruction::Halt);
    Ok(prog)
}

fn compile_insert(insert: &ast::Insert, catalog: &Catalog) -> LunarisResult<Program> {
    let table_name = insert.table.to_string();
    let schema = catalog.get_schema(&table_name)?;

    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| LunarisError::Compile("INSERT requires VALUES(...)".into()))?;
    let rows = match source.body.as_ref() {
        SetExpr::Values(values) => &values.rows,
        _ => return Err(LunarisError::Compile("only VALUES(...) supported".into())),
    };

    let mut prog = Program::new();
    let init_addr = prog.emit(Instruction::Init { target: 0 });
    prog.emit(Instruction::Halt);
    let body = prog.current_addr();
    prog.update_target(init_addr, body);

    prog.emit(Instruction::OpenReadWriteCursor {
        cursor: 0,
        table: table_name.clone(),
    });

    for row in rows {
        if row.len() != schema.columns.len() {
            return Err(LunarisError::ValueCountMismatch {
                expected: schema.columns.len(),
                got: row.len(),
            });
        }

        let base_reg = 1;
        for (i, expr) in row.iter().enumerate() {
            let dest = base_reg + i;
            emit_expr(&mut prog, expr, dest)?;
        }

        prog.emit(Instruction::CreateRecord {
            start: base_reg,
            count: schema.columns.len(),
        });

        // Key = first column value (row_id)
        prog.emit(Instruction::InsertRecord {
            cursor: 0,
            key_reg: base_reg,
        });
    }

    prog.emit(Instruction::CloseCursor { cursor: 0 });
    prog.emit(Instruction::Halt);
    Ok(prog)
}

fn compile_select(query: &ast::Query, catalog: &Catalog) -> LunarisResult<Program> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(LunarisError::Compile("only simple SELECT supported".into())),
    };

    if select.from.len() != 1 {
        return Err(LunarisError::Compile(
            "exactly one table in FROM required".into(),
        ));
    }

    let table_name = match &select.from[0].relation {
        ast::TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err(LunarisError::Compile("only table names in FROM".into())),
    };

    let schema = catalog.get_schema(&table_name)?;
    let mut prog = Program::new();

    // Resolve projected columns
    let projected_columns = parse_column_projection(&select.projection, &schema)?;
    prog.result_columns = projected_columns
        .iter()
        .map(|(name, _)| name.clone())
        .collect();

    let init_addr = prog.emit(Instruction::Init { target: 0 });
    prog.emit(Instruction::Halt);
    let body = prog.current_addr();
    prog.update_target(init_addr, body);

    prog.emit(Instruction::OpenReadCursor {
        cursor: 0,
        table: table_name.clone(),
    });

    // Rewind — jump to close if empty
    let rewind_addr = prog.emit(Instruction::RewindCursor {
        cursor: 0,
        empty_target: 0,
    });

    let loop_top = prog.current_addr();

    // WHERE clause — emit negated condition that skips to Next
    let next_placeholder = if let Some(where_expr) = &select.selection {
        Some(emit_where_skip(&mut prog, where_expr, &schema)?)
    } else {
        None
    };

    // Emit columns into registers and produce a result row
    let result_base = 32; // use high registers to avoid conflicts with WHERE
    for (i, (_name, col_idx)) in projected_columns.iter().enumerate() {
        prog.emit(Instruction::ReadColumn {
            cursor: 0,
            col_index: *col_idx,
            reg: result_base + i,
        });
    }
    prog.emit(Instruction::WriteResultRow {
        start: result_base,
        count: projected_columns.len(),
    });

    let next_addr = prog.emit(Instruction::CursorAdvance {
        cursor: 0,
        loop_target: loop_top,
    });

    // Patch the WHERE skip and Rewind to jump here (past the loop)
    let after_loop = prog.current_addr();
    if let Some(skip_addr) = next_placeholder {
        prog.update_target(skip_addr, next_addr);
    }
    prog.update_target(rewind_addr, after_loop);

    prog.emit(Instruction::CloseCursor { cursor: 0 });
    prog.emit(Instruction::Halt);
    Ok(prog)
}

fn compile_delete(delete: &ast::Delete, catalog: &Catalog) -> LunarisResult<Program> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    if tables.len() != 1 {
        return Err(LunarisError::Compile(
            "DELETE requires exactly one table".into(),
        ));
    }
    let table_name = match &tables[0].relation {
        ast::TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err(LunarisError::Compile("only table names in FROM".into())),
    };

    let schema = catalog.get_schema(&table_name)?;
    let mut prog = Program::new();

    let init_addr = prog.emit(Instruction::Init { target: 0 });
    prog.emit(Instruction::Halt);
    let body = prog.current_addr();
    prog.update_target(init_addr, body);

    prog.emit(Instruction::OpenReadWriteCursor {
        cursor: 0,
        table: table_name.clone(),
    });
    let rewind_addr = prog.emit(Instruction::RewindCursor {
        cursor: 0,
        empty_target: 0,
    });

    let loop_top = prog.current_addr();

    // WHERE — skip non-matching rows
    let next_placeholder = if let Some(where_expr) = &delete.selection {
        Some(emit_where_skip(&mut prog, where_expr, &schema)?)
    } else {
        None
    };

    prog.emit(Instruction::DeleteRow { cursor: 0 });

    let next_addr = prog.emit(Instruction::CursorAdvance {
        cursor: 0,
        loop_target: loop_top,
    });

    let after_loop = prog.current_addr();
    if let Some(skip_addr) = next_placeholder {
        prog.update_target(skip_addr, next_addr);
    }
    prog.update_target(rewind_addr, after_loop);

    prog.emit(Instruction::CloseCursor { cursor: 0 });
    prog.emit(Instruction::Halt);
    Ok(prog)
}

/// Resolve `SELECT <column1>, <column2>, ...` into (name, column_index) pairs.
fn parse_column_projection(
    projection: &[SelectItem],
    schema: &TableSchema,
) -> LunarisResult<Vec<(String, usize)>> {
    let mut result = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (i, col) in schema.columns.iter().enumerate() {
                    result.push((col.name.clone(), i));
                }
            }
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let name = &ident.value;
                let idx = schema
                    .find_column(name)
                    .ok_or_else(|| LunarisError::ColumnNotFound(name.clone()))?;
                result.push((name.clone(), idx));
            }
            other => {
                return Err(LunarisError::Compile(format!(
                    "unsupported projection: {other}"
                )));
            }
        }
    }
    Ok(result)
}

fn parse_column_type(dt: &ast::DataType) -> LunarisResult<ColumnType> {
    match dt {
        ast::DataType::Integer(_) | ast::DataType::Int(_) | ast::DataType::BigInt(_) => {
            Ok(ColumnType::Integer)
        }
        ast::DataType::Float(_) | ast::DataType::Double(_) | ast::DataType::Real => {
            Ok(ColumnType::Float)
        }
        ast::DataType::Boolean => Ok(ColumnType::Boolean),
        ast::DataType::Varchar(len_opt) => {
            let Some(IntegerLength { length, .. }) = len_opt.as_ref() else {
                return Err(LunarisError::Compile("VARCHAR requires a length".into()));
            };

            if *length >= CELL_AREA_SIZE as u64 {
                return Err(LunarisError::Compile(
                    "VARCHAR too big - record must fit in a single page".into(),
                ));
            }

            Ok(ColumnType::Varchar(*length as u16))
        }
        ast::DataType::Text => Ok(ColumnType::Varchar(255)),
        _ => Err(LunarisError::Compile(format!("unsupported type: {dt}"))),
    }
}

fn emit_where_skip(prog: &mut Program, expr: &Expr, schema: &TableSchema) -> LunarisResult<usize> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let skip1 = emit_where_skip(prog, left, schema)?;
                let skip2 = emit_where_skip(prog, right, schema)?;
                let goto_addr = prog.emit(Instruction::Goto { target: 0 });
                prog.update_target(skip1, goto_addr);
                prog.update_target(skip2, goto_addr);
                Ok(goto_addr)
            }
            BinaryOperator::Or => {
                let true_check = emit_where_pass(prog, left, schema)?;
                let skip2 = emit_where_skip(prog, right, schema)?;
                let body_start = prog.current_addr();
                prog.update_target(true_check, body_start);
                Ok(skip2)
            }
            _ => emit_inversed_conditional(prog, left, op, right, schema),
        },
        _ => Err(LunarisError::Compile(format!(
            "unsupported WHERE expression: {expr}"
        ))),
    }
}

fn emit_where_pass(prog: &mut Program, expr: &Expr, schema: &TableSchema) -> LunarisResult<usize> {
    match expr {
        Expr::BinaryOp { left, op, right }
            if !matches!(op, BinaryOperator::And | BinaryOperator::Or) =>
        {
            emit_comparison_jump(prog, left, op, right, schema)
        }
        _ => Err(LunarisError::Compile(format!(
            "unsupported OR sub-expression: {expr}"
        ))),
    }
}

fn emit_inversed_conditional(
    prog: &mut Program,
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    schema: &TableSchema,
) -> LunarisResult<usize> {
    let (col_reg, lit_reg) = emit_comparison_operands(prog, left, right, schema)?;

    match op {
        BinaryOperator::Eq => Ok(prog.emit(Instruction::Jne {
            left: col_reg,
            right: lit_reg,
            target: 0,
        })),
        BinaryOperator::NotEq => Ok(prog.emit(Instruction::Jeq {
            left: col_reg,
            right: lit_reg,
            target: 0,
        })),
        BinaryOperator::Gt => Ok(prog.emit(Instruction::Jle {
            left: col_reg,
            right: lit_reg,
            target: 0,
        })),
        BinaryOperator::GtEq => Ok(prog.emit(Instruction::Jlt {
            left: col_reg,
            right: lit_reg,
            target: 0,
        })),
        BinaryOperator::Lt => Ok(prog.emit(Instruction::Jge {
            left: col_reg,
            right: lit_reg,
            target: 0,
        })),
        BinaryOperator::LtEq => Ok(prog.emit(Instruction::Jgt {
            left: col_reg,
            right: lit_reg,
            target: 0,
        })),
        _ => Err(LunarisError::Compile(format!("unsupported operator: {op}"))),
    }
}

fn emit_comparison_jump(
    prog: &mut Program,
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    schema: &TableSchema,
) -> LunarisResult<usize> {
    let (col_reg, lit_reg) = emit_comparison_operands(prog, left, right, schema)?;

    let addr = match op {
        BinaryOperator::Eq => prog.emit(Instruction::Jeq {
            left: col_reg,
            right: lit_reg,
            target: 0,
        }),
        BinaryOperator::NotEq => prog.emit(Instruction::Jne {
            left: col_reg,
            right: lit_reg,
            target: 0,
        }),
        BinaryOperator::Gt => prog.emit(Instruction::Jgt {
            left: col_reg,
            right: lit_reg,
            target: 0,
        }),
        BinaryOperator::GtEq => prog.emit(Instruction::Jge {
            left: col_reg,
            right: lit_reg,
            target: 0,
        }),
        BinaryOperator::Lt => prog.emit(Instruction::Jlt {
            left: col_reg,
            right: lit_reg,
            target: 0,
        }),
        BinaryOperator::LtEq => prog.emit(Instruction::Jle {
            left: col_reg,
            right: lit_reg,
            target: 0,
        }),
        _ => return Err(LunarisError::Compile(format!("unsupported operator: {op}"))),
    };
    Ok(addr)
}

/// Load both sides of a comparison into registers. Returns (col_reg, lit_reg).
fn emit_comparison_operands(
    prog: &mut Program,
    left: &Expr,
    right: &Expr,
    schema: &TableSchema,
) -> LunarisResult<(usize, usize)> {
    // Registers 1-16 are used for comparison operands
    let col_reg = 1;
    let lit_reg = 2;

    emit_operand(prog, left, col_reg, schema)?;
    emit_operand(prog, right, lit_reg, schema)?;
    Ok((col_reg, lit_reg))
}

fn emit_operand(
    prog: &mut Program,
    expr: &Expr,
    dest: usize,
    schema: &TableSchema,
) -> LunarisResult<()> {
    match expr {
        Expr::Identifier(ident) => {
            let col_idx = schema
                .find_column(&ident.value)
                .ok_or_else(|| LunarisError::ColumnNotFound(ident.value.clone()))?;
            prog.emit(Instruction::ReadColumn {
                cursor: 0,
                col_index: col_idx,
                reg: dest,
            });
        }
        Expr::Value(val) => {
            emit_literal(prog, &val.value, dest)?;
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let Expr::Value(val) = expr.as_ref() else {
                return Err(LunarisError::Compile(format!(
                    "unsupported expression: -{expr}"
                )));
            };

            let SqlValue::Number(n, _) = &val.value else {
                return Err(LunarisError::Compile(format!(
                    "unsupported expression: -{expr}"
                )));
            };

            prog.emit(parse_number(n, dest)?);
        }
        _ => {
            return Err(LunarisError::Compile(format!(
                "unsupported expression: {expr}"
            )));
        }
    }
    Ok(())
}

fn parse_number(n: &str, dest: usize) -> LunarisResult<Instruction> {
    if let Ok(i) = n.parse::<i64>() {
        Ok(Instruction::Integer {
            value: i,
            reg: dest,
        })
    } else if let Ok(f) = n.parse::<f64>() {
        Ok(Instruction::Float {
            value: f,
            reg: dest,
        })
    } else {
        Err(LunarisError::Compile(format!("invalid number: {n}")))
    }
}

fn emit_literal(prog: &mut Program, val: &SqlValue, dest: usize) -> LunarisResult<()> {
    match val {
        SqlValue::Number(n, _) => {
            prog.emit(parse_number(n, dest)?);
        }

        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            prog.emit(Instruction::String {
                value: s.clone(),
                reg: dest,
            });
        }

        SqlValue::Boolean(b) => {
            prog.emit(Instruction::Bool {
                value: *b,
                reg: dest,
            });
        }

        SqlValue::Null => {
            prog.emit(Instruction::Null { reg: dest });
        }
        _ => return Err(LunarisError::Compile(format!("unsupported literal: {val}"))),
    }
    Ok(())
}

fn emit_expr(prog: &mut Program, expr: &Expr, dest: usize) -> LunarisResult<()> {
    match expr {
        Expr::Value(val) => emit_literal(prog, &val.value, dest),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let Expr::Value(val) = expr.as_ref() else {
                return Err(LunarisError::Compile(format!(
                    "unsupported expression: -{expr}"
                )));
            };

            let SqlValue::Number(n, _) = &val.value else {
                return Err(LunarisError::Compile(format!(
                    "unsupported expression: -{expr}"
                )));
            };

            prog.emit(parse_number(n, dest)?);
            Ok(())
        }
        _ => Err(LunarisError::Compile(format!(
            "unsupported expression: {expr}"
        ))),
    }
}
