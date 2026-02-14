use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{LunarisError, LunarisResult};

pub fn parse_sql(sql: &str) -> LunarisResult<Statement> {
    let dialect = GenericDialect {};
    let mut stmts =
        Parser::parse_sql(&dialect, sql).map_err(|e| LunarisError::Parse(e.to_string()))?;

    if stmts.is_empty() {
        return Err(LunarisError::Parse("empty statement".into()));
    }
    if stmts.len() > 1 {
        return Err(LunarisError::Parse("only one statement at a time".into()));
    }

    Ok(stmts.remove(0))
}
