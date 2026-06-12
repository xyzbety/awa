/// Marks dynamically assembled SQL as audited for SQLx 0.9.
///
/// Use this only for statements whose dynamic fragments are validated
/// identifiers or internally generated table names. Dynamic values should still
/// be passed through bind parameters.
pub fn audited_sql(sql: String) -> sqlx::AssertSqlSafe<String> {
    sqlx::AssertSqlSafe(sql)
}
