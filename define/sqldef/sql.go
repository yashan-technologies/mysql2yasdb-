package sqldef

const (
	SQL_QUERY_MYSQL_COLUMNS = `
	SELECT table_name, column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, column_comment,
	substring(column_type,instr(column_type,'(')+1,instr(column_type,')')-instr(column_type,'(')-1) as column_type_length,
	is_nullable,ifnull(column_default,"")
	FROM information_schema.columns
	WHERE table_schema = ? 
	and table_name = ? order by  ORDINAL_POSITION`

	SQL_ALTER_COLUMN_NOT_NULL = "ALTER TABLE \"%s\".\"%s\" modify \"%s\" NOT NULL;\n"

	SQL_CREATE_TABLE = "CREATE TABLE \"%s\".\"%s\" (\n\t%s\n);"

	SQL_QUERY_AUTO_INCREMENT = `SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA = 'auto_increment'`
)
