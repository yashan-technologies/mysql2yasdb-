package sqldef

const (
	M_SQL_QUERY_COLUMNS = `
	SELECT table_name, column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, column_comment,
	substring(column_type,instr(column_type,'(')+1,instr(column_type,')')-instr(column_type,'(')-1) as column_type_length,
	is_nullable,ifnull(column_default,"")
	FROM information_schema.columns
	WHERE table_schema = ? 
	and table_name = ? order by  ORDINAL_POSITION`
	M_SQL_QUERY_TABLE_COMMENTS = `
    SELECT table_comment
    FROM information_schema.tables
    WHERE table_schema = ? AND table_name = ? and table_type = 'BASE TABLE'`
	M_SQL_QUERY_MAX_ID      = "SELECT ifnull(max(%s),0)+1 FROM `%s`.`%s`"
	M_SQL_SHOW_INDEX        = "SHOW INDEXES FROM `%s`.`%s`"
	M_SQL_SHOW_DATABASES    = "SHOW DATABASES"
	M_SQL_QUERY_FOREIGN_KEY = `
	SELECT
	constraint_name,
	group_concat(column_name),
	referenced_table_name,
	group_concat(referenced_column_name)
	FROM
	information_schema.key_column_usage
	WHERE
	table_schema = ?
	AND table_name = ?
	AND referenced_table_name IS NOT NULL
	group by constraint_name,referenced_table_name`
	M_SQL_QUERY_TABLES = `select table_name 
    from information_schema.TABLES 
    where table_schema=? and table_type = 'BASE TABLE';`
	M_SQL_QUERY_VIEW           = "SELECT TABLE_NAME,VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '%s'"
	M_SQL_QUERY_TABLE_COUNT    = "SELECT COUNT(*) FROM `%s`.`%s` "
	M_SQL_QUERY_TABLE_DATA     = "SELECT * FROM `%s`.`%s` LIMIT %d OFFSET %d"
	M_SQL_QUERY_AUTO_INCREMENT = `SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA = 'auto_increment'`
	M_SQL_QUERY_PRIMARY_KEY    = `
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI'`
	M_SQL_QUERY_TABLE_ALL_DATA   = "SELECT * FROM `%s`.`%s`"
	M_SQL_QUERY_ORDER_RAND_LIMIT = "%s order by rand() limit %d"
)

const (
	Y_SQL_QUERY_TABLE_COUNT               = "SELECT COUNT(*) FROM \"%s\".\"%s\""
	Y_SQL_QUERY_TABLE_ROW_DATA            = "SELECT * FROM \"%s\".\"%s\" WHERE %s"
	Y_SQL_SET_DEFINE_OFF                  = "SET DEFINE OFF;\n"
	Y_SQL_ALTER_COLUMN_NOT_NULL           = "ALTER TABLE \"%s\".\"%s\" modify \"%s\" NOT NULL;\n"
	Y_SQL_COLUMN_STMT_FORMAT              = "\"%s\" %s%s"
	Y_SQL_COLUMN_COMMENT_FORMAT           = "COMMENT ON COLUMN \"%s\".\"%s\".\"%s\" IS '%s';\n"
	Y_SQL_TABLE_COMMENT_FORMAT            = "COMMENT ON TABLE \"%s\".\"%s\" IS '%s' ;\n"
	Y_SQL_CREATE_SEQUENCE_FORMAT          = "CREATE SEQUENCE \"%s\".\"%s\" START WITH %s INCREMENT BY 1;\n"
	Y_SQL_CREATE_TABLE                    = "CREATE TABLE \"%s\".\"%s\" (\n\t%s\n);"
	Y_SQL_SET_COLUMN_DEFAULT_VALUE_FORMAT = "ALTER TABLE \"%s\".\"%s\" MODIFY \"%s\" DEFAULT \"%s\".\"%s\".NEXTVAL;\n"
	Y_SQL_ADD_PRIMARY_KEY                 = "ALTER TABLE \"%s\".\"%s\" ADD PRIMARY KEY (%s);\n"
	Y_SQL_CREATE_UNIQUE_INDEX             = "CREATE UNIQUE INDEX \"%s\".\"%s\" ON \"%s\".\"%s\" (%s);\n"
	Y_SQL_ADD_UNIQUE_CONSTRAINT           = "ALTER TABLE  \"%s\".\"%s\" ADD CONSTRAINT %s UNIQUE (%s);\n"
	Y_SQL_CREATE_INDEX                    = "CREATE INDEX \"%s\".\"%s\" ON \"%s\".\"%s\" (%s);\n"
	Y_SQL_ADD_FOREIGN_KEY                 = "ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT %s FOREIGN KEY (\"%s\") REFERENCES \"%s\".\"%s\"(\"%s\");\n"
	Y_SQL_CREATE_VIEW                     = "CREATE VIEW \"%s\".\"%s\" AS %s ;\n"
	Y_SQL_INSERT_DATA                     = "INSERT INTO \"%s\".\"%s\" ( %s ) VALUES (%s)"
	Y_SQL_QUERY_COLUMN                    = "select DATA_TYPE,COLUMN_NAME from all_tab_columns where owner='%s' and TABLE_NAME='%s' order by COLUMN_ID"
)

const (
	M_DEFAULT_COLUMN_CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP"
	M_DEFAULT_COLUMN_NULL              = "\x00"
)

const (
	Y_DEFAULT_NUMBER_FORMAT = " default %s"
	Y_DEFAULT_STRING_FORMAT = " default '%s'"
	Y_DEFAULT_NULL          = " default NULL"
)

const (
	Y_CHAR_FORMAT  = "%s(%d char)"
	Y_INT_FORMAT   = "%s(%s)"
	Y_FLOAT_FORMAT = "%s(%d, %d)"
	Y_BIT_FORMAT   = "%s(%d)"
	Y_RAW_FORMAT   = "%s(%d)"
)

const (
	Y_MAX_NUMERIC_PRECISION = 38
)
