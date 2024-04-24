package modules

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"m2y/db"
	"m2y/defs/runtimedef"
	"m2y/defs/sqldef"
	"m2y/defs/typedef"
	"m2y/log"

	"git.yasdb.com/go/yasutil/fs"
)

const (
	tables_ddl = "tables"
	others_ddl = "others"
)

type Index struct {
	Table      string
	NonUnique  int
	KeyName    string
	ColumnName string
	IndexType  string
	SeqInIndex int
}

func DealTablesDDLs(mysql *sql.DB, mysqlSchema, yasdbSchema string, tables []string, withViews bool) error {
	if err := mkdirDDLPath(); err != nil {
		return err
	}
	tabFileName := path.Join(getTablesDDLPath(), fmt.Sprintf("%s_tables.sql", mysqlSchema))
	idxFileName := path.Join(getOthersDDLPath(), fmt.Sprintf("%s_others.sql", mysqlSchema))

	tableFile, err := os.Create(tabFileName)
	if err != nil {
		return err
	}
	defer tableFile.Close()
	idxFile, err := os.Create(idxFileName)
	if err != nil {
		return err
	}
	defer idxFile.Close()

	// 处理 &转义问题
	if _, err = tableFile.WriteString(sqldef.Y_SQL_SET_DEFINE_OFF); err != nil {
		return err
	}
	msgTab := "--先创建数据库内的表,列默认值,自增序列,列注释\n"
	if _, err = tableFile.WriteString(msgTab); err != nil {
		return err
	}
	for _, tableName := range tables {
		tableDDLs, nullableStrs, err := getTableDDL(mysql, mysqlSchema, yasdbSchema, tableName)
		if err != nil {
			log.Logger.Errorf("表 %s.%s DDL导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
		if _, err := tableFile.WriteString(strings.Join(tableDDLs, "\n")); err != nil {
			log.Logger.Errorf("表 %s.%s DDL导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
		tableComments, err := getTableComments(mysql, mysqlSchema, yasdbSchema, tableName)
		if err != nil {
			log.Logger.Errorf("表 %s.%s 注释导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
		if _, err := tableFile.WriteString(strings.Join(tableComments, "\n")); err != nil {
			log.Logger.Errorf("表 %s.%s 注释导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
		if _, err = idxFile.WriteString(strings.Join(nullableStrs, "\n")); err != nil {
			log.Logger.Errorf("表 %s.%s 非空约束导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
	}

	msgIdx := "\n--再创建数据库内的索引\n"
	if _, err = idxFile.WriteString(msgIdx); err != nil {
		return err
	}
	for _, tableName := range tables {
		primarykeys, err := getPrimaryKeyDDLs(mysql, mysqlSchema, yasdbSchema, tableName)
		if err != nil {
			log.Logger.Errorf("表 %s.%s 主键约束导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
		if _, err = idxFile.WriteString(strings.Join(primarykeys, "\n")); err != nil {
			log.Logger.Errorf("表 %s.%s 主键约束导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
		uniqIndexes, err := getUniqueIndexDDLs(mysql, mysqlSchema, yasdbSchema, tableName)
		if err != nil {
			log.Logger.Errorf("表 %s.%s unique索引导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
		if _, err = idxFile.WriteString(strings.Join(uniqIndexes, "\n")); err != nil {
			log.Logger.Errorf("表 %s.%s unique索引导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
		nonUniqueIndexes, err := getNonUniqueIndexDDL(mysql, mysqlSchema, yasdbSchema, tableName)
		if err != nil {
			log.Logger.Errorf("表 %s.%s non unique索引导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
		if _, err = idxFile.WriteString(strings.Join(nonUniqueIndexes, "\n")); err != nil {
			log.Logger.Errorf("表 %s.%s non unique索引导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
	}
	consIdx := "\n--最后创建外键约束\n"
	if _, err = idxFile.WriteString(consIdx); err != nil {
		return err
	}
	for _, tableName := range tables {
		constraints, err := getTableForeignKeys(mysql, mysqlSchema, yasdbSchema, tableName)
		if err != nil {
			log.Logger.Errorf("表 %s.%s 外键约束导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
		if _, err = idxFile.WriteString(strings.Join(constraints, "\n")); err != nil {
			log.Logger.Errorf("表 %s.%s 外键约束导出失败: %v", mysqlSchema, tableName, err)
			continue
		}
	}
	if withViews {
		viewMsg := "\n--创建视图\n"
		if _, err = idxFile.WriteString(viewMsg); err != nil {
			return err
		}
		viewDDLs, err := getViewDDLs(mysql, mysqlSchema, yasdbSchema)
		if err != nil {
			return err
		}
		if _, err = idxFile.WriteString(strings.Join(viewDDLs, "\n")); err != nil {
			return err
		}
	}
	return nil
}

func DealSchemasDDL(mysqlDB *sql.DB, schemas, remapSchemas []string, excludeTables []string) error {
	if err := mkdirDDLPath(); err != nil {
		return err
	}
	// 查询表的信息
	mysqlDbs, err := getMysqlAllDbs(mysqlDB)
	if err != nil {
		return err
	}
	log.Logger.Infof("开始导出DDL......")
	start := time.Now()
	for i, schema := range schemas {
		if !inArrayStr(schema, mysqlDbs) {
			log.Logger.Errorf("mysql database %s 不存在, 请检查配置文件或mysql环境\n", schema)
			continue
		}
		var tables []string
		allTables, err := getMysqlSchemaTables(mysqlDB, schema)
		if err != nil {
			log.Logger.Errorf("获取schema %s 中的表失败: %v", schema, err)
			continue
		}
		for _, table := range allTables {
			if !inArrayStr(table, excludeTables) {
				tables = append(tables, table)
			}
		}
		if err := DealTablesDDLs(mysqlDB, schema, remapSchemas[i], tables, true); err != nil {
			log.Logger.Errorf("schema %s DDL导出失败: %v", schema, err)
			continue
		}
	}
	log.Logger.Infof("任务完成，耗时: %v, 结果保存在: %s", time.Since(start), runtimedef.GetExportPath())
	return nil
}

func mkdirDDLPath() error {
	if err := fs.Mkdir(getTablesDDLPath()); err != nil {
		return err
	}
	if err := fs.Mkdir(getOthersDDLPath()); err != nil {
		return err
	}
	return nil
}

func getTablesDDLPath() string {
	return path.Join(runtimedef.GetExportPath(), tables_ddl)
}

func getOthersDDLPath() string {
	return path.Join(runtimedef.GetExportPath(), others_ddl)
}

func getTableDDL(mysql *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]string, []string, error) {
	tableDDLs, nullableStrs, err := getTableColumnDDLs(mysql, mysqlSchema, yasdbSchema, tableName)
	if err != nil {
		return nil, nil, err
	}
	autoIncrementDDLs, err := getTableAutoIncrementDDLs(mysql, mysqlSchema, yasdbSchema, tableName)
	if err != nil {
		return nil, nil, err
	}
	tableDDLs = append(tableDDLs, autoIncrementDDLs...)
	return tableDDLs, nullableStrs, nil
}

func getTableColumnDDLs(mysql *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]string, []string, error) {
	var tableDDLs, nullableStrs []string
	// 查询表的列信息
	columns, err := mysql.Query(sqldef.M_SQL_QUERY_COLUMNS, mysqlSchema, tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("查询表属性 information_schema.columns 出错: %s", err.Error())
	}
	defer columns.Close()

	// 存储表名和列信息的映射关系
	tableColumns := make(map[string][]string)
	// 存储列注释信息
	columnComments := make(map[string]string)
	// 遍历列信息结果
	for columns.Next() {
		var (
			tableName, columnName, columnComment, dataType, isNullable, columnDefault string
			maxLength, numericPrecision, numericScale                                 sql.NullInt64
			columnTypeLength                                                          sql.NullString
		)
		if err := columns.Scan(&tableName, &columnName, &dataType, &maxLength, &numericPrecision, &numericScale, &columnComment, &columnTypeLength, &isNullable, &columnDefault); err != nil {
			return nil, nil, fmt.Errorf("查询表属性 information_schema.columns 出错: %s", err.Error())
		}
		// 将MySQL数据类型映射为目标端数据类型和长度信息
		yasType, err := typedef.MysqlToYasType(dataType)
		if err != nil {
			return nil, nil, err
		}
		hasDefault := len(columnDefault) != 0
		var nullableStr, columnDefaultStr string
		switch yasType {
		case typedef.Y_VARCHAR, typedef.Y_CHAR, typedef.Y_NCHAR, typedef.Y_NVARCHAR:
			if maxLength.Valid {
				yasType = fmt.Sprintf(sqldef.Y_CHAR_FORMAT, yasType, maxLength.Int64)
			}
			columnDefaultStr = getDefaultStmt(yasType, columnDefault, hasDefault)
		case typedef.Y_INTEGER, typedef.Y_SMALLINT, typedef.Y_BIGINT:
			if columnTypeLength.Valid {
				if db.MysqlVersion != db.MYSQL_VERSION_8 {
					yasType = fmt.Sprintf(sqldef.Y_INT_FORMAT, yasType, columnTypeLength.String)
				}
			}
			columnDefaultStr = getDefaultStmt(yasType, columnDefault, hasDefault)
		case typedef.Y_FLOAT, typedef.Y_DOUBLE, typedef.Y_NUMBER:
			if numericPrecision.Valid && numericScale.Valid {
				if numericPrecision.Int64 > sqldef.Y_MAX_NUMERIC_PRECISION {
					numericPrecision.Int64 = sqldef.Y_MAX_NUMERIC_PRECISION
				}
				yasType = fmt.Sprintf(sqldef.Y_FLOAT_FORMAT, yasType, numericPrecision.Int64, numericScale.Int64)
			}
			columnDefaultStr = getDefaultStmt(yasType, columnDefault, hasDefault)
		case typedef.Y_BIT:
			if numericPrecision.Valid {
				yasType = fmt.Sprintf(sqldef.Y_BIT_FORMAT, yasType, numericPrecision.Int64)
			}
		case typedef.Y_RAW:
			if maxLength.Valid {
				yasType = fmt.Sprintf(sqldef.Y_RAW_FORMAT, yasType, maxLength.Int64)
			}
		default:
			columnDefaultStr = getDefaultStmt(yasType, columnDefault, hasDefault)
		}
		//构建not null的单独语句
		if isNullable == "NO" {
			// nullableStr = " not null"
			nullableStr = fmt.Sprintf(sqldef.Y_SQL_ALTER_COLUMN_NOT_NULL, yasdbSchema, tableName, columnName)
			nullableStrs = append(nullableStrs, nullableStr)
		}
		// 构建列语句
		columnStmt := fmt.Sprintf(sqldef.Y_SQL_COLUMN_STMT_FORMAT, columnName, yasType, columnDefaultStr)
		// 将列信息添加到对应的表
		tableColumns[tableName] = append(tableColumns[tableName], columnStmt)
		columnComment = strings.Replace(columnComment, "'", "''", -1)
		// 将列注释信息添加到map中
		columnComments[columnName] = columnComment
	}
	// 构建建表语句
	for tableName, columns := range tableColumns {
		createTableStmt := fmt.Sprintf(sqldef.Y_SQL_CREATE_TABLE, yasdbSchema, tableName, strings.Join(columns, ",\n\t"))
		tableDDL := fmt.Sprintln(createTableStmt)
		tableDDLs = append(tableDDLs, tableDDL)
	}
	for column, comment := range columnComments {
		if comment != "" {
			commentDDL := fmt.Sprintf(sqldef.Y_SQL_COLUMN_COMMENT_FORMAT, yasdbSchema, tableName, column, comment)
			tableDDLs = append(tableDDLs, commentDDL)
		}
	}
	return tableDDLs, nullableStrs, nil
}

func getTableAutoIncrementDDLs(mysql *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]string, error) {
	var ddls []string
	// 查询表的自增主键列信息
	rows, err := mysql.Query(sqldef.M_SQL_QUERY_AUTO_INCREMENT, mysqlSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询自增主键属性 information_schema.COLUMNS 出错: %s", err.Error())
	}
	defer rows.Close()

	// 存储自增主键列名信息
	var autoIncrementColumn string
	// 检查是否有错误发生
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("查询自增主键属性 information_schema.COLUMNS 出错: %s", err.Error())
	}
	// 遍历结果集
	for rows.Next() {
		err = rows.Scan(&autoIncrementColumn)
		if err != nil {
			return nil, fmt.Errorf("查询自增主键属性 information_schema.COLUMNS 出错: %s", err.Error())
		}
	}
	// 判断是否找到自增主键列
	if autoIncrementColumn != "" {
		maxidsql := fmt.Sprintf(sqldef.M_SQL_QUERY_MAX_ID, autoIncrementColumn, mysqlSchema, tableName)
		maxidrows, err := mysql.Query(maxidsql)
		if err != nil {
			return nil, fmt.Errorf("查询自增主键列的最大值出错 %v", err)
		}
		defer maxidrows.Close()

		// 存储自增主键列名信息
		var maxidvalue string

		// 遍历结果集
		for maxidrows.Next() {
			err = maxidrows.Scan(&maxidvalue)
			if err != nil {
				return nil, fmt.Errorf("查询自增主键列的最大值出错 %v", err)
			}
		}
		// 检查是否有错误发生
		if err = maxidrows.Err(); err != nil {
			return nil, fmt.Errorf("查询自增主键列的最大值出错 %v", err)
		}
		// 创建 YashanDB Sequence 的名称
		sequenceName := strings.ToUpper("SEQ_" + tableName + "_" + autoIncrementColumn)

		// 生成创建 YashanDB Sequence 的语句
		createSequenceSQL := fmt.Sprintf(sqldef.Y_SQL_CREATE_SEQUENCE_FORMAT, yasdbSchema, sequenceName, maxidvalue)
		// 生成设置列默认值的语句
		setDefaultValueSQL := fmt.Sprintf(sqldef.Y_SQL_SET_COLUMN_DEFAULT_VALUE_FORMAT, yasdbSchema, tableName, autoIncrementColumn, yasdbSchema, sequenceName)
		ddls = append(ddls, createSequenceSQL)
		ddls = append(ddls, setDefaultValueSQL)
	}
	return ddls, nil
}

func getTableComments(db *sql.DB, tableSchema, yasdbSchema, tableName string) ([]string, error) {
	var tablecomments []string
	rows, err := db.Query(sqldef.M_SQL_QUERY_TABLE_COMMENTS, tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableComment sql.NullString
		err := rows.Scan(&tableComment)
		if err != nil {
			return nil, err
		}
		if tableComment.String != "" {
			tablecomment := fmt.Sprintf(sqldef.Y_SQL_TABLE_COMMENT_FORMAT, yasdbSchema, tableName, tableComment.String)
			tablecomments = append(tablecomments, tablecomment)

		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return tablecomments, nil
}

func getDefaultStmt(yasType string, columnDefault string, hasDefault bool) (defaultStmt string) {
	if !hasDefault {
		return
	}
	switch yasType {
	case typedef.Y_INTEGER, typedef.Y_SMALLINT, typedef.Y_BIGINT, typedef.Y_FLOAT, typedef.Y_DOUBLE, typedef.Y_NUMBER, typedef.Y_BIT:
		defaultStmt = fmt.Sprintf(sqldef.Y_DEFAULT_NUMBER_FORMAT, columnDefault)
	case typedef.Y_TIMESTAMP:
		if columnDefault == sqldef.M_DEFAULT_COLUMN_CURRENT_TIMESTAMP {
			defaultStmt = fmt.Sprintf(sqldef.Y_DEFAULT_NUMBER_FORMAT, columnDefault)
		} else {
			defaultStmt = fmt.Sprintf(sqldef.Y_DEFAULT_STRING_FORMAT, columnDefault)
		}
	default:
		if columnDefault == sqldef.M_DEFAULT_COLUMN_NULL {
			defaultStmt = sqldef.Y_DEFAULT_NULL
		} else {
			defaultStmt = fmt.Sprintf(sqldef.Y_DEFAULT_STRING_FORMAT, columnDefault)
		}
	}
	return
}

func getPrimaryKeyDDLs(mysql *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]string, error) {
	var primarykeys []string
	indexes, err := getIndexes(mysql, mysqlSchema, yasdbSchema, tableName)
	if err != nil {
		return nil, err
	}
	// 以索引名称分组索引列
	indexMap := make(map[string][]string)
	for _, index := range indexes {
		if strings.ToUpper(index.KeyName) != "PRIMARY" { // 排除主键
			continue
		}
		indexMap[index.KeyName] = append(indexMap[index.KeyName], index.ColumnName)
	}
	// 生成创建索引的语句
	for _, columns := range indexMap {
		primarykey := fmt.Sprintf(sqldef.Y_SQL_ADD_PRIMARY_KEY, yasdbSchema, tableName, genColumnString(columns))
		primarykeys = append(primarykeys, primarykey)
	}
	return primarykeys, nil
}

func getUniqueIndexDDLs(mysql *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]string, error) {
	var ddls []string
	indexes, err := getIndexes(mysql, mysqlSchema, yasdbSchema, tableName)
	if err != nil {
		return nil, err
	}
	indexMap := make(map[string][]string)
	for _, index := range indexes {
		if index.KeyName == "PRIMARY" || index.NonUnique != 0 {
			continue
		}
		indexMap[index.KeyName] = append(indexMap[index.KeyName], index.ColumnName)
	}

	// 生成创建索引的语句
	for _, columns := range indexMap {
		columnString := genColumnString(columns)
		columnStringName := strings.Join(columns, "_")
		indexName := "idx_" + tableName + "_" + columnStringName
		if len(indexName) > 64 {
			indexName = indexName[0:64]
		}
		ddls = append(ddls, fmt.Sprintf(sqldef.Y_SQL_CREATE_UNIQUE_INDEX, yasdbSchema, indexName, yasdbSchema, tableName, columnString))
		ddls = append(ddls, fmt.Sprintf(sqldef.Y_SQL_ADD_UNIQUE_CONSTRAINT, yasdbSchema, tableName, indexName, columnString))
	}
	return ddls, nil
}

func getNonUniqueIndexDDL(mysql *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]string, error) {
	var ddls []string
	indexes, err := getIndexes(mysql, mysqlSchema, yasdbSchema, tableName)
	if err != nil {
		return nil, err
	}
	// 以索引名称分组索引列
	indexMap := make(map[string][]string)
	for _, index := range indexes {
		if index.KeyName == "PRIMARY" || index.NonUnique != 1 { // 排除主键和唯一索引
			continue
		}
		indexMap[index.KeyName] = append(indexMap[index.KeyName], index.ColumnName)
	}
	// 生成创建索引的语句
	for _, columns := range indexMap {
		columnString := genColumnString(columns)
		columnStringName := strings.Join(columns, "_")
		indexName := "idx_" + tableName + "_" + columnStringName
		if len(indexName) > 64 {
			indexName = indexName[0:64]
		}
		ddls = append(ddls, fmt.Sprintf(sqldef.Y_SQL_CREATE_INDEX, yasdbSchema, indexName, yasdbSchema, tableName, columnString))
	}
	return ddls, nil
}

func getTableForeignKeys(db *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]string, error) {
	rows, err := db.Query(sqldef.M_SQL_QUERY_FOREIGN_KEY, mysqlSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询外键信息 information_schema.key_column_usage 出错: %v", err)
	}
	defer rows.Close()

	var constraints []string
	for rows.Next() {
		var constraintName, columnName, referencedTableName, referencedColumnName sql.NullString
		err := rows.Scan(&constraintName, &columnName, &referencedTableName, &referencedColumnName)
		if err != nil {
			return nil, err
		}
		constraint := fmt.Sprintf(
			sqldef.Y_SQL_ADD_FOREIGN_KEY,
			yasdbSchema,
			tableName,
			constraintName.String,
			columnName.String,
			yasdbSchema,
			referencedTableName.String,
			referencedColumnName.String,
		)
		constraints = append(constraints, constraint)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return constraints, nil
}

func getViewDDLs(db *sql.DB, mysqlSchema, yasdbSchema string) ([]string, error) {
	var viewDDLs []string
	rows, err := db.Query(fmt.Sprintf(sqldef.M_SQL_QUERY_VIEW, mysqlSchema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var viewDDL, viewName string
		if err := rows.Scan(&viewName, &viewDDL); err != nil {
			return nil, err
		}
		viewDDL = strings.ReplaceAll(viewDDL, "`"+mysqlSchema+"`.", "")
		viewDDL = strings.ReplaceAll(viewDDL, mysqlSchema+".", "")
		viewDDL = strings.ReplaceAll(viewDDL, "`", "\"")
		if len(strings.TrimSpace(viewDDL)) == 0 {
			continue
		}
		viewDDL = fmt.Sprintf(sqldef.Y_SQL_CREATE_VIEW, yasdbSchema, viewName, viewDDL)
		viewDDLs = append(viewDDLs, viewDDL)
	}
	return viewDDLs, nil
}

func genColumnString(columns []string) string {
	var newColumns []string
	for _, column := range columns {
		newColumns = append(newColumns, fmt.Sprintf("\"%s\"", column))
	}
	return strings.Join(newColumns, ", ")
}

func getIndexes(mysql *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]Index, error) {
	switch db.MysqlVersion {
	case db.MYSQL_VERSION_5:
		return getIndexes5(mysql, mysqlSchema, yasdbSchema, tableName)
	case db.MYSQL_VERSION_8:
		return getIndexes8(mysql, mysqlSchema, yasdbSchema, tableName)
	default:
		return nil, nil
	}
}

func getIndexes8(mysql *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]Index, error) {
	if db.MysqlVersion != db.MYSQL_VERSION_8 {
		return nil, nil
	}
	// 执行SHOW INDEXES查询
	rows, err := mysql.Query(fmt.Sprintf(sqldef.M_SQL_SHOW_INDEX, mysqlSchema, tableName))
	if err != nil {
		return nil, fmt.Errorf("查询索引属性SHOW INDEXES FROM %s.%s出错: %v", mysqlSchema, tableName, err)
	}
	defer rows.Close()

	var (
		table         string
		nonUnique     int
		keyName       string
		seqInIndex    int
		columnName    string
		Collation     sql.NullString
		Cardinality   sql.NullString
		Sub_part      sql.NullString
		Packed        sql.NullString
		Null          sql.NullString
		indexType     string
		Comment       sql.NullString
		Index_comment sql.NullString
		Visible       string
		Expression    sql.NullString
	)
	var indexes []Index
	// 解析查询结果
	for rows.Next() {
		err = rows.Scan(&table, &nonUnique, &keyName, &seqInIndex, &columnName, &Collation, &Cardinality, &Sub_part, &Packed, &indexType, &Null, &Comment, &Index_comment, &Visible, &Expression)
		if err != nil {
			return nil, fmt.Errorf("查询索引属性SHOW INDEXES FROM %s.%s出错: %v", mysqlSchema, tableName, err)
		}
		index := Index{
			Table:      table,
			NonUnique:  nonUnique,
			KeyName:    keyName,
			ColumnName: columnName,
			IndexType:  indexType,
			SeqInIndex: seqInIndex,
		}
		indexes = append(indexes, index)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("查询索引属性SHOW INDEXES FROM %s.%s出错: %v", mysqlSchema, tableName, err)
	}
	return indexes, nil
}

func getIndexes5(mysql *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]Index, error) {
	if db.MysqlVersion != db.MYSQL_VERSION_5 {
		return nil, nil
	}
	// 执行SHOW INDEXES查询
	rows, err := mysql.Query(fmt.Sprintf(sqldef.M_SQL_SHOW_INDEX, mysqlSchema, tableName))
	if err != nil {
		return nil, fmt.Errorf("查询索引属性SHOW INDEXES FROM %s.%s出错: %v", mysqlSchema, tableName, err)
	}
	defer rows.Close()

	var (
		table        string
		nonUnique    int
		keyName      string
		seqInIndex   int
		columnName   string
		collation    sql.NullString
		cardinality  sql.NullString
		subPart      sql.NullString
		packed       sql.NullString
		null         sql.NullString
		indexType    string
		comment      sql.NullString
		indexComment sql.NullString
	)
	var indexes []Index
	// 解析查询结果
	for rows.Next() {
		err = rows.Scan(&table, &nonUnique, &keyName, &seqInIndex, &columnName, &collation, &cardinality, &subPart, &packed, &indexType, &null, &comment, &indexComment)
		if err != nil {
			return nil, fmt.Errorf("查询索引属性SHOW INDEXES FROM %s.%s出错: %v", mysqlSchema, tableName, err)
		}
		index := Index{
			Table:      table,
			NonUnique:  nonUnique,
			KeyName:    keyName,
			ColumnName: columnName,
			IndexType:  indexType,
			SeqInIndex: seqInIndex,
		}
		indexes = append(indexes, index)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("查询索引属性SHOW INDEXES FROM %s.%s出错: %v", mysqlSchema, tableName, err)
	}
	return indexes, nil
}

func getMysqlAllDbs(mysql *sql.DB) ([]string, error) {
	var dbs []string
	// 查询数据库信息
	rows, err := mysql.Query(sqldef.M_SQL_SHOW_DATABASES)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 遍历结果
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, err
		}
		dbs = append(dbs, dbName)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return dbs, nil
}

func getMysqlSchemaTables(mysql *sql.DB, mysqlSchema string) ([]string, error) {
	var tables []string
	rows, err := mysql.Query(sqldef.M_SQL_QUERY_TABLES, mysqlSchema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// 遍历结果
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func inArrayStr(target string, arr []string) bool {
	for _, value := range arr {
		if value == target {
			return true
		}
	}
	return false
}
