package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"m2y/db"
	"m2y/define/confdef"
	"m2y/define/sqldef"
	"m2y/define/typedef"
	"m2y/log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	_ "git.yasdb.com/go/yasdb-go"
	"git.yasdb.com/go/yasutil/fs"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
)

const (
	tables_ddl = "tables_ddl"
	others_ddl = "others_ddl"
)

var (
	version = "${version}"
)

type Index struct {
	Table      string
	NonUnique  int
	KeyName    string
	ColumnName string
	IndexType  string
	SeqInIndex int
}

func get_non_uniq_index_ddl(mysqlDB *sql.DB, table_schema, yasdb_schema, table_name string) ([]string, error) {
	var nonuniqindexes []string
	sql_str := fmt.Sprintf("SHOW INDEXES FROM %s.`%s`", table_schema, table_name)
	// 执行SHOW INDEXES查询
	rows, err := mysqlDB.Query(sql_str)
	if err != nil {
		log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错", err)
		return nil, err
	}
	defer rows.Close()

	if db.MysqlVersion == "8" {
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
				log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错 %v", err)
				return nil, err
			}

			index := Index{
				Table:      table,
				NonUnique:  nonUnique,
				KeyName:    keyName,
				ColumnName: columnName,
				IndexType:  indexType,
				SeqInIndex: seqInIndex,
			}
			if keyName != "PRIMARY" && nonUnique == 1 { // 排除主键和唯一索引
				indexes = append(indexes, index)
			}
			//indexes = append(indexes, index)
		}
		if err = rows.Err(); err != nil {
			log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错 %v", err)
			return nil, err
		}

		// 以索引名称分组索引列
		indexMap := make(map[string][]string)
		for _, index := range indexes {
			indexMap[index.KeyName] = append(indexMap[index.KeyName], index.ColumnName)
		}

		// 生成创建索引的语句
		for _, columns := range indexMap {
			columnString := strings.Join(columns, ", ")
			columnStringName := strings.Join(columns, "_")
			index_name := "idx_" + table + "_" + columnStringName
			if len(index_name) > 64 {
				index_name = index_name[0:64]
			}
			nonuniqindex := fmt.Sprintf("CREATE INDEX %s.%s ON %s.%s (%s);\n", yasdb_schema, index_name, yasdb_schema, table, columnString)
			nonuniqindexes = append(nonuniqindexes, nonuniqindex)
		}
		if err = rows.Err(); err != nil {
			return nil, err
		}

	} else if db.MysqlVersion == "5" {
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
		)

		var indexes []Index

		// 解析查询结果
		for rows.Next() {
			err = rows.Scan(&table, &nonUnique, &keyName, &seqInIndex, &columnName, &Collation, &Cardinality, &Sub_part, &Packed, &indexType, &Null, &Comment, &Index_comment)
			if err != nil {
				log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错 %v", err)
				return nil, err
			}

			index := Index{
				Table:      table,
				NonUnique:  nonUnique,
				KeyName:    keyName,
				ColumnName: columnName,
				IndexType:  indexType,
				SeqInIndex: seqInIndex,
			}
			if keyName != "PRIMARY" && nonUnique == 1 { // 排除主键和唯一索引
				indexes = append(indexes, index)
			}
			//indexes = append(indexes, index)
		}
		if err = rows.Err(); err != nil {
			log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错 %v", err)
			return nil, err
		}

		// 以索引名称分组索引列
		indexMap := make(map[string][]string)
		for _, index := range indexes {
			indexMap[index.KeyName] = append(indexMap[index.KeyName], index.ColumnName)
		}

		// 生成创建索引的语句
		for _, columns := range indexMap {
			columnString := strings.Join(columns, ", ")
			columnStringName := strings.Join(columns, "_")
			index_name := "idx_" + table + "_" + columnStringName
			nonuniqindex := fmt.Sprintf("CREATE INDEX %s.%s ON %s.%s (%s);\n", yasdb_schema, index_name, yasdb_schema, table, columnString)
			nonuniqindexes = append(nonuniqindexes, nonuniqindex)
		}
		if err = rows.Err(); err != nil {
			return nil, err
		}
	}

	return nonuniqindexes, nil
}

func get_uniq_index_ddl(mysqlDB *sql.DB, table_schema, yasdb_schema, table_name string) ([]string, error) {
	var uniqindexes []string
	// 连接到MySQL数据库

	sql_str := fmt.Sprintf("SHOW INDEXES FROM %s.`%s`", table_schema, table_name)
	// 执行SHOW INDEXES查询
	rows, err := mysqlDB.Query(sql_str)
	if err != nil {
		log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错: %s", err)
		return nil, err
	}
	defer rows.Close()

	if db.MysqlVersion == "8" {
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
				log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错: %s", err)
				return nil, err
			}

			index := Index{
				Table:      table,
				NonUnique:  nonUnique,
				KeyName:    keyName,
				ColumnName: columnName,
				IndexType:  indexType,
				SeqInIndex: seqInIndex,
			}
			if keyName != "PRIMARY" && nonUnique == 0 { // 排除主键,保留唯一索引
				indexes = append(indexes, index)
			}
			//indexes = append(indexes, index)
		}

		if err = rows.Err(); err != nil {
			log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错: %s", err)
			return nil, err
		}

		// 以索引名称分组索引列
		indexMap := make(map[string][]string)
		for _, index := range indexes {
			indexMap[index.KeyName] = append(indexMap[index.KeyName], index.ColumnName)
		}

		// 生成创建索引的语句
		for _, columns := range indexMap {
			columnString := strings.Join(columns, ", ")
			columnStringName := strings.Join(columns, "_")
			index_name := "idx_" + table + "_" + columnStringName
			if len(index_name) > 64 {
				index_name = index_name[0:64]
			}
			uniqindex := fmt.Sprintf("CREATE UNIQUE INDEX %s.%s ON %s.%s (%s);\n", yasdb_schema, index_name, yasdb_schema, table, columnString)
			uniqcons := fmt.Sprintf("ALTER TABLE  %s.%s ADD CONSTRAINT %s UNIQUE (%s);\n", yasdb_schema, table, index_name, columnString)
			uniqindexes = append(uniqindexes, uniqindex)
			uniqindexes = append(uniqindexes, uniqcons)

		}
		if err = rows.Err(); err != nil {
			return nil, err
		}
	} else if db.MysqlVersion == "5" {
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
		)

		var indexes []Index

		// 解析查询结果
		for rows.Next() {
			err = rows.Scan(&table, &nonUnique, &keyName, &seqInIndex, &columnName, &Collation, &Cardinality, &Sub_part, &Packed, &indexType, &Null, &Comment, &Index_comment)
			if err != nil {
				log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错: %v", err)
				return nil, err
			}

			index := Index{
				Table:      table,
				NonUnique:  nonUnique,
				KeyName:    keyName,
				ColumnName: columnName,
				IndexType:  indexType,
				SeqInIndex: seqInIndex,
			}
			if keyName != "PRIMARY" && nonUnique == 0 { // 排除主键,保留唯一索引
				indexes = append(indexes, index)
			}
			//indexes = append(indexes, index)
		}

		if err = rows.Err(); err != nil {
			log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错: %v", err)
			return nil, err
		}

		// 以索引名称分组索引列
		indexMap := make(map[string][]string)
		for _, index := range indexes {
			indexMap[index.KeyName] = append(indexMap[index.KeyName], index.ColumnName)
		}

		// 生成创建索引的语句
		for _, columns := range indexMap {
			columnString := strings.Join(columns, ", ")
			columnStringName := strings.Join(columns, "_")
			index_name := "idx_" + table + "_" + columnStringName
			if len(index_name) > 64 {
				index_name = index_name[0:64]
			}
			uniqindex := fmt.Sprintf("CREATE UNIQUE INDEX %s.%s ON %s.%s (%s);\n", yasdb_schema, index_name, yasdb_schema, table, columnString)
			uniqcons := fmt.Sprintf("ALTER TABLE  %s.%s ADD CONSTRAINT %s UNIQUE (%s);\n", yasdb_schema, table, index_name, columnString)
			uniqindexes = append(uniqindexes, uniqindex)
			uniqindexes = append(uniqindexes, uniqcons)

		}
		if err = rows.Err(); err != nil {
			return nil, err
		}
	}

	return uniqindexes, nil
}

func get_primary_key_ddl(mysqlDB *sql.DB, table_schema, yasdb_schema, table_name string) ([]string, error) {
	var primarykeys []string

	// 执行SHOW INDEXES查询
	sql_str := fmt.Sprintf("SHOW INDEXES FROM %s.`%s`", table_schema, table_name)
	rows, err := mysqlDB.Query(sql_str)
	if err != nil {
		log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错: %v", err)
		return nil, err
	}
	defer rows.Close()
	if db.MysqlVersion == "8" {

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
				log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错: %s", err)
				return nil, err
			}

			index := Index{
				Table:      table,
				NonUnique:  nonUnique,
				KeyName:    keyName,
				ColumnName: columnName,
				IndexType:  indexType,
				SeqInIndex: seqInIndex,
			}
			if keyName == "PRIMARY" { // 只要主键
				indexes = append(indexes, index)
			}
			//indexes = append(indexes, index)
		}

		if err = rows.Err(); err != nil {
			log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错: %v", err)
			return nil, err
		}

		// 以索引名称分组索引列
		indexMap := make(map[string][]string)
		for _, index := range indexes {
			indexMap[index.KeyName] = append(indexMap[index.KeyName], index.ColumnName)
		}

		// 生成创建索引的语句
		for _, columns := range indexMap {
			columnString := strings.Join(columns, ", ")
			//columnStringName := strings.Join(columns, "_")
			//index_name := "idx_" + table + "_" + columnStringName
			primarykey := fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY (%s);\n", yasdb_schema, table, columnString)
			primarykeys = append(primarykeys, primarykey)
		}
		if err = rows.Err(); err != nil {
			return nil, err
		}

	} else if db.MysqlVersion == "5" {

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
		)

		var indexes []Index

		// 解析查询结果
		for rows.Next() {
			err = rows.Scan(&table, &nonUnique, &keyName, &seqInIndex, &columnName, &Collation, &Cardinality, &Sub_part, &Packed, &indexType, &Null, &Comment, &Index_comment)
			if err != nil {
				log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错: %v", err)
				return nil, err
			}

			index := Index{
				Table:      table,
				NonUnique:  nonUnique,
				KeyName:    keyName,
				ColumnName: columnName,
				IndexType:  indexType,
				SeqInIndex: seqInIndex,
			}
			if keyName == "PRIMARY" { // 只要主键
				indexes = append(indexes, index)
			}
			//indexes = append(indexes, index)
		}

		if err = rows.Err(); err != nil {
			log.ConsoleSugar.Errorf("查询索引属性SHOW INDEXES FROM xx.xx出错: %v", err)
			return nil, err
		}

		// 以索引名称分组索引列
		indexMap := make(map[string][]string)
		for _, index := range indexes {
			indexMap[index.KeyName] = append(indexMap[index.KeyName], index.ColumnName)
		}

		// 生成创建索引的语句
		for _, columns := range indexMap {
			columnString := strings.Join(columns, ", ")
			//columnStringName := strings.Join(columns, "_")
			//index_name := "idx_" + table + "_" + columnStringName
			primarykey := fmt.Sprintf("ALTER  TABLE %s.%s ADD PRIMARY KEY (%s);\n", yasdb_schema, table, columnString)
			primarykeys = append(primarykeys, primarykey)
		}
		if err = rows.Err(); err != nil {
			return nil, err
		}

	}

	return primarykeys, nil
}

func get_table_ddl(mysqlDB *sql.DB, table_schema, yasdb_schema, table_name string) ([]string, []string, error) {
	var tableddls, nullable_strs []string
	// 查询表的列信息
	columns, err := mysqlDB.Query(sqldef.SQL_QUERY_MYSQL_COLUMNS, table_schema, table_name)
	if err != nil {
		log.ConsoleSugar.Errorf("查询表属性,information_schema.columns出错: %s", err.Error())
		return nil, nil, err
	}
	defer columns.Close()

	// 存储表名和列信息的映射关系
	tableColumns := make(map[string][]string)
	// 存储列注释信息
	columnComments := make(map[string]string)

	// 遍历列信息结果
	for columns.Next() {
		var (
			tableName, columnName, columnComment, dataType, is_nullable, column_default string
			maxLength, numericPrecision, numericScale                                   sql.NullInt64
			columnTypeLength                                                            sql.NullString
		)
		if err := columns.Scan(&tableName, &columnName, &dataType, &maxLength, &numericPrecision, &numericScale, &columnComment, &columnTypeLength, &is_nullable, &column_default); err != nil {
			log.ConsoleSugar.Errorf("查询表属性,information_schema.columns出错: %v", err)
			return nil, nil, err
		}

		// 将MySQL数据类型映射为目标端数据类型和长度信息
		yasType, err := typedef.MysqlToYasType(dataType)
		if err != nil {
			log.ConsoleSugar.Error(err)
			return nil, nil, err
		}
		hasDefault := len(column_default) != 0
		var nullable_str, column_default_str string
		switch yasType {
		case typedef.Y_VARCHAR, typedef.Y_CHAR, typedef.Y_NCHAR, typedef.Y_NVARCHAR:
			if maxLength.Valid {
				yasType = fmt.Sprintf("%s(%d char)", yasType, maxLength.Int64)
			}
			column_default_str = getDefaultStmt(yasType, column_default, hasDefault)
		case typedef.Y_INTEGER, typedef.Y_SMALLINT, typedef.Y_BIGINT:
			if columnTypeLength.Valid {
				if db.MysqlVersion != "8" {
					yasType = fmt.Sprintf("%s(%s)", yasType, columnTypeLength.String)
				}
			}
			column_default_str = getDefaultStmt(yasType, column_default, hasDefault)
		case typedef.Y_FLOAT, typedef.Y_DOUBLE, typedef.Y_NUMBER:
			if numericPrecision.Valid && numericScale.Valid {
				if numericPrecision.Int64 > 38 {
					numericPrecision.Int64 = 38
				}
				yasType = fmt.Sprintf("%s(%d, %d)", yasType, numericPrecision.Int64, numericScale.Int64)
			}
			column_default_str = getDefaultStmt(yasType, column_default, hasDefault)
		case typedef.Y_TIMESTAMP:
			if numericPrecision.Valid && numericScale.Valid {
				yasType = fmt.Sprintf("%s", yasType)
			}
			column_default_str = getDefaultStmt(yasType, column_default, hasDefault)
		case typedef.Y_BIT:
			if numericPrecision.Valid {
				yasType = fmt.Sprintf("%s(%d)", yasType, numericPrecision.Int64)
			}
		case typedef.Y_RAW:
			if maxLength.Valid {
				yasType = fmt.Sprintf("%s(%d)", yasType, maxLength.Int64)
			}
		default:
			column_default_str = getDefaultStmt(yasType, column_default, hasDefault)
		}

		//构建not null的单独语句
		if is_nullable == "NO" {
			// nullable_str = " not null"
			nullable_str = fmt.Sprintf(sqldef.SQL_ALTER_COLUMN_NOT_NULL, yasdb_schema, table_name, columnName)
			nullable_strs = append(nullable_strs, nullable_str)
		}

		// 构建列语句
		columnStmt := fmt.Sprintf("%s %s%s", columnName, yasType, column_default_str)

		// 将列信息添加到对应的表
		tableColumns[tableName] = append(tableColumns[tableName], columnStmt)
		columnComment = strings.Replace(columnComment, "'", "''", -1)
		// 将列注释信息添加到map中
		columnComments[columnName] = columnComment
	}

	// 构建建表语句
	for tableName, columns := range tableColumns {
		createTableStmt := fmt.Sprintf(sqldef.SQL_CREATE_TABLE, yasdb_schema, tableName, strings.Join(columns, ",\n\t"))
		tablesddl := fmt.Sprintln(createTableStmt)
		tableddls = append(tableddls, tablesddl)
	}
	for column, comment := range columnComments {
		if comment != "" {
			commentddl := fmt.Sprintf("COMMENT ON COLUMN %s.%s.%s IS '%s';\n", yasdb_schema, table_name, column, comment)
			tableddls = append(tableddls, commentddl)
		}

	}

	// 查询表的自增主键列信息
	rows, err := mysqlDB.Query(sqldef.SQL_QUERY_AUTO_INCREMENT, table_schema, table_name)
	if err != nil {
		log.ConsoleSugar.Errorf("查询自增主键属性information_schema.COLUMNS出错: %s", err.Error())
		return nil, nil, err
	}
	defer rows.Close()

	// 存储自增主键列名信息
	var autoIncrementColumn string
	// 检查是否有错误发生
	err = rows.Err()
	if err != nil {
		log.ConsoleSugar.Errorf("查询自增主键属性information_schema.COLUMNS出错: %s", err.Error())
	}
	// 遍历结果集
	for rows.Next() {
		err = rows.Scan(&autoIncrementColumn)
		if err != nil {
			log.ConsoleSugar.Errorf("查询自增主键属性information_schema.COLUMNS出错: %s", err.Error())
			return nil, nil, err
		}
	}
	// 判断是否找到自增主键列
	if autoIncrementColumn != "" {
		maxidsql := fmt.Sprintf(`SELECT ifnull(max(%s),0)+1 FROM %s.%s`, autoIncrementColumn, table_schema, table_name)
		maxidrows, err := mysqlDB.Query(maxidsql)
		if err != nil {
			log.ConsoleSugar.Errorf("查询自增主键列的最大值出错 %v", err)
			return nil, nil, err
		}
		defer maxidrows.Close()

		// 存储自增主键列名信息
		var maxidvalue string

		// 遍历结果集
		for maxidrows.Next() {
			err = maxidrows.Scan(&maxidvalue)
			if err != nil {
				log.ConsoleSugar.Errorf("查询自增主键列的最大值出错: %v", err)
				return nil, nil, err
			}
		}
		// 检查是否有错误发生
		err = maxidrows.Err()
		if err != nil {
			log.ConsoleSugar.Errorf("查询自增主键列的最大值出错: %v", err)
			return nil, nil, err
		}
		// 创建 YashanDB Sequence 的名称
		sequenceName := strings.ToUpper("SEQ_" + table_name + "_" + autoIncrementColumn)

		// 生成创建 YashanDB Sequence 的语句
		createSequenceSQL := fmt.Sprintf("CREATE SEQUENCE %s.%s START WITH %s INCREMENT BY 1;\n", yasdb_schema, sequenceName, maxidvalue)

		// 生成设置列默认值的语句
		setDefaultValueSQL := fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s DEFAULT %s.%s.NEXTVAL;\n", yasdb_schema, table_name, autoIncrementColumn, yasdb_schema, sequenceName)
		tableddls = append(tableddls, createSequenceSQL)
		tableddls = append(tableddls, setDefaultValueSQL)
	}
	return tableddls, nullable_strs, nil
}

func getDefaultStmt(yasType string, columnDefault string, hasDefault bool) (defaultStmt string) {
	if !hasDefault {
		return
	}
	numberFormat := " default %s"
	strFormat := " default '%s'"
	switch yasType {
	case typedef.Y_INTEGER, typedef.Y_SMALLINT, typedef.Y_BIGINT, typedef.Y_FLOAT, typedef.Y_DOUBLE, typedef.Y_NUMBER, typedef.Y_BIT:
		defaultStmt = fmt.Sprintf(numberFormat, columnDefault)
	case typedef.Y_TIMESTAMP:
		if columnDefault == "CURRENT_TIMESTAMP" {
			defaultStmt = fmt.Sprintf(numberFormat, columnDefault)
		} else {
			defaultStmt = fmt.Sprintf(strFormat, columnDefault)
		}
	default:
		if columnDefault == "\x00" {
			defaultStmt = " default NULL"
		} else {
			defaultStmt = fmt.Sprintf(strFormat, columnDefault)
		}
	}
	return
}

func inArrayStr(target string, arr []string) bool {
	for _, value := range arr {
		if value == target {
			return true
		}
	}
	return false
}

func getViewDDLs(db *sql.DB, schemaName, yasdb_schema string) ([]string, error) {
	var view_ddls []string
	var view_ddl string
	var view_name string
	rows, err := db.Query(fmt.Sprintf("SELECT TABLE_NAME,VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '%s'", schemaName))
	if err != nil {
		log.ConsoleSugar.Errorf("查询视图信息INFORMATION_SCHEMA.VIEWS出错: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&view_name, &view_ddl); err != nil {
			log.ConsoleSugar.Errorf("查询视图信息INFORMATION_SCHEMA.VIEWS出错: %v", err)
			return nil, err
		}
		view_ddl = strings.ReplaceAll(view_ddl, "`", "")
		view_ddl = strings.ReplaceAll(view_ddl, schemaName+".", "")
		view_ddl = fmt.Sprint("CREATE VIEW ", yasdb_schema, ".", view_name, " AS ", view_ddl, ";\n")

		view_ddls = append(view_ddls, view_ddl)
	}
	return view_ddls, nil
}

func getTriggerSQL(db *sql.DB, triggerSchema, yasdb_schema string) ([]string, error) {
	existingTriggerSQL := []string{}

	rows, err := db.Query(`SELECT 
				TRIGGER_NAME, 
				ACTION_TIMING,
				ACTION_STATEMENT, 
				EVENT_MANIPULATION, 
				EVENT_OBJECT_TABLE 
				FROM INFORMATION_SCHEMA.TRIGGERS 
				WHERE TRIGGER_SCHEMA = ?
				and EVENT_OBJECT_SCHEMA = ?
				and ACTION_ORIENTATION = 'ROW'
				`, triggerSchema, triggerSchema)
	if err != nil {
		log.ConsoleSugar.Errorf("查询触发器信息INFORMATION_SCHEMA.TRIGGERS出错", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var triggerName, actionTiming, actionStatement, eventManipulation, eventObjectTable string
		if err := rows.Scan(&triggerName, &actionTiming, &actionStatement, &eventManipulation, &eventObjectTable); err != nil {
			log.ConsoleSugar.Errorf("查询触发器信息INFORMATION_SCHEMA.TRIGGERS出错", err)
			return nil, err
		}

		triggerSQL := fmt.Sprintf("CREATE TRIGGER %s.%s %s %s ON %s.%s FOR EACH ROW %s;\n/\n", yasdb_schema, triggerName, actionTiming, eventManipulation, yasdb_schema, eventObjectTable, actionStatement)
		// fmt.Println(triggerSQL)
		existingTriggerSQL = append(existingTriggerSQL, triggerSQL)
	}

	return existingTriggerSQL, nil
}

func getTableForeignKeys(db *sql.DB, tableSchema, yasdb_schema, tableName string) ([]string, error) {
	var constraints []string

	rows, err := db.Query(`
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
	group by constraint_name,referenced_table_name
	`, tableSchema, tableName)
	if err != nil {
		log.ConsoleSugar.Errorf("查询外键信息information_schema.key_column_usage出错: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var constraintName, columnName, referencedTableName, referencedColumnName sql.NullString
		err := rows.Scan(&constraintName, &columnName, &referencedTableName, &referencedColumnName)
		if err != nil {
			return nil, err
		}
		constraint := fmt.Sprintf(
			"ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s(%s);\n",
			yasdb_schema,
			tableName,
			constraintName.String,
			columnName.String,
			yasdb_schema,
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

func getTableComments(db *sql.DB, tableSchema, yasdb_schema, tableName string) ([]string, error) {
	var tablecomments []string

	rows, err := db.Query(`
		SELECT
			table_comment
		FROM
			information_schema.tables
		WHERE
			table_schema = ?
			AND table_name = ?
			and table_type = 'BASE TABLE'
	`, tableSchema, tableName)
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
			tablecomment := fmt.Sprintf(
				"COMMENT ON TABLE %s.%s IS '%s' ;\n",
				yasdb_schema,
				tableName,
				tableComment.String,
			)
			tablecomments = append(tablecomments, tablecomment)

		}

	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return tablecomments, nil
}

func printHelp() {
	const helpText = `
	全局选项:
	-h, --help     显示帮助信息
	-v, --version  显示程序版本号
	-c, --config   指定DB配置信息文件
	-d, --data     仅同步表数据,此参数开启时,不生成ddl文件

	用法示例1:     直接执行,使用当前目录下的db.ini配置文件获取程序运行时的配置信息,导出对象ddl
	./mysql2yasdb 

	用法示例2:     使用自定义配置文件xxx.ini,导出对象ddl
	./mysql2yasdb -c xxx.ini   或 ./mysql2yasdb --config=xxx.ini

	用法示例3:     使用当前目录下的db.ini配置文件,并进行表数据的同步,但不生成ddl文件
	./mysql2yasdb -d
	`
	fmt.Println(helpText)
}

func printVersion() {
	fmt.Printf("版本号: %s\n", string(version))
}

type schema_table struct {
	schema       string
	remap_schema string
	table        string
}

func deal_schemas_data(mysqlDb, yasdDb *sql.DB, table_schemas, table_remap_schemas []string, excludeTables []string) {
	// 查询表的信息
	mysqDbs := getMysqlAllDbs(mysqlDb)

	sts := []schema_table{}
	var st schema_table

	for i, schema := range table_schemas {
		row, err := mysqlDb.Query(`select table_name 
							from information_schema.TABLES 
							where table_schema=?  and table_type = 'BASE TABLE';`, schema)
		if err != nil {
			log.ConsoleSugar.Errorf("查询表的信息information_schema.TABLES出错 %s", err)
		}
		defer row.Close()

		if !inArrayStr(schema, mysqDbs) {
			log.ConsoleSugar.Errorf("Mysql Database %s 不存在,请检查配置文件或Mysql环境\n", schema)
			continue
		}

		for row.Next() {
			var tableName string
			if err := row.Scan(&tableName); err != nil {
				log.ConsoleSugar.Errorf("查询表的信息information_schema.TABLES出错 %v", err)
			}
			if inArrayStr(tableName, excludeTables) {
				continue
			}
			st.schema = schema
			st.table = tableName
			st.remap_schema = table_remap_schemas[i]
			sts = append(sts, st)
			// sync_from_mysql_to_yasdb(mysqlDb, yasdb, schema, table_remap_schemas[i], tableName, tableName)
		}
	}

	taskCount := len(sts)
	start := time.Now() // 记录开始时间

	// 创建一个带有缓冲区的通道，用于控制并发数量
	semaphore := make(chan bool, confdef.GetM2yConfig().Mysql.Parallel)
	// 创建一个等待组，用于等待所有goroutine完成
	var wg sync.WaitGroup

	for i := 0; i < taskCount; i++ {

		wg.Add(1)

		// 在每次循环开始前获取一个信号量
		semaphore <- true
		go func(mysdb, yasdDb *sql.DB, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable string) {
			sync_table_date_from_mysql_to_yasdb(mysdb, yasdDb, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable)
			// 任务完成后释放信号量
			<-semaphore
			wg.Done()

		}(mysqlDb, yasdDb, sts[i].schema, sts[i].remap_schema, sts[i].table, sts[i].table)
	}

	// 等待所有goroutine完成
	wg.Wait()
	elapsed := time.Since(start) // 计算经过的时间
	log.ConsoleSugar.Infof("任务完成,共耗时: %v", elapsed)
}

func deal_schemas_ddl(mysqlDB *sql.DB, table_schemas, table_remap_schemas []string, excludeTables []string) {
	// 查询表的信息

	mysqDbs := getMysqlAllDbs(mysqlDB)

	for i, schema := range table_schemas {

		if !inArrayStr(schema, mysqDbs) {
			log.ConsoleSugar.Errorf("Mysql Database %s 不存在,请检查配置文件或Mysql环境\n", schema)
			continue
		}

		yasdb_schema := table_remap_schemas[i]

		// data := "Hello, World!"
		tab_filename := fmt.Sprintf("%s_tables.sql", schema)
		idx_filename := fmt.Sprintf("%s_others.sql", schema)

		table_file, err := os.Create(tab_filename)
		if err != nil {
			log.ConsoleSugar.Errorf("Failed to create file: %v", err)
			return
		}
		defer table_file.Close()
		idx_file, err := os.Create(idx_filename)
		if err != nil {
			log.ConsoleSugar.Errorf("Failed to create file: %v", err)
			return
		}
		defer idx_file.Close()

		// 处理 &转义问题
		define_str := "SET DEFINE OFF;\n"
		_, err = table_file.WriteString(define_str)

		msg_tab := "--创建数据库内的表,列默认值,自增序列,列注释\n"
		_, err = table_file.WriteString(msg_tab)

		nullable_idx := "--创建表的非空约束语句\n"
		_, err = idx_file.WriteString(nullable_idx)

		tables, err := mysqlDB.Query(`select table_name 
							from information_schema.TABLES 
							where table_schema=? and table_type = 'BASE TABLE';`, schema)
		if err != nil {
			log.ConsoleSugar.Errorf("查询表的信息information_schema.TABLES出错: %v", err)
		}
		defer tables.Close()
		for tables.Next() {
			var tableName string
			if err := tables.Scan(&tableName); err != nil {
				log.ConsoleSugar.Errorf("查询表的信息information_schema.TABLES出错: %v", err)
			}
			if inArrayStr(tableName, excludeTables) {
				continue
			}
			tableddls, nullable_strs, _ := get_table_ddl(mysqlDB, schema, yasdb_schema, tableName)
			for _, tableddl := range tableddls {
				_, err = table_file.WriteString(tableddl)
				if err != nil {
					log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
					return
				}
			}
			tablecomments, _ := getTableComments(mysqlDB, schema, yasdb_schema, tableName)
			for _, tablecomment := range tablecomments {
				_, err = table_file.WriteString(tablecomment)
				if err != nil {
					log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
					return
				}
			}

			for _, nullable_str := range nullable_strs {
				_, err = idx_file.WriteString(nullable_str)
				if err != nil {
					log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
					return
				}
			}
		}
		msg_idx := "\n--创建数据库内的索引\n"
		_, err = idx_file.WriteString(msg_idx)

		tables_idx, err := mysqlDB.Query(`select table_name 
								from information_schema.TABLES 
								where table_schema=?  and table_type = 'BASE TABLE';`, schema)
		if err != nil {
			log.ConsoleSugar.Errorf("查询表的信息information_schema.TABLES出错: %s", err)
		}
		defer tables_idx.Close()
		for tables_idx.Next() {
			var tableName string
			if err := tables_idx.Scan(&tableName); err != nil {
				log.ConsoleSugar.Errorf("查询表的信息information_schema.TABLES出错: %s", err)
			}
			if inArrayStr(tableName, excludeTables) {
				continue
			}

			primarykeys, _ := get_primary_key_ddl(mysqlDB, schema, yasdb_schema, tableName)
			for _, primarykey := range primarykeys {
				_, err = idx_file.WriteString(primarykey)
				if err != nil {
					log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
					return
				}
			}
			uniqindexes, _ := get_uniq_index_ddl(mysqlDB, schema, yasdb_schema, tableName)
			for _, uniqindex := range uniqindexes {
				_, err = idx_file.WriteString(uniqindex)
				if err != nil {
					log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
					return
				}
			}
			nonuniqindexes, _ := get_non_uniq_index_ddl(mysqlDB, schema, yasdb_schema, tableName)
			for _, nonuniqindex := range nonuniqindexes {
				_, err = idx_file.WriteString(nonuniqindex)
				if err != nil {
					log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
					return
				}
			}
		}

		cons_idx := "\n--创建外键约束\n"
		_, err = idx_file.WriteString(cons_idx)

		tables_cons, err := mysqlDB.Query(`select table_name 
									from information_schema.TABLES 
									where table_schema=?  and table_type = 'BASE TABLE';`, schema)
		if err != nil {
			log.ConsoleSugar.Errorf("查询表的信息information_schema.TABLES出错: %v", err)
		}
		defer tables_cons.Close()
		for tables_cons.Next() {
			var tableName string
			if err := tables_cons.Scan(&tableName); err != nil {
				log.ConsoleSugar.Errorf("查询表的信息information_schema.TABLES出错: %v", err)
			}
			if inArrayStr(tableName, excludeTables) {
				continue
			}

			constraints, _ := getTableForeignKeys(mysqlDB, schema, yasdb_schema, tableName)
			for _, constraint := range constraints {
				_, err = idx_file.WriteString(constraint)
				if err != nil {
					log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
					return
				}

			}

		}

		view_ddls := "\n--创建视图\n"
		_, err = idx_file.WriteString(view_ddls)

		viewDDLs, _ := getViewDDLs(mysqlDB, schema, yasdb_schema)
		for _, viewDDL := range viewDDLs {
			_, err = idx_file.WriteString(viewDDL)
			if err != nil {
				log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
				return
			}

		}

		// trigger_ddls := "\n--创建触发器\n"
		// _, err = idx_file.WriteString(trigger_ddls)

		// triggerDDLs, _ := getTriggerSQL(mysqlDB, schema, yasdb_schema)
		// for _, triggerDDL := range triggerDDLs {
		// 	_, err = idx_file.WriteString(triggerDDL)
		// 	if err != nil {
		// 		fmt.Printf("Failed to write to file: %v", err)
		// 		return
		// 	}
		// }

	}
}

func deal_table_data(mysqlDb, yasdDb *sql.DB, mysqlSchema, yasdbSchema string, alltables []string) {

	taskCount := len(alltables)
	start := time.Now() // 记录开始时间

	// 创建一个带有缓冲区的通道，用于控制并发数量
	semaphore := make(chan bool, confdef.GetM2yConfig().Mysql.Parallel)
	// 创建一个等待组，用于等待所有goroutine完成
	var wg sync.WaitGroup

	for i := 0; i < taskCount; i++ {

		wg.Add(1)

		// 在每次循环开始前获取一个信号量
		semaphore <- true
		go func(mysdb, yasdDb *sql.DB, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable string) {
			sync_table_date_from_mysql_to_yasdb(mysdb, yasdDb, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable)
			// 任务完成后释放信号量
			<-semaphore
			wg.Done()

		}(mysqlDb, yasdDb, mysqlSchema, yasdbSchema, alltables[i], alltables[i])
	}

	// 等待所有goroutine完成
	wg.Wait()
	elapsed := time.Since(start) // 计算经过的时间

	log.ConsoleSugar.Infof("任务完成,共耗时: %v", elapsed)

	// for _, tableName := range alltables {
	// 	sync_from_mysql_to_yasdb(mysqlDb, yasdDb, mysqlSchema, yasdbSchema, tableName, tableName)
	// }

}

func deal_tables_ddl(db *sql.DB, schema, yasdb_schema string, alltables []string) {

	if err := fs.Mkdir(tables_ddl); err != nil {
		log.ConsoleSugar.Errorf("mkdir tables err: %s", err.Error())
		return
	}
	if err := fs.Mkdir(others_ddl); err != nil {
		log.ConsoleSugar.Errorf("mkdir tables err: %s", err.Error())
		return
	}
	tab_filename := path.Join(tables_ddl, fmt.Sprintf("%s_tables.sql", schema))
	idx_filename := path.Join(others_ddl, fmt.Sprintf("%s_others.sql", schema))

	table_file, err := os.Create(tab_filename)
	if err != nil {
		log.ConsoleSugar.Errorf("Failed to create file: %v", err)
		return
	}
	defer table_file.Close()
	idx_file, err := os.Create(idx_filename)
	if err != nil {
		log.ConsoleSugar.Errorf("Failed to create file: %v", err)
		return
	}
	defer idx_file.Close()
	// 处理 &转义问题
	typedef_str := "SET DEFINE OFF;\n"
	_, err = table_file.WriteString(typedef_str)

	msg_tab := "--先创建数据库内的表,列默认值,自增序列,列注释\n"
	_, err = table_file.WriteString(msg_tab)
	for _, tableName := range alltables {
		tableddls, nullable_strs, _ := get_table_ddl(db, schema, yasdb_schema, tableName)
		for _, tableddl := range tableddls {
			_, err = table_file.WriteString(tableddl)
			if err != nil {
				log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
				return
			}
		}
		tablecomments, _ := getTableComments(db, schema, yasdb_schema, tableName)
		for _, tablecomment := range tablecomments {
			_, err = table_file.WriteString(tablecomment)
			if err != nil {
				log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
				return
			}
		}
		nullable_idx := "\n--创建表的非空约束语句\n"
		_, err = idx_file.WriteString(nullable_idx)
		for _, nullable_str := range nullable_strs {
			_, err = idx_file.WriteString(nullable_str)
			if err != nil {
				log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
				return
			}
		}
	}
	msg_idx := "\n--再创建数据库内的索引\n"
	_, err = idx_file.WriteString(msg_idx)
	for _, tableName := range alltables {
		primarykeys, _ := get_primary_key_ddl(db, schema, yasdb_schema, tableName)
		for _, primarykey := range primarykeys {
			_, err = idx_file.WriteString(primarykey)
			if err != nil {
				log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
				return
			}
		}
		uniqindexes, _ := get_uniq_index_ddl(db, schema, yasdb_schema, tableName)
		for _, uniqindex := range uniqindexes {
			_, err = idx_file.WriteString(uniqindex)
			if err != nil {
				log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
				return
			}
		}
		nonuniqindexes, _ := get_non_uniq_index_ddl(db, schema, yasdb_schema, tableName)
		for _, nonuniqindex := range nonuniqindexes {
			_, err = idx_file.WriteString(nonuniqindex)
			if err != nil {
				log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
				return
			}
		}
	}

	cons_idx := "\n--最后创建外键约束\n"
	_, err = idx_file.WriteString(cons_idx)
	for _, tableName := range alltables {
		constraints, _ := getTableForeignKeys(db, schema, yasdb_schema, tableName)
		for _, constraint := range constraints {
			_, err = idx_file.WriteString(constraint)
			if err != nil {
				log.ConsoleSugar.Errorf("Failed to write to file: %v", err)
				return
			}

		}
	}

}

type ColumnInfo struct {
	ColumnName string
	ColumnType string
}

type queryFunc func(string) string

func withLimit(limit, offset int) queryFunc {
	return func(s string) string {
		return s + fmt.Sprintf("limit %d offset %d", limit, offset)
	}
}

func mysqlCount(mysdb *sql.DB, schema, table string, opts ...queryFunc) (count int, err error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s ", schema, table)
	for _, opt := range opts {
		sql = opt(sql)
	}
	if querySql := confdef.GetM2yConfig().Mysql.QueryStr; querySql != "" {
		sql = sql + querySql
	}
	err = mysdb.QueryRow(sql).Scan(&count)
	if err != nil {
		return
	}
	return
}

func sync_table_date_from_mysql_to_yasdb(mysdb, yasdDb *sql.DB, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable string) {
	//处理总行数
	var totalCount int
	// 查询总记录数
	count, err := mysqlCount(mysdb, mysqlSchema, mysqlTable)
	if err != nil {
		log.ConsoleSugar.Errorf("mysql %s.%s count err: %s", mysqlSchema, mysqlTable, err.Error())
		return
	}
	yasdbColumns, err := getYasdbColums(yasdDb, yasdbSchema, yasdbTable)
	if err != nil {
		str := fmt.Sprintf("%s", err)
		log.ConsoleSugar.Errorf(str)
		return
	}

	//设置当前表并行度

	var parallel_this_table int
	//设置limit大小
	var limit int
	if count < 1000 {
		parallel_this_table = 1
		limit = 1000
	} else {
		parallel_this_table = confdef.GetM2yConfig().Mysql.ParallelPerTable
		limit = count / parallel_this_table
	}

	// 记录开始时间
	start := time.Now()
	log.ConsoleSugar.Info(yasdbTable, "开始同步")

	// 创建一个带有缓冲区的通道，用于控制并发数量
	semaphore := make(chan bool, parallel_this_table)
	// 创建一个等待组，用于等待所有goroutine完成
	var wg sync.WaitGroup

	for i := 0; i < parallel_this_table; i++ {

		wg.Add(1)

		// 分批读取数据
		offset := i * limit

		// 在每次循环开始前获取一个信号量
		semaphore <- true
		go func(mysqlSchema, yasdbSchema, mysqlTable, yasdbTable string, yasdbColumns []ColumnInfo, limit, offset int) {
			resultCount := sync_from_mysql_to_yasdb_ol(mysdb, yasdDb, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable, yasdbColumns, limit, offset)
			totalCount = totalCount + resultCount
			// 任务完成后释放信号量
			<-semaphore
			wg.Done()

		}(mysqlSchema, yasdbSchema, mysqlTable, yasdbTable, yasdbColumns, limit, offset)
	}

	// 等待所有goroutine完成
	wg.Wait()
	elapsed := time.Since(start) // 计算经过的时间

	log.ConsoleSugar.Infof("%s 处理完成,迁移数据量: %d 耗时 %v", yasdbTable, totalCount, elapsed)
}

func sync_from_mysql_to_yasdb_ol(mysdb, yasdb *sql.DB, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable string, yasdbColumns []ColumnInfo, limit, offset int) int {
	count, err := mysqlCount(mysdb, mysqlSchema, mysqlTable, withLimit(limit, offset))
	if err != nil {
		logError("mysql %s.%s limit: %d offset: %d count err: %s", mysqlSchema, mysqlTable, limit, offset, err.Error())
		return 0
	}
	if count == 0 {
		log.ConsoleSugar.Warnf("mysql %s.%s limit: %d offset: %d no data ,skip sync", mysqlSchema, mysqlTable, limit, offset)
		return 0
	}
	var resultCount int
	var batchCount int
	// 开始事务
	targetTx, err := yasdb.Begin()
	if err != nil {
		log.ConsoleSugar.Errorf("无法开始事务: %s", err.Error())
		return 0
	}

	// 查询源表数据
	sql := fmt.Sprintf("SELECT * FROM %s.%s LIMIT %d OFFSET %d", mysqlSchema, mysqlTable, limit, offset)
	rows, err := mysdb.Query(sql)
	if err != nil {
		log.ConsoleSugar.Errorf("查询源表数据时发生错误: %s", err)
		return 0
	}
	defer rows.Close()

	// 保存MySQL表的列信息
	columns := []ColumnInfo{}

	columnTypes, _ := rows.ColumnTypes()

	for _, columnType := range columnTypes {
		column := ColumnInfo{
			ColumnName: columnType.Name(),
			ColumnType: columnType.DatabaseTypeName(),
		}
		columns = append(columns, column)
		// values = append(values, new(interface{}))
		// fmt.Println(columnType.DatabaseTypeName())
	}
	for rows.Next() {
		// 准备值的切片
		values := make([]interface{}, len(columns))
		valuePointers := make([]interface{}, len(columns))
		for i := 0; i < len(columns); i++ {
			valuePointers[i] = &values[i]
		}
		err := rows.Scan(valuePointers...)
		if err != nil {
			log.ConsoleSugar.Errorf("扫描源表数据时发生错误:", err)
			break
		}
		yashanValues := make([]interface{}, len(values))
		for i, value := range values {
			// fmt.Println(columns[i].ColumnType)
			yashanValues[i] = convertToYashanType(value, columns[i].ColumnType)
		}
		// 构建YashanDB插入语句
		yashanInsertQuery := buildYashanInsertQuery(yasdbSchema, yasdbTable, yasdbColumns)
		_, err = targetTx.Exec(yashanInsertQuery, yashanValues...)
		if err != nil {
			logError("%s 数据插入时发生错误: %s", yasdbTable, err.Error())
			var values string
			for i, val := range yashanValues {
				if i > 0 {
					values += " , "
				}
				values += fmt.Sprintf("%v", val)
			}
			logError("insert sql: %s ,values: (%s)", yashanInsertQuery, values)
			continue
		}
		// 计数器递增
		batchCount++
		resultCount++
		// 达到批次提交的数据量上限时,执行提交操作

		if batchCount >= confdef.GetM2yConfig().Mysql.BatchSize {
			err = targetTx.Commit()
			if err != nil {
				log.ConsoleSugar.Errorf("提交事务时发生错误: %v", err)
				break
			}
			// 重置计数器
			batchCount = 0
			// 开始新的事务
			targetTx, err = yasdb.Begin()
			if err != nil {
				log.ConsoleSugar.Errorf("无法开始事务: %v", err)
				break
			}
		}

	}
	// 执行最后一批数据的提交操作
	if batchCount > 0 {
		err = targetTx.Commit()
		if err != nil {
			log.ConsoleSugar.Errorf("提交事务时发生错误: %v", err)
			return 0
		}
	}
	return resultCount
}

func getYasdbColums(yasdb *sql.DB, yasdbSchema, yasdbTable string) ([]ColumnInfo, error) {
	var yasdbColumns []ColumnInfo
	var yasdbColumnName string
	var yasdbColumnType string
	// 查询目标表结构
	// 处理用户是小写的情况 (create user "test" itentified bu xxx)
	if isWarpByQuote(yasdbSchema) {
		yasdbSchema = unWarpQuote(yasdbSchema)
	} else {
		yasdbSchema = strings.ToUpper(yasdbSchema)
	}
	yasdbTable = strings.ToUpper(yasdbTable)
	yasdbSql := fmt.Sprintf("select DATA_TYPE,COLUMN_NAME from all_tab_columns where owner='%s' and TABLE_NAME='%s' order by COLUMN_ID", yasdbSchema, yasdbTable)
	yasdbRows, err := yasdb.Query(yasdbSql)
	if err != nil {
		log.ConsoleSugar.Errorf("查询目标结构时发生错误: %v", err)
		return nil, err
	}
	defer yasdbRows.Close()

	for yasdbRows.Next() {

		err := yasdbRows.Scan(&yasdbColumnType, &yasdbColumnName)
		if err != nil {
			log.ConsoleSugar.Errorf("查询目标结构时发生错误:", err)
			break
		}
		var ci ColumnInfo
		ci.ColumnName = yasdbColumnName
		ci.ColumnType = yasdbColumnType
		yasdbColumns = append(yasdbColumns, ci)
	}
	if len(yasdbColumns) == 0 {
		// fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "目标表", yasdbTable, "不存在:")
		err = fmt.Errorf(time.Now().Format("2006-01-02 15:04:05") + " 目标表:" + yasdbTable + "不存在,请检查目标库是否已创建此表.")
	}
	return yasdbColumns, err
}

func uint8SliceToInt(slice []uint8) int {
	var result int
	for _, val := range slice {
		result = result*256 + int(val)
	}
	return result
}

func isWarpByQuote(s string) bool {
	return len(s) > 2 && s[0] == '"' && s[len(s)-1] == '"'
}

func unWarpQuote(s string) string {
	if isWarpByQuote(s) {
		return strings.ReplaceAll(s, `"`, "")
	}
	return s
}

// 将值转换为YashanDB类型
func convertToYashanType(value interface{}, columnType string) interface{} {
	if value != nil {
		switch columnType {
		case "DATETIME", "TIMESTAMP":
			t, _ := time.Parse("2006-01-02 15:04:05", string(value.([]uint8)))
			cstLocation, _ := time.LoadLocation("Asia/Shanghai")
			return t.In(cstLocation)
		case "DATE":
			t, _ := time.Parse("2006-01-02", string(value.([]uint8)))
			cstLocation, _ := time.LoadLocation("Asia/Shanghai")
			return t.In(cstLocation)
		case "YEAR":
			t, _ := time.Parse("2006", string(value.([]uint8)))
			cstLocation, _ := time.LoadLocation("Asia/Shanghai")
			return t.In(cstLocation)
		case "JSON", "BLOB", "VARBINARY", "BINARY", "MEDIUMBLOB", "LONGBLOB":
			return value
		case "BIT":
			return uint8SliceToInt(value.([]uint8))
		default:
			return string(value.([]uint8))
		}
	} else {
		return value
	}
}

// 构建YashanDB插入语句
func buildYashanInsertQuery(yasdbSchema, tableName string, columns []ColumnInfo) string {
	query := fmt.Sprintf("INSERT INTO %s.%s (", yasdbSchema, tableName)
	for i, column := range columns {
		if i > 0 {
			query += ","
		}
		query += "\"" + column.ColumnName + "\""
	}
	query += ") VALUES ("
	for i := range columns {
		if i > 0 {
			query += ","
		}
		query += "?"
	}
	query += ")"
	return query
}

func getMysqlAllDbs(mysqlDB *sql.DB) []string {
	var dbs []string
	// 查询数据库信息
	rows, err := mysqlDB.Query("SHOW DATABASES")
	if err != nil {
		log.ConsoleSugar.Errorf("查询Mysql DATABASES失败: %v", err)
		return nil
	}
	defer rows.Close()

	// 遍历结果
	var dbName string
	for rows.Next() {
		err := rows.Scan(&dbName)
		if err != nil {
			log.ConsoleSugar.Errorf("遍历Mysql DATABASES结果失败: %v", err)
			return nil
		}
		dbs = append(dbs, dbName)
	}

	err = rows.Err()
	if err != nil {
		log.ConsoleSugar.Errorf("遍历Mysql DATABASES结果失败: %v", err)
		return nil
	}
	return dbs
}

func initApp(config string) error {
	confdef.InitConfig(config)
	db.LoadMysqlDB(confdef.GetM2yConfig().Mysql)
	db.LoadYashanDB(confdef.GetM2yConfig().Yashan)
	log.InitLog()
	return nil
}

func main() {
	help, version, data, config := parseFlag()
	fmt.Println()
	if help {
		printHelp()
		return
	}
	if version {
		printVersion()
		return
	}
	if config == "" {
		config = "db.toml"
	}
	if err := initApp(config); err != nil {
		fmt.Println(err)
		return
	}
	m2yConf := confdef.GetM2yConfig()
	mConf, yConf := m2yConf.Mysql, m2yConf.Yashan
	mTables, mSchemas := mConf.Tables, mConf.Schemas
	mTableLen, mSchemasLen := len(mTables), len(mSchemas)
	yRempSchemas := yConf.RemapSchemas
	yRempSchemasLen := len(yRempSchemas)
	if data {
		log.ConsoleSugar.Infof("本次数据同步多表并发度为: %d", mConf.Parallel)
		log.ConsoleSugar.Infof("本次数据同步表内并行度为: %d", mConf.ParallelPerTable)
		log.ConsoleSugar.Infof("本次数据同步批处理大小为: %d", mConf.BatchSize)
		if len(mTables) != 0 {
			yasdb_schema := yConf.RemapSchemas[0]
			deal_table_data(db.MysqlDB, db.YashanDB, mConf.Database, yasdb_schema, mConf.Tables)
		}
		if len(mSchemas) != 0 {
			if mSchemasLen != yRempSchemasLen {
				log.ConsoleSugar.Infof("schemas和remap_schemas数量不一致,请检查配置文件:")
				return
			} else {
				deal_schemas_data(db.MysqlDB, db.YashanDB, mConf.Schemas, yConf.RemapSchemas, mConf.ExcludeTables)
			}
		}
		return
	}
	if mTableLen != 0 {
		deal_tables_ddl(db.MysqlDB, mConf.Database, yConf.RemapSchemas[0], mConf.Tables)
	}
	if mSchemasLen != 0 {
		if mSchemasLen != yRempSchemasLen {
			log.ConsoleSugar.Error("schemas和remap_schemas数量不一致,请检查配置文件")
			return
		}
		deal_schemas_ddl(db.MysqlDB, mSchemas, yRempSchemas, mConf.ExcludeTables)
	}
	log.ConsoleSugar.Info("ddl文件生成完成。")
}

func ToJsonStr(v interface{}) string {
	bytes, _ := json.MarshalIndent(v, "", "   ")
	return string(bytes)
}

func parseFlag() (bool, bool, bool, string) {
	help := flag.Bool("h", false, "显示帮助信息")
	version := flag.Bool("v", false, "显示版本号")
	config := flag.String("c", "", "设置配置文件")
	data := flag.Bool("d", false, "同步表数据")
	var lang_help, lang_data, lang_version bool
	flag.BoolVar(&lang_help, "help", false, "显示帮助信息")
	flag.BoolVar(&lang_version, "version", false, "显示程序版本号")
	flag.StringVar(config, "config", "", "设置配置文件")
	flag.BoolVar(&lang_data, "data", false, "同步表数据")
	flag.Parse()
	return *help || lang_help, *version || lang_version, *data || lang_data, *config
}

func logError(format string, args ...any) {
	log.ConsoleSugar.Errorf(format, args...)
	log.ErrorSugar.Errorf(format, args...)
}
