package modules

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"m2y/defs/confdef"
	"m2y/defs/sqldef"
	"m2y/log"
)

type queryFunc func(string) string

type ColumnInfo struct {
	ColumnName string
	ColumnType string
}

type schemaTable struct {
	mysqlSchema string
	yasdbSchema string
	table       string
}

func DealTableData(mysql, yasdb *sql.DB, mysqlSchema, yasdbSchema string, alltables []string, parallel, tableParallel, batchSize int) error {
	taskCount := len(alltables)
	start := time.Now() // 记录开始时间
	// 创建一个带有缓冲区的通道，用于控制并发数量
	semaphore := make(chan bool, parallel)
	// 创建一个等待组，用于等待所有goroutine完成
	var wg sync.WaitGroup
	log.Logger.Infof("开始同步mysql中表数据到yashandb......")
	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		// 在每次循环开始前获取一个信号量
		semaphore <- true
		go func(mysdb, yasdDb *sql.DB, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable string) {
			defer wg.Done()
			syncTableDataFromMySQLToYasdb(0, mysdb, yasdDb, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable, tableParallel, batchSize)
			// 任务完成后释放信号量
			<-semaphore

		}(mysql, yasdb, mysqlSchema, yasdbSchema, alltables[i], alltables[i])
	}
	// 等待所有goroutine完成
	wg.Wait()
	elapsed := time.Since(start) // 计算经过的时间
	log.Logger.Infof("任务完成, 共耗时: %v\n", elapsed)
	return nil
}

func DealSchemasData(mysql, yasdb *sql.DB, mysqlSchemas, yasdbSchemas []string, excludeTables []string, parallel, tableParallel, batchSize int) error {
	// 查询表的信息
	mysqDbs, err := getMySQLAllDbs(mysql)
	if err != nil {
		return fmt.Errorf("获取mysql所有schema失败: %v", err)
	}
	sts := []schemaTable{}
	for i, schema := range mysqlSchemas {
		if !inArrayStr(schema, mysqDbs) {
			log.Logger.Infof("schema%s不存在, 请检查配置文件或mysql环境信息", schema)
			continue
		}
		tables, err := getMySQLSchemaTables(mysql, schema)
		if err != nil {
			return err
		}
		for _, table := range tables {
			if inArrayStr(table, excludeTables) {
				continue
			}
			sts = append(sts, schemaTable{table: table, mysqlSchema: schema, yasdbSchema: yasdbSchemas[i]})
		}
	}
	taskCount := len(sts)
	start := time.Now() // 记录开始时间
	// 创建一个带有缓冲区的通道，用于控制并发数量
	semaphore := make(chan bool, parallel)
	// 创建一个等待组，用于等待所有goroutine完成
	var wg sync.WaitGroup
	log.Logger.Infof("开始同步mysql数据到yashandb......")
	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		// 在每次循环开始前获取一个信号量
		semaphore <- true
		go func(i int, mysdb, yasdDb *sql.DB, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable string, tableParallel, batchSize int) {
			defer wg.Done()
			syncTableDataFromMySQLToYasdb(i, mysdb, yasdDb, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable, tableParallel, batchSize)
			// 任务完成后释放信号量
			<-semaphore

		}(i, mysql, yasdb, sts[i].mysqlSchema, sts[i].yasdbSchema, sts[i].table, sts[i].table, tableParallel, batchSize)
	}
	// 等待所有goroutine完成
	wg.Wait()
	elapsed := time.Since(start) // 计算经过的时间
	log.Logger.Infof("数据同步任务完成, 共耗时: %v", elapsed)
	return nil
}

func syncTableDataFromMySQLToYasdb(i int, mysql, yasdb *sql.DB, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable string, tableParallel, batchSize int) {
	// 记录开始时间
	if i == 100 {
		fmt.Println(i)
	}
	start := time.Now()
	log.Logger.Infof("开始同步mysql表 %s.%s", mysqlSchema, mysqlTable)
	//处理总行数
	var totalCount int
	count, err := getMySQLTableCount(mysql, mysqlSchema, mysqlTable)
	if err != nil {
		log.Logger.Errorf("表 %s.%s 同步失败, 获取mysql端表数据失败: %v", mysqlSchema, mysqlTable, err)
		return
	}
	yasdbColumns, err := getYasdbColumns(yasdb, yasdbSchema, yasdbTable)
	if err != nil {
		log.Logger.Errorf("表 %s.%s 同步失败, 获取yashandb端表结构失败: %v", mysqlSchema, mysqlTable, err)
		return
	}
	//设置当前表并行度
	//设置limit大小
	var limit int
	if count < 1000 {
		tableParallel = 1
		limit = 1000
	} else {
		limit = count/tableParallel + 1
	}
	// 创建一个带有缓冲区的通道，用于控制并发数量
	semaphore := make(chan bool, tableParallel)
	// 创建一个等待组，用于等待所有goroutine完成
	var wg sync.WaitGroup
	for i := 0; i < tableParallel && i*limit <= count; i++ {
		wg.Add(1)
		// 分批读取数据
		offset := i * limit
		// 在每次循环开始前获取一个信号量
		semaphore <- true
		go func(mysqlSchema, yasdbSchema, mysqlTable, yasdbTable string, yasdbColumns []ColumnInfo, limit, offset int) {
			defer wg.Done()
			resultCount := syncTableDataFromMySQLToYasdbParallel(mysql, yasdb, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable, yasdbColumns, limit, offset, batchSize)
			totalCount = totalCount + resultCount
			// 任务完成后释放信号量
			<-semaphore
		}(mysqlSchema, yasdbSchema, mysqlTable, yasdbTable, yasdbColumns, limit, offset)
	}
	// 等待所有goroutine完成
	wg.Wait()
	elapsed := time.Since(start) // 计算经过的时间
	log.Logger.Infof("表 %s.%s 同步完成, 迁移数据量: %d 耗时 %v\n", mysqlSchema, mysqlTable, totalCount, elapsed)
}

func getYasdbColumns(yasdb *sql.DB, yasdbSchema, yasdbTable string) ([]ColumnInfo, error) {
	var yasdbColumns []ColumnInfo
	var yasdbColumnName string
	var yasdbColumnType string
	// 查询目标表结构
	// 处理用户是小写的情况 (create user "test" itentified bu xxx)
	if isWarpByQuote(yasdbSchema) {
		yasdbSchema = unWarpQuote(yasdbSchema)
	} else if !confdef.GetM2YConfig().Yashan.CaseSensitive {
		yasdbSchema = strings.ToUpper(yasdbSchema)
	}
	if !confdef.GetM2YConfig().Yashan.CaseSensitive {
		yasdbTable = strings.ToUpper(yasdbTable)
	}
	yasdbSql := fmt.Sprintf(sqldef.Y_SQL_QUERY_COLUMN, yasdbSchema, yasdbTable)
	yasdbRows, err := yasdb.Query(yasdbSql)
	if err != nil {
		return nil, err
	}
	defer yasdbRows.Close()

	for yasdbRows.Next() {
		if err = yasdbRows.Scan(&yasdbColumnType, &yasdbColumnName); err != nil {
			return nil, err
		}
		yasdbColumns = append(yasdbColumns, ColumnInfo{
			ColumnName: yasdbColumnName,
			ColumnType: yasdbColumnType})
	}
	if len(yasdbColumns) == 0 {
		return nil, fmt.Errorf("目标表 %s.%s 不存在, 请检查目标库是否已创建此表", yasdbSchema, yasdbTable)
	}
	return yasdbColumns, err
}

func syncTableDataFromMySQLToYasdbParallel(mysdb, yasdb *sql.DB, mysqlSchema, yasdbSchema, mysqlTable, yasdbTable string, yasdbColumns []ColumnInfo, limit, offset, batchSize int) int {
	var resultCount int
	var batchCount int
	// 开始事务
	targetTx, err := yasdb.Begin()
	if err != nil {
		log.Logger.Errorf("表 %s.%s 同步失败, 事务开始失败: %v", mysqlSchema, mysqlTable, err)
		return 0
	}
	// 查询源表数据
	rows, err := mysdb.Query(fmt.Sprintf(sqldef.M_SQL_QUERY_TABLE_DATA, mysqlSchema, mysqlTable, limit, offset))
	if err != nil {
		log.Logger.Errorf("表 %s.%s 同步失败, 源端数据查询失败: %v", mysqlSchema, mysqlTable, err)
		return 0
	}
	defer rows.Close()

	// 保存MySQL表的列信息
	columns := []ColumnInfo{}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Logger.Errorf("表 %s.%s 同步失败, 源端数据列信息获取失败: %v", mysqlSchema, mysqlTable, err)
		return 0
	}
	for _, columnType := range columnTypes {
		column := ColumnInfo{
			ColumnName: columnType.Name(),
			ColumnType: columnType.DatabaseTypeName(),
		}
		columns = append(columns, column)
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
			log.Logger.Errorf("表 %s.%s 同步失败, 源端数据查询失败: %v", mysqlSchema, mysqlTable, err)
			break
		}
		yashanValues := make([]interface{}, len(values))
		for i, value := range values {
			// fmt.Println(columns[i].ColumnType)
			yashanValues[i] = convertValueFromMySQLToYashan(value, columns[i].ColumnType)
		}
		// 构建YashanDB插入语句
		yashanInsertSQL := buildYashanInsertSQL(yasdbSchema, yasdbTable, yasdbColumns)
		_, err = targetTx.Exec(yashanInsertSQL, yashanValues...)
		if err != nil {
			log.Logger.Errorf("表 %s.%s 同步失败, 目标端数据插入失败, sql: %s value: %v, err: %v", mysqlSchema, mysqlTable, yashanInsertSQL, yashanValues, err)
			continue
		}
		// 计数器递增
		batchCount++
		resultCount++
		// 达到批次提交的数据量上限时,执行提交操作
		if batchCount >= batchSize {
			err = targetTx.Commit()
			if err != nil {
				log.Logger.Errorf("表 %s.%s 同步失败, 事务提交失败: %v", mysqlSchema, mysqlTable, err)
				break
			}
			// 重置计数器
			batchCount = 0
			// 开始新的事务
			targetTx, err = yasdb.Begin()
			if err != nil {
				log.Logger.Errorf("表 %s.%s 同步失败, 事务开始失败: %v", mysqlSchema, mysqlTable, err)
				break
			}
		}
	}
	err = targetTx.Commit()
	if err != nil {
		log.Logger.Errorf("表 %s.%s 同步失败, 事务提交失败: %v", mysqlSchema, mysqlTable, err)
		return resultCount
	}
	// 执行最后一批数据的提交操作
	// if batchCount > 0 {
	// err = targetTx.Commit()
	// if err != nil {
	// 	log.Logger.Errorf("表 %s.%s 同步失败, 事务提交失败: %v", mysqlSchema, mysqlTable, err)
	// 	return resultCount
	// }
	// }
	return resultCount
}

func getMySQLTableCount(mysdb *sql.DB, schema, table string, opts ...queryFunc) (count int, err error) {
	sql := fmt.Sprintf(sqldef.M_SQL_QUERY_TABLE_COUNT, schema, table)
	for _, opt := range opts {
		sql = opt(sql)
	}
	if querySql := confdef.GetM2YConfig().MySQL.QueryStr; querySql != "" {
		sql = sql + querySql
	}
	err = mysdb.QueryRow(sql).Scan(&count)
	if err != nil {
		return
	}
	return
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
func convertValueFromMySQLToYashan(value interface{}, columnType string) interface{} {
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
			var v string
			switch val := value.(type) {
			case []uint8:
				v = string(value.([]uint8))
			case uint, uint8, uint16, uint32, uint64, int, int8, int16, int32, int64:
				return val
			default:
				v = fmt.Sprint(value)
			}
			year, _ := strconv.ParseInt(v, 10, 64)
			return year
		case "JSON", "BLOB", "VARBINARY", "BINARY", "MEDIUMBLOB", "LONGBLOB":
			return value
		case "BIT":
			return uint8SliceToInt(value.([]uint8))
		default:
			if str, ok := value.([]uint8); ok {
				return string(str)
			}
			return value
		}
	} else {
		return value
	}
}

func uint8SliceToInt(slice []uint8) int {
	var result int
	for _, val := range slice {
		result = result*256 + int(val)
	}
	return result
}

// 构建YashanDB插入语句
func buildYashanInsertSQL(yasdbSchema, tableName string, columns []ColumnInfo) string {
	caseSensitive := confdef.GetM2YConfig().Yashan.CaseSensitive
	var columnNames, placeholders []string
	for _, column := range columns {
		columnName := column.ColumnName
		if caseSensitive {
			columnName = fmt.Sprintf("\"%s\"", column.ColumnName)
		}
		columnNames = append(columnNames, formatKeyWord(columnName))
		placeholders = append(placeholders, "?")
	}
	formatter := getSQLFormatter(sqldef.Y_SQL_INSERT_DATA, sqldef.Y_SQL_INSERT_DATA_CASE_SENSITIVE)
	return fmt.Sprintf(formatter, formatKeyWord(yasdbSchema), formatKeyWord(tableName), strings.Join(columnNames, ","), strings.Join(placeholders, ","))
}
