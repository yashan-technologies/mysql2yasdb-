package modules

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"m2y/defs/confdef"
	"m2y/defs/sqldef"
	"m2y/log"
)

type tableData struct {
	RowData []interface{}
	PkData  map[string]interface{}
}

var cannotUsedPrimaryDateType = map[string]struct{}{
	"tinyblob":   {},
	"blob":       {},
	"mediumblob": {},
	"longblob":   {},
	"tinytext":   {},
	"text":       {},
	"mediumtext": {},
	"longtext":   {},
}

func getMysqlPrimaryKey(mysqlDB *sql.DB, mysqlSchema, tableName string) ([]string, error) {
	var pkColumns []string
	// 查询主键列信息
	rows, err := mysqlDB.Query(sqldef.M_SQL_QUERY_PRIMARY_KEY, mysqlSchema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return nil, err
		}
		// lob类型的数据不能作为查询字段，跳过该字段作为主键列
		if _, ok := cannotUsedPrimaryDateType[dataType]; ok {
			continue
		}
		pkColumns = append(pkColumns, columnName)
		// fmt.Printf("Column Name: %s, Data Type: %s\n", columnName, dataType)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pkColumns, nil
}

func compareTableCount(mysqlDB, yashanDB *sql.DB, mysqlSchema, yasdbSchema, tableName string) ([]string, error) {
	var result []string
	// 查询 MySQL 表总行数
	mysqlQuery := fmt.Sprintf(sqldef.M_SQL_QUERY_TABLE_COUNT, mysqlSchema, tableName)
	mysqlRows, err := mysqlDB.Query(mysqlQuery)
	if err != nil {
		return nil, err
	}
	defer mysqlRows.Close()

	var mysqlRowCount int
	if mysqlRows.Next() {
		if err := mysqlRows.Scan(&mysqlRowCount); err != nil {
			return nil, err
		}
	}

	// 查询 yasdb 表总行数
	yasdbQuery := fmt.Sprintf(sqldef.Y_SQL_QUERY_TABLE_COUNT, yasdbSchema, tableName)
	yasdbRows, err := yashanDB.Query(yasdbQuery)
	if err != nil {
		return nil, err
	}
	defer yasdbRows.Close()

	var yasdbRowCount int
	if yasdbRows.Next() {
		if err := yasdbRows.Scan(&yasdbRowCount); err != nil {
			return nil, err
		}
	}
	result = append(result, mysqlSchema)
	result = append(result, yasdbSchema)
	result = append(result, tableName)
	result = append(result, strconv.Itoa(mysqlRowCount))
	result = append(result, strconv.Itoa(yasdbRowCount))
	result = append(result, strconv.Itoa(mysqlRowCount-yasdbRowCount))
	return result, nil
}

func CompareTables(mysqlDB, yashanDB *sql.DB, mysqlSchema, yasdbSchema string, tables []string, parallel, sampleLine int) ([][]string, error) {
	sts := []schemaTable{}
	for _, table := range tables {
		sts = append(sts, schemaTable{table: table, mysqlSchema: mysqlSchema, yasdbSchema: yasdbSchema})
	}
	return compareTables(mysqlDB, yashanDB, sts, parallel, sampleLine)
}

func CompareSchemas(mysqlDB, yashanDB *sql.DB, mysqlSchemas, remapSchemas []string, excludeTables []string, parallel, sampleLine int) ([][]string, error) {
	// 查询表的信息
	mysqDbs, err := getMysqlAllDbs(mysqlDB)
	if err != nil {
		return nil, err
	}
	sts := []schemaTable{}
	for i, mysqlSchema := range mysqlSchemas {
		if !inArrayStr(mysqlSchema, mysqDbs) {
			log.Logger.Errorf("Mysql Database %s 不存在,请检查配置文件或Mysql环境\n", mysqlSchema)
			continue
		}
		yasdbSchema := remapSchemas[i]
		tables, err := getMysqlSchemaTables(mysqlDB, mysqlSchema)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			if containsString(excludeTables, table) {
				continue
			}
			sts = append(sts, schemaTable{
				mysqlSchema: mysqlSchema,
				yasdbSchema: yasdbSchema,
				table:       table,
			})
		}
	}
	taskCount := len(sts)
	if taskCount < parallel {
		parallel = taskCount
	}
	return compareTables(mysqlDB, yashanDB, sts, parallel, sampleLine)
}

func compareTables(mysqlDB, yashanDB *sql.DB, tables []schemaTable, parallel, sampleLine int) ([][]string, error) {
	var results [][]string
	// 创建一个带有缓冲区的通道，用于控制并发数量
	taskCount := len(tables)
	if taskCount < parallel {
		parallel = taskCount
	}
	semaphore := make(chan bool, parallel)
	// 创建一个等待组，用于等待所有goroutine完成
	var wg sync.WaitGroup
	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		// 在每次循环开始前获取一个信号量
		semaphore <- true
		go func(mysqlDB, yashanDB *sql.DB, mysqlSchema, yasdbSchema, tableName string) {
			defer func() {
				<-semaphore
				wg.Done()
			}()
			log.Logger.Infof("开始对比Mysql表 %s.%s 和YashanDB表 %s.%s ...\n", mysqlSchema, tableName, yasdbSchema, tableName)
			// 记录开始时间
			start := time.Now()
			log.Logger.Infof("开始对比Mysql表 %s.%s 和YashanDB表 %s.%s 总行数...\n", mysqlSchema, tableName, yasdbSchema, tableName)
			result, err := compareTableCount(mysqlDB, yashanDB, mysqlSchema, yasdbSchema, tableName)
			if err != nil {
				log.Logger.Errorf("Mysql表 %s.%s 和YashanDB表 %s.%s 总行数对比失败: %v\n", mysqlSchema, tableName, yasdbSchema, tableName, err)
				return
			}
			results = append(results, result)
			var errCount int
			if !confdef.GetM2YConfig().Mysql.RowsOnly {
				log.Logger.Infof("开始对比Mysql表 %s.%s 和YashanDB表 %s.%s 内容...\n", mysqlSchema, tableName, yasdbSchema, tableName)
				errCount, err = compareTableContent(mysqlDB, yashanDB, mysqlSchema, yasdbSchema, tableName, sampleLine)
				if err != nil {
					log.Logger.Errorf("Mysql表 %s.%s 和YashanDB表 %s.%s 内容对比失败: %v\n", mysqlSchema, tableName, yasdbSchema, tableName, err)
					return
				}
			}
			elapsed := time.Since(start) // 计算经过的时间
			if errCount > 0 {
				log.Logger.Infof("Mysql表 %s.%s 和YashanDB表 %s.%s 数据对比完成, 错误行数: %d, 耗时: %s\n", mysqlSchema, tableName, yasdbSchema, tableName, errCount, elapsed)
			} else {
				log.Logger.Infof("Mysql表 %s.%s 和YashanDB表 %s.%s 数据对比完成, 无异常, 耗时: %s\n", mysqlSchema, tableName, yasdbSchema, tableName, elapsed)
			}
		}(mysqlDB, yashanDB, tables[i].mysqlSchema, tables[i].yasdbSchema, tables[i].table)
	}
	// 等待所有goroutine完成
	wg.Wait()
	return results, nil
}

func compareTableContent(mysqlDB, yashanDB *sql.DB, mysqlSchema, yasdbSchema, tableName string, sampleLine int) (int, error) {
	errCount := 0
	pkColumnName, err := getMysqlPrimaryKey(mysqlDB, mysqlSchema, tableName)
	if err != nil {
		log.Logger.Errorf("获取Mysql表 %s.%s 主键失败: %v\n", mysqlSchema, tableName, err)
		return 0, err
	}
	if len(pkColumnName) == 0 {
		log.Logger.Errorf("Mysql表 %s.%s 没有主键\n", mysqlSchema, tableName)
		return 0, fmt.Errorf("跳过无主键表 %s.%s 的内容比对", mysqlSchema, tableName)
	}

	// 获取 MySQL 表数据
	mysqlData, columnNames, err := getMysqlTableData(mysqlDB, mysqlSchema, tableName, pkColumnName, sampleLine)
	if err != nil {
		log.Logger.Errorf("获取Mysql表 %s.%s 数据失败: %v\n", mysqlSchema, tableName, err)
		return 0, err
	}

	// 遍历 MySQL 表数据，逐行比较
	for _, mysqlRow := range mysqlData {
		// 根据主键值获取 yasdb 对应行数据
		yasdbRow, err := getYasdbTableRowByPK(yashanDB, yasdbSchema, tableName, pkColumnName, mysqlRow.PkData)
		if err != nil {
			fmt.Printf("Failed to get table %s row from yasdb: %v\n", tableName, err)
			errCount++
			continue
		}
		// 比较两行数据
		if !compareTableRowData(mysqlRow, yasdbRow, columnNames, tableName) {
			errCount++
		}
	}
	return errCount, nil
}

func getMysqlTableData(db *sql.DB, mysqlSchema, tableName string, pkColumnNames []string, sampleLine int) ([]tableData, []string, error) {
	var columnNames []string
	query := fmt.Sprintf(sqldef.M_SQL_QUERY_TABLE_ALL_DATA, mysqlSchema, tableName)
	if sampleLine != 0 {
		query = fmt.Sprintf(sqldef.M_SQL_QUERY_ORDER_RAND_LIMIT, query, sampleLine)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columns := []ColumnInfo{}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	for _, columnType := range columnTypes {
		column := ColumnInfo{
			ColumnName: columnType.Name(),
			ColumnType: columnType.DatabaseTypeName(),
		}
		columns = append(columns, column)
		columnNames = append(columnNames, columnType.Name())
	}

	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	data := make([]tableData, 0)

	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, nil, err
		}

		rowData := make([]interface{}, count)
		pkData := make(map[string]interface{}, count)
		mysqlValues := make([]interface{}, len(values))
		for i, value := range values {
			// fmt.Println(columns[i].ColumnType)
			mysqlValues[i] = convertToMysqlType(value, columns[i].ColumnType)
		}

		for i, column := range columns {
			val := mysqlValues[i]
			// fmt.Println(val)
			if val != nil {
				rowData[i] = val
			} else {
				rowData[i] = nil
			}
			if containsString(pkColumnNames, column.ColumnName) {
				pkData[columns[i].ColumnName] = mysqlValues[i]
			}
		}
		data = append(data, tableData{RowData: rowData, PkData: pkData})
	}
	return data, columnNames, nil
}

// 根据主键从 yasdb 中获取一行数据
func getYasdbTableRowByPK(db *sql.DB, tableSchema, tableName string, pkColumnName []string, primaryKey map[string]interface{}) (tableData, error) {
	var pkValues []interface{}
	arr := make([]string, 0)
	for i, columnName := range pkColumnName {
		arr = append(arr, fmt.Sprintf("\"%s\" = :%d", columnName, i+1))
		pkValues = append(pkValues, primaryKey[columnName])
	}

	rows, err := db.Query(fmt.Sprintf(sqldef.Y_SQL_QUERY_TABLE_ROW_DATA, tableSchema, tableName, strings.Join(arr, " AND ")), pkValues...)
	if err != nil {
		return tableData{}, err
	}
	defer rows.Close()

	columns := []ColumnInfo{}
	columnTypes, _ := rows.ColumnTypes()
	for _, columnType := range columnTypes {
		column := ColumnInfo{
			ColumnName: columnType.Name(),
			ColumnType: columnType.DatabaseTypeName(),
		}
		columns = append(columns, column)
	}
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	rowData := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return tableData{}, err
		}
		for i := range columns {
			val := values[i]
			if val != nil {
				rowData[i] = val
			} else {
				rowData[i] = nil
			}
		}
	}
	return tableData{RowData: rowData}, nil
}

// 比较两行数据是否完全一致
func compareTableRowData(row1 tableData, row2 tableData, columnNames []string, tableName string) bool {
	// 比较两行数据的字段数量是否一致
	if len(row1.RowData) != len(row2.RowData) {
		return false
	}
	// 比较两行数据的每个字段值是否一致
	for i, value1 := range row1.RowData {
		value2 := row2.RowData[i]
		// 对比字段值是否一致
		// if !reflect.DeepEqual(value1, value2) {
		if fmt.Sprintf("%v", value1) != strings.TrimSpace(fmt.Sprintf("%v", value2)) {
			// fmt.Println(i, value1, value2, fmt.Sprintf("%T", value1), fmt.Sprintf("%T", value2))
			log.Logger.Errorf("Mysql表 %s 字段 %s 主键值为 %v 的数据不一致, Mysql数据为: %v(%v) YashanDB数据为: %v(%v)\n", tableName, columnNames[i], row1.PkData, value1, reflect.TypeOf(value1), value2, reflect.TypeOf(value2))
			return false
		}
	}
	return true
}

// 将值转换为Mysql类型
func convertToMysqlType(value interface{}, columnType string) interface{} {
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
		case "TIME":
			t, _ := time.Parse("2006-01-02 15:04:05", "1970-01-01 "+string(value.([]uint8)))
			cstLocation, _ := time.LoadLocation("Asia/Shanghai")
			return t.In(cstLocation)
		case "BINARY", "VARBINARY", "BLOB":
			return value
		case "BIT":
			return convertBitToString(value.([]uint8))
		case "FLOAT":
			str, ok := value.([]uint8)
			if ok {
				return convertMysqlFloat(string(str))
			}
			return value
		case "DOUBLE", "DECIMAL", "BIGINT":
			str, ok := value.([]uint8)
			if ok {
				return convertMysqlDoubleOrDecimal(string(str))
			}
			return value
		case "JSON":
			str := string(value.([]uint8))
			var data map[string]interface{}
			err := json.Unmarshal([]byte(str), &data)
			if err != nil {
				fmt.Println("字符串转换成JSON失败:", err)
			}
			jsonStr, err := json.Marshal(data)
			if err != nil {
				fmt.Println("JSON转换成字符串失败:", err)
			}
			return string(jsonStr)
		default:
			str, ok := value.([]uint8)
			if ok {
				return string(str)
			}
			return value
		}
	} else {
		return value
	}
}

func convertMysqlFloat(value string) interface{} {
	res, err := strconv.ParseFloat(value, 32)
	if err != nil {
		return value
	}
	return float32(res)
}

func convertMysqlDoubleOrDecimal(value string) interface{} {
	res, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return value
	}
	return res
}

type BIT uint8

func convertBitToString(bits []uint8) string {
	str := ""
	for _, b := range bits {
		for i := 7; i >= 0; i-- {
			bit := (b >> uint(i)) & 1
			str += strconv.Itoa(int(BIT(bit)))
		}
	}
	lenb := len(str)
	str = strings.TrimLeft(str, "0")
	if str == "" && lenb > 0 {
		str = "0"
	}
	return str
}

func containsString(arr []string, target string) bool {
	for _, str := range arr {
		if str == target {
			return true
		}
	}
	return false
}

func PrintCheckResults(results [][]string) {
	var sames, not_sames [][]string
	mdbnameWidth := 16
	ydbnameWidth := 21
	tbnameWidth := 11
	mrcWidth := 14
	yrcWidth := 19
	drcWidth := 15
	if len(results) == 0 {
		fmt.Printf("没有需要对比的表\n")
		return
	}
	for _, result := range results {
		if len(result) == 0 {
			continue
		}
		lastElement := result[len(result)-1]
		num, err := strconv.Atoi(lastElement)
		if err != nil {
			fmt.Printf("无法将字符串转换为整数：%v", err)
			return
		}
		if num == 0 {
			sames = append(sames, result)
		} else {
			not_sames = append(not_sames, result)
		}
		if len(result[0]) >= mdbnameWidth {
			mdbnameWidth = len(result[0]) + 2
		}
		if len(result[1]) >= ydbnameWidth {
			ydbnameWidth = len(result[1]) + 2
		}
		if len(result[2]) >= tbnameWidth {
			tbnameWidth = len(result[2]) + 2
		}
		if len(result[3]) >= mrcWidth {
			mrcWidth = len(result[3])
		}
		if len(result[4]) >= yrcWidth {
			yrcWidth = len(result[4]) + 2
		}
		if len(result[5]) >= drcWidth {
			drcWidth = len(result[5]) + 2
		}
	}
	fmt.Println("\n表总行数一致的表统计信息如下:")

	// maxWidth := 20
	if len(sames) == 0 {
		fmt.Println("无")
	} else {
		fmt.Printf("%-*s%-*s%-*s%*s%*s%*s\n", mdbnameWidth, "MySQL-database", ydbnameWidth, "YashanDB-tableOwner", tbnameWidth, "tableName", mrcWidth, "MySQL-rowCount", yrcWidth, "YashanDB-rowCount", drcWidth, "diff-rowCount")
	}
	for _, same := range sames {
		fmt.Printf("%-*s%-*s%-*s%*s%*s%*s\n", mdbnameWidth, same[0], ydbnameWidth, same[1], tbnameWidth, same[2], mrcWidth, same[3], yrcWidth, same[4], drcWidth, same[5])
	}
	fmt.Println("\n表总行数不一致的表统计信息如下:")
	if len(not_sames) == 0 {
		fmt.Println("无")
	} else {
		fmt.Printf("%-*s%-*s%-*s%*s%*s%*s\n", mdbnameWidth, "MySQL-database", ydbnameWidth, "YashanDB-tableowner", tbnameWidth, "tableName", mrcWidth, "MySQL-rowCount", yrcWidth, "YashanDB-rowCount", drcWidth, "diff-rowCount")
	}
	for _, not_same := range not_sames {
		fmt.Printf("%-*s%-*s%-*s%*s%*s%*s\n", mdbnameWidth, not_same[0], ydbnameWidth, not_same[1], tbnameWidth, not_same[2], mrcWidth, not_same[3], yrcWidth, not_same[4], drcWidth, not_same[5])
	}
}
