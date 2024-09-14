package modules

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"m2y/defs/confdef"
	"m2y/defs/sqldef"
	"m2y/log"

	"github.com/olekukonko/tablewriter"
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

func getMySQLPrimaryKey(mysqlDB *sql.DB, mysqlSchema, tableName string) ([]string, error) {
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
	formatter := getSQLFormatter(sqldef.Y_SQL_QUERY_TABLE_COUNT, sqldef.Y_SQL_QUERY_TABLE_COUNT_CASE_SENSITIVE)
	yasdbQuery := fmt.Sprintf(formatter, formatKeyWord(yasdbSchema), formatKeyWord(tableName))
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
	mysqDbs, err := getMySQLAllDbs(mysqlDB)
	if err != nil {
		return nil, err
	}
	sts := []schemaTable{}
	for i, mysqlSchema := range mysqlSchemas {
		if !inArrayStr(mysqlSchema, mysqDbs) {
			log.Logger.Errorf("MySQL Database %s 不存在,请检查配置文件或MySQL环境\n", mysqlSchema)
			continue
		}
		yasdbSchema := remapSchemas[i]
		tables, err := getMySQLSchemaTables(mysqlDB, mysqlSchema)
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
			log.Logger.Infof("开始对比 MySQL表 %s.%s 和 YashanDB表 %s.%s ...\n", mysqlSchema, tableName, yasdbSchema, tableName)
			// 记录开始时间
			start := time.Now()
			log.Logger.Infof("开始对比 MySQL表 %s.%s 和 YashanDB表 %s.%s 总行数...\n", mysqlSchema, tableName, yasdbSchema, tableName)
			result, err := compareTableCount(mysqlDB, yashanDB, mysqlSchema, yasdbSchema, tableName)
			if err != nil {
				log.Logger.Errorf("MySQL表 %s.%s 和 YashanDB表 %s.%s 总行数对比失败: %v\n", mysqlSchema, tableName, yasdbSchema, tableName, err)
				return
			}
			results = append(results, result)
			var errCount int
			if !confdef.GetM2YConfig().MySQL.RowsOnly {
				log.Logger.Infof("开始对比 MySQL表 %s.%s 和 YashanDB表 %s.%s 内容...\n", mysqlSchema, tableName, yasdbSchema, tableName)
				errCount, err = compareTableContent(mysqlDB, yashanDB, mysqlSchema, yasdbSchema, tableName, sampleLine)
				if err != nil {
					log.Logger.Errorf("MySQL表 %s.%s 和 YashanDB表 %s.%s 内容对比失败: %v\n", mysqlSchema, tableName, yasdbSchema, tableName, err)
					return
				}
			}
			elapsed := time.Since(start) // 计算经过的时间
			if errCount > 0 {
				log.Logger.Infof("MySQL表 %s.%s 和 YashanDB表 %s.%s 数据对比完成, 错误行数: %d, 耗时: %s\n", mysqlSchema, tableName, yasdbSchema, tableName, errCount, elapsed)
			} else {
				log.Logger.Infof("MySQL表 %s.%s 和 YashanDB表 %s.%s 数据对比完成, 无异常, 耗时: %s\n", mysqlSchema, tableName, yasdbSchema, tableName, elapsed)
			}
		}(mysqlDB, yashanDB, tables[i].mysqlSchema, tables[i].yasdbSchema, tables[i].table)
	}
	// 等待所有goroutine完成
	wg.Wait()
	return results, nil
}

func compareTableContent(mysqlDB, yashanDB *sql.DB, mysqlSchema, yasdbSchema, tableName string, sampleLine int) (int, error) {
	errCount := 0
	pkColumnName, err := getMySQLPrimaryKey(mysqlDB, mysqlSchema, tableName)
	if err != nil {
		log.Logger.Errorf("获取MySQL表 %s.%s 主键失败: %v\n", mysqlSchema, tableName, err)
		return 0, err
	}
	if len(pkColumnName) == 0 {
		// 不支持无主键表的对比
		log.Logger.Warnf("MySQL表 %s.%s 没有主键, 跳过无主键表的内容比对\n", mysqlSchema, tableName)
		return 0, nil
	}

	// 获取 MySQL 表数据
	mysqlData, columnNames, err := getMySQLTableData(mysqlDB, mysqlSchema, tableName, pkColumnName, sampleLine)
	if err != nil {
		log.Logger.Errorf("获取MySQL表 %s.%s 数据失败: %v\n", mysqlSchema, tableName, err)
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

func getMySQLTableData(db *sql.DB, mysqlSchema, tableName string, pkColumnNames []string, sampleLine int) ([]tableData, []string, error) {
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
			mysqlValues[i] = convertToMySQLType(value, columns[i].ColumnType)
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
	caseSensitive := confdef.GetM2YConfig().Yashan.CaseSensitive
	arr := make([]string, 0)
	for i, columnName := range pkColumnName {
		if caseSensitive {
			arr = append(arr, fmt.Sprintf("\"%s\" = :%d", columnName, i+1))
		} else if confdef.IsKeyword(columnName) {
			arr = append(arr, fmt.Sprintf("\"%s\" = :%d", strings.ToUpper(columnName), i+1))
		} else {
			arr = append(arr, fmt.Sprintf("%s = :%d", columnName, i+1))
		}
		pkValues = append(pkValues, primaryKey[columnName])
	}

	formatter := getSQLFormatter(sqldef.Y_SQL_QUERY_TABLE_ROW_DATA, sqldef.Y_SQL_QUERY_TABLE_ROW_DATA_CASE_SENSITIVE)
	rows, err := db.Query(fmt.Sprintf(formatter, formatKeyWord(tableSchema), formatKeyWord(tableName), strings.Join(arr, " AND ")), pkValues...)
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

// mysql和yasdb对空字符串的处理不一样
func isDataEqual(v1, v2 any) bool {
	switch value1 := v1.(type) {
	case string:
		switch value2 := v2.(type) {
		case string:
			return value1 == value2
		case nil:
			return len(value1) == 0
		}
	case nil:
		switch value2 := v2.(type) {
		case string:
			return len(value2) == 0
		case nil:
			return true
		}
	case time.Time:
		switch value2 := v2.(type) {
		case time.Time:
			return value1.Equal(value2)
		}
	}
	return fmt.Sprint(v1) == fmt.Sprint(v2)
}

func showValue(v any) string {
	return fmt.Sprintf("[value: %v, type: %T]", v, v)
}

func showPrimaryKeys(keys map[string]any) string {
	var s string
	for k, v := range keys {
		s += fmt.Sprintf("[%s:%v]", k, v)
	}
	return s
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
		if !isDataEqual(value1, value2) {
			// fmt.Println(i, value1, value2, fmt.Sprintf("%T", value1), fmt.Sprintf("%T", value2))
			log.Logger.Errorf("表：[%s]，字段：[%s] 的数据不一致，主键值：%s, MySQL数据为: %s YashanDB数据为: %s", tableName, columnNames[i], showPrimaryKeys(row1.PkData), showValue(value1), showValue(value2))
			return false
		}
	}
	return true
}

// 将值转换为MySQL类型
func convertToMySQLType(value interface{}, columnType string) interface{} {
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
				return convertMySQLFloat(string(str))
			}
			return value
		case "DOUBLE", "DECIMAL", "BIGINT":
			str, ok := value.([]uint8)
			if ok {
				return convertMySQLDoubleOrDecimal(string(str))
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

func convertMySQLFloat(value string) interface{} {
	res, err := strconv.ParseFloat(value, 32)
	if err != nil {
		return value
	}
	return float32(res)
}

func convertMySQLDoubleOrDecimal(value string) interface{} {
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

func printTable(message string, header []string, data [][]string) {
	fmt.Print(message)
	if len(data) == 0 {
		fmt.Println("无")
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetTablePadding("\t")
	table.SetNoWhiteSpace(true)
	table.AppendBulk(data)
	table.Render()
}

func PrintCheckResults(results [][]string) {
	var sames, not_sames [][]string
	header := []string{"MySQL-Database", "YashanDB-Schema", "Table-Name", "MySQL-Rows", "YashanDB-Rows", "Diff-Rows"}
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
	}
	printTable("表总行数一致的表统计信息如下：", header, sames)
	printTable("表总行数不一致的表统计信息如下：", header, not_sames)
}
