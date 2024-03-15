package db

import (
	"database/sql"
	"fmt"
	"strings"

	"m2y/defs/confdef"

	_ "git.yasdb.com/go/yasdb-go"
	_ "github.com/go-sql-driver/mysql"
)

const (
	driver_mysql    = "mysql"
	driver_yashandb = "yasdb"
)

const (
	MYSQL_VERSION_5 = "5"
	MYSQL_VERSION_8 = "8"
)

var (
	MysqlDB      *sql.DB
	YashanDB     *sql.DB
	MysqlVersion string
)

func LoadMysqlDB(mysql *confdef.MysqlConfig) (err error) {
	mysqlDsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", mysql.UserName, mysql.Password, mysql.Host, mysql.Port, mysql.Database)
	mysqlDB, err := sql.Open(driver_mysql, mysqlDsn)
	if err != nil {
		err = fmt.Errorf("连接mysql时出错: %s", err.Error())
		return
	}
	if err = mysqlDB.Ping(); err != nil {
		err = fmt.Errorf("连接mysql时出错: %s", err.Error())
		return
	}
	MysqlDB = mysqlDB
	err = queryVersion()
	return
}

func LoadYashanDB(yashan *confdef.YashanConfig) (err error) {
	yasdbDsn := fmt.Sprintf("%s/%s@%s:%d", yashan.UserName, formatPassword(yashan.Password), yashan.Host, yashan.Port)
	yasDB, err := sql.Open(driver_yashandb, yasdbDsn)
	if err != nil {
		err = fmt.Errorf("连接yashandb时出错: %s, 请检查配置文件或环境变量", err.Error())
		return
	}
	if err = yasDB.Ping(); err != nil {
		err = fmt.Errorf("连接yashandb时出错: %s, 请检查配置文件或环境变量", err.Error())
		return
	}
	yasDB.SetMaxOpenConns(100)
	yasDB.SetMaxIdleConns(50)
	YashanDB = yasDB
	return
}

func queryVersion() (err error) {
	var version string
	err = MysqlDB.QueryRow("SELECT VERSION()").Scan(&version)
	if err != nil {
		err = fmt.Errorf("查询 MySQL 版本失败: %s", err.Error())
		return
	}
	MysqlVersion = version[0:1]
	return
}

func formatPassword(password string) (newPassword string) {
	var newPwd strings.Builder
	for _, r := range password {
		if r == '\\' || r == '@' || r == '/' {
			newPwd.WriteRune('\\')
		}
		newPwd.WriteRune(r)
	}
	newPassword = newPwd.String()
	return
}
