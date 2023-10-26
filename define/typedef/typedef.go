package typedef

import "m2y/define/errdef"

// mysql type
const (
	M_TINYINT            string = "tinyint"
	M_SMALLINT           string = "smallint"
	M_MEDIUMINT          string = "mediumint"
	M_INT                string = "int"
	M_BIGINT             string = "bigint"
	M_DECIMAL            string = "decimal"
	M_FLOAT              string = "float"
	M_DOUBLE             string = "double"
	M_BIT                string = "bit"
	M_DATE               string = "date"
	M_DATETIME           string = "datetime"
	M_TIMESTAMP          string = "timestamp"
	M_TIME               string = "time"
	M_YEAR               string = "year"
	M_CHAR               string = "char"
	M_VARCHAR            string = "varchar"
	M_NCHAR              string = "nchar"
	M_NVARCHAR           string = "nvarchar"
	M_BINARY             string = "binary"
	M_VARBINARY          string = "varbinary"
	M_TINYBLOB           string = "tinyblob"
	M_TINYTEXT           string = "tinytext"
	M_BLOB               string = "blob"
	M_TEXT               string = "text"
	M_MEDIUMBLOB         string = "mediumblob"
	M_MEDIUMTEXT         string = "mediumtext"
	M_LONGBLOB           string = "longblob"
	M_LONGTEXT           string = "longtext"
	M_JSON               string = "json"
	M_ENUM               string = "enum"
	M_SET                string = "set"
	M_TINYINT_UNSIGNED   string = "tinyint unsigned"
	M_SMALLINT_UNSIGNED  string = "smallint unsigned"
	M_MEDIUMINT_UNSIGNED string = "mediumint unsigned"
	M_INT_UNSIGNED       string = "int unsigned"
	M_BIGINT_UNSIGNED    string = "bigint unsigned"
	M_GEOMETRY           string = "geometry"
)

// yashandb type
const (
	Y_SMALLINT  string = "smallint"
	Y_INTEGER   string = "integer"
	Y_BIGINT    string = "bigint"
	Y_NUMBER    string = "number"
	Y_FLOAT     string = "float"
	Y_DOUBLE    string = "double"
	Y_BIT       string = "bit"
	Y_TIMESTAMP string = "timestamp"
	Y_TIME      string = "time"
	Y_DATE      string = "date"
	Y_CHAR      string = "char"
	Y_VARCHAR   string = "varchar"
	Y_NCHAR     string = "nchar"
	Y_NVARCHAR  string = "nvarchar"
	Y_RAW       string = "raw"
	Y_BLOB      string = "blob"
	Y_CLOB      string = "clob"
	Y_JSON      string = "json"
	Y_GEOMETRY  string = "geometry"
)

// mysql to yashan map
var (
	_DataTypeMap = map[string]string{
		M_TINYINT:            Y_SMALLINT,
		M_SMALLINT:           Y_INTEGER,
		M_MEDIUMINT:          Y_INTEGER,
		M_INT:                Y_BIGINT,
		M_BIGINT:             Y_NUMBER,
		M_DECIMAL:            Y_NUMBER,
		M_FLOAT:              Y_FLOAT,
		M_DOUBLE:             Y_DOUBLE,
		M_BIT:                Y_BIT,
		M_DATE:               Y_DATE,
		M_DATETIME:           Y_TIMESTAMP,
		M_TIMESTAMP:          Y_TIMESTAMP,
		M_TIME:               Y_TIME,
		M_YEAR:               Y_DATE,
		M_CHAR:               Y_CHAR,
		M_VARCHAR:            Y_VARCHAR,
		M_NCHAR:              Y_NCHAR,
		M_NVARCHAR:           Y_NVARCHAR,
		M_BINARY:             Y_RAW,
		M_VARBINARY:          Y_RAW,
		M_TINYBLOB:           Y_BLOB,
		M_TINYTEXT:           Y_CLOB,
		M_BLOB:               Y_BLOB,
		M_TEXT:               Y_CLOB,
		M_MEDIUMBLOB:         Y_BLOB,
		M_MEDIUMTEXT:         Y_CLOB,
		M_LONGBLOB:           Y_BLOB,
		M_LONGTEXT:           Y_CLOB,
		M_JSON:               Y_JSON,
		M_ENUM:               Y_VARCHAR,
		M_SET:                Y_VARCHAR,
		M_TINYINT_UNSIGNED:   Y_SMALLINT,
		M_SMALLINT_UNSIGNED:  Y_INTEGER,
		M_MEDIUMINT_UNSIGNED: Y_INTEGER,
		M_INT_UNSIGNED:       Y_BIGINT,
		M_BIGINT_UNSIGNED:    Y_NUMBER,
		M_GEOMETRY:           Y_GEOMETRY,
	}
)

func MysqlToYasType(t string) (yas string, err error) {
	yas, ok := _DataTypeMap[t]
	if !ok {
		err = errdef.NewTransUnSupportTypeErr(string(t))
		return
	}
	return
}
