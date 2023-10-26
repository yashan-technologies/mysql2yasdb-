package errdef

import "fmt"

type TransUnSupportTypeErr struct {
	MysqlType string
}

func (e TransUnSupportTypeErr) Error() string {
	return fmt.Sprintf("mysql type: %s unsupport", e.MysqlType)
}

func NewTransUnSupportTypeErr(t string) *TransUnSupportTypeErr {
	return &TransUnSupportTypeErr{
		MysqlType: t,
	}
}
