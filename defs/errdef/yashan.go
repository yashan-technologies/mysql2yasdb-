package errdef

import "fmt"

type TransUnSupportTypeErr struct {
	MySQLType string
}

func (e TransUnSupportTypeErr) Error() string {
	return fmt.Sprintf("mysql type: %s unsupport", e.MySQLType)
}

func NewTransUnSupportTypeErr(t string) *TransUnSupportTypeErr {
	return &TransUnSupportTypeErr{
		MySQLType: t,
	}
}
