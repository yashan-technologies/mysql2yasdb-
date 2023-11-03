package runtimedef

func InitRuntime() error {
	if err := initHome(); err != nil {
		return err
	}
	return nil
}
