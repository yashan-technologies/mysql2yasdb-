package runtimedef

func InitRuntime() error {
	if err := initM2YHome(); err != nil {
		return err
	}
	if err := initExecuter(); err != nil {
		return err
	}
	if err := initExecuteable(); err != nil {
		return err
	}
	return nil
}
