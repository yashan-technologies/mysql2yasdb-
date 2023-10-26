package confdef

import (
	"errors"

	"github.com/BurntSushi/toml"
)

const (
	key_schemas            = "schemas"
	key_parallel           = "parallel"
	key_parallel_per_table = "parallel_per_table"
)

var (
	ErrParallel                   = errors.New("parallel的取值范围是1-8,请检查配置文件")
	ErrParallelPerTable           = errors.New("parallel_per_table的取值范围是1-8,请检查配置文件")
	ErrSchemasAndTablesAllExist   = errors.New("schemas 和 tables 这两个参数不能同时配置,请检查配置文件")
	ErrSchemasAndTablesAtLeastOne = errors.New("schemas 和 tables 这两个参数至少需要配置一个,请检查配置文件")
	ErrNeedRemapSchemas           = errors.New("需要配置remap_schemas,指定在崖山要导入的用户,请检查配置文件")
	ErrRemapSchema                = errors.New("需要配置remap_schemas,指定在崖山要导入的用户,请检查配置文件")
)

var _config M2YConfig

type MysqlConfig struct {
	Host             string   `toml:"host"`
	Port             int      `toml:"port"`
	Database         string   `toml:"database"`
	UserName         string   `toml:"username"`
	Password         string   `toml:"password"`
	Schemas          []string `toml:"schemas"`
	Tables           []string `toml:"tables"`
	ExcludeTables    []string `toml:"exclude_tables"`
	QueryStr         string   `toml:"query_str"`
	Parallel         int      `toml:"parallel"              default:"1"`
	ParallelPerTable int      `toml:"parallel_per_table"    default:"1"`
	BatchSize        int      `toml:"batch_size"            default:"1000"`
}

type YashanConfig struct {
	Host         string   `toml:"host"`
	Port         int      `toml:"port"`
	Database     string   `toml:"database"`
	UserName     string   `toml:"username"`
	Password     string   `toml:"password"`
	RemapSchemas []string `toml:"remap_schemas"`
}

type M2YConfig struct {
	Mysql  *MysqlConfig  `toml:"mysql"`
	Yashan *YashanConfig `toml:"yashandb"`
}

func InitConfig(config string) (err error) {
	conf := &M2YConfig{}
	if _, err = toml.DecodeFile(config, conf); err != nil {
		return
	}
	if err = conf.validateAndFillDefault(); err != nil {
		return
	}
	_config = *conf
	return
}

func (conf *M2YConfig) validateAndFillDefault() (err error) {
	if err = conf.Mysql.validateAndFillMysql(); err != nil {
		return
	}
	if err = conf.Yashan.validateAndFillYashandb(); err != nil {
		return
	}
	return
}

func (conf *MysqlConfig) validateAndFillMysql() (err error) {
	if err := conf.validateAndFillSchemasAndTables(); err != nil {
		return err
	}
	if err := conf.validateAndFillParallel(); err != nil {
		return err
	}
	conf.validateAndFillBatchSize()
	return
}

func (conf *MysqlConfig) validateAndFillSchemasAndTables() (err error) {
	lenTables := len(conf.Tables)
	lenSchemas := len(conf.Schemas)
	if lenSchemas != 0 && lenTables != 0 {
		err = ErrSchemasAndTablesAllExist
		return
	}
	if lenSchemas == 0 && lenTables == 0 {
		err = ErrSchemasAndTablesAtLeastOne
		return
	}
	return
}

func (conf *MysqlConfig) validateAndFillParallel() (err error) {
	getParallel := func(parallel_set int, key string) (parallel int, err error) {
		parallel = 1
		if parallel_set >= 0 && parallel_set <= 8 {
			if parallel_set > 0 {
				parallel = parallel_set
			}
		} else {
			if key == key_schemas {
				err = ErrParallel
			} else {
				err = ErrParallelPerTable
			}
		}
		return
	}
	conf.Parallel, err = getParallel(conf.Parallel, key_parallel)
	if err != nil {
		return
	}
	conf.ParallelPerTable, err = getParallel(conf.ParallelPerTable, key_parallel_per_table)
	if err != nil {
		return
	}
	return
}

func (conf *MysqlConfig) validateAndFillBatchSize() (batchSize int) {
	batchSize = 1
	commitSize_set := conf.BatchSize
	if commitSize_set > 1 {
		batchSize = commitSize_set
	}
	conf.BatchSize = batchSize
	return
}

func (conf *YashanConfig) validateAndFillYashandb() (err error) {
	if len(conf.RemapSchemas) == 0 {
		err = ErrRemapSchema
		return
	}
	return
}

func GetM2yConfig() *M2YConfig {
	return &_config
}
