package confdef

import (
	"errors"
	"path"

	"m2y/defs/errdef"
	"m2y/defs/runtimedef"

	"git.yasdb.com/go/yasutil/fs"
	"github.com/BurntSushi/toml"
)

var (
	ErrSchemasAndTablesAllExist   = errors.New("schemas 和 tables 这两个参数不能同时配置, 请检查配置文件")
	ErrSchemasAndTablesAtLeastOne = errors.New("schemas 和 tables 这两个参数至少需要配置一个, 请检查配置文件")
	ErrNeedRemapSchemas           = errors.New("需要配置remap_schemas, 指定在崖山要导入的用户, 请检查配置文件")
	ErrRemapSchema                = errors.New("需要配置remap_schemas, 指定在崖山要导入的用户, 请检查配置文件")
	ErrSampleLines                = errors.New("需要配置sample_lines, 指定数据校验时单表的随机采样行数, 参数大于等于0, 为0表示全表校验")
)

var (
	DefaultParallel         = 1
	DefaultParallelPerTable = 1
	DefaultBatchSize        = 1000
	DefaultSampleLine       = 1000

	MaxParallel = 8
)

var _config M2YConfig

type MySQLConfig struct {
	Host              string   `toml:"host"`
	Port              int      `toml:"port"`
	Database          string   `toml:"database"`
	UserName          string   `toml:"username"`
	Password          string   `toml:"password"`
	Schemas           []string `toml:"schemas"`
	Tables            []string `toml:"tables"`
	ExcludeTables     []string `toml:"exclude_tables"`
	QueryStr          string   `toml:"query_str"`
	Parallel          int      `toml:"parallel"              default:"1"`
	ParallelPerTable  int      `toml:"parallel_per_table"    default:"1"`
	BatchSize         int      `toml:"batch_size"            default:"1000"`
	SampleLines       int      `toml:"sample_lines"          default:"1000"`
	RowsOnly          bool     `toml:"rows_only"`
	NoCheckNullString bool     `toml:"no_check_null_string"`
}

type YashanConfig struct {
	Host          string   `toml:"host"`
	Port          int      `toml:"port"`
	Database      string   `toml:"database"`
	UserName      string   `toml:"username"`
	Password      string   `toml:"password"`
	RemapSchemas  []string `toml:"remap_schemas"`
	CaseSensitive bool     `toml:"case_sensitive"`
}

type M2YConfig struct {
	LogLevel string        `toml:"log_level"`
	MySQL    *MySQLConfig  `toml:"mysql"`
	Yashan   *YashanConfig `toml:"yashandb"`
}

func InitM2YConfig(config string) error {
	conf := &M2YConfig{}
	if !path.IsAbs(config) {
		config = path.Join(runtimedef.GetM2YHome(), config)
	}
	if !fs.IsFileExist(config) {
		return &errdef.ErrFileNotFound{FName: config}
	}
	if _, err := toml.DecodeFile(config, conf); err != nil {
		return err
	}
	if err := conf.validate(); err != nil {
		return err
	}
	_config = *conf
	return nil
}

func GetM2YConfig() *M2YConfig {
	return &_config
}

func (c *M2YConfig) validate() error {
	if len(c.Yashan.RemapSchemas) == 0 {
		return ErrNeedRemapSchemas
	}
	if len(c.MySQL.Schemas) == 0 && len(c.MySQL.Tables) == 0 {
		return ErrSchemasAndTablesAtLeastOne
	}
	if len(c.MySQL.Schemas) > 0 && len(c.MySQL.Tables) > 0 {
		return ErrSchemasAndTablesAllExist
	}
	return nil
}
