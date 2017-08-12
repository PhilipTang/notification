package config

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/go-yaml/yaml"
)

var (
	MyConfig Config
	path     string
)

func init() {
	fn := "init"
	flag.StringVar(&path, "config", "config/config.yaml", "config file's path")
	if err := load(path); err != nil {
		printErrorAndExit(69, "@%s, reload config failed, err=%s", fn, err)
	}
}

func load(path string) (err error) {
	fn := "load"
	var content []byte
	if content, err = ioutil.ReadFile(path); err != nil {
		printErrorAndExit(69, "@%s, ioutil.ReadFile failed, err=%s", fn, err)
	}
	if err = yaml.Unmarshal(content, &MyConfig); err != nil {
		printErrorAndExit(69, "@%s, yaml.Unmarshal failed, err=%s", fn, err)
	}
	return
}

type Config struct {
	Redis Redis
}

type Redis struct {
	Server    string
	Password  string
	DB        int
	MaxIdle   int
	MaxActive int
	Protocol  string
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}
