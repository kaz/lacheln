package dummy

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"plugin"
	"time"
)

type (
	registrar = func(loader)
	loader    = func(string, generator)
	generator = func(...string) (string, error)
)

var (
	generators = map[string]generator{}
)

func init() {
	rand.Seed(time.Now().Unix())

	plugins, err := filepath.Glob("*.so")
	if err != nil {
		panic(fmt.Errorf("filepath.Glob failed: %v", err))
	}

	for _, pluginPath := range plugins {
		p, err := plugin.Open(pluginPath)
		if err != nil {
			panic(fmt.Errorf("plugin.Open failed: %v", err))
		}

		sym, err := p.Lookup("Register")
		if err != nil {
			panic(fmt.Errorf("plugin symbol Lookup failed: %v", err))
		}

		register, ok := sym.(registrar)
		if !ok {
			panic(fmt.Errorf("unexpected symbol type: %#v", sym))
		}

		register(func(key string, fn generator) { generators[key] = fn })
	}
}

func Get(key string, args ...string) (string, error) {
	generate, ok := generators[key]
	if !ok {
		return "", fmt.Errorf("No such dummy generator: %v", key)
	}
	return generate(args...)
}
