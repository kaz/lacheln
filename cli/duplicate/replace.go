package duplicate

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

var (
	definedGenerators = map[string]func(args ...string) (string, error){}
)

func getDummy(key string, args ...string) (string, error) {
	generator, ok := definedGenerators[key]
	if !ok {
		return "", fmt.Errorf("No such dummy generator: %v", key)
	}
	return generator(args...)
}

func initGenerators() {
	rand.Seed(time.Now().Unix())

	definedGenerators["user_id"] = dummyUserID
}

func dummyUserID(args ...string) (string, error) {
	return strconv.Itoa(rand.Int()), nil
}
