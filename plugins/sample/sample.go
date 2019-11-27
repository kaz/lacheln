package main

import (
	"math/rand"
	"strconv"
	"time"
)

type (
	loader    = func(string, generator)
	generator = func(...string) (string, error)
)

func Register(load loader) {
	load("id", id)
	load("date", date)
}

func id(args ...string) (string, error) {
	return strconv.Itoa(rand.Int()), nil
}
func date(args ...string) (string, error) {
	return time.Unix(rand.Int63(), rand.Int63()).Format("2006-01-02 15:04:05"), nil
}
