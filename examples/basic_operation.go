package main

import (
	"fmt"
	logicKV "github.com/guanghuihuang88/logicKV"
)

func main() {
	opts := logicKV.DefaultOptions
	opts.DirPath = "/tmp/bitcask-go"
	db, err := logicKV.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
