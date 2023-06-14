package main

import (
	"encoding/json"
	"fmt"
	logicKV "github.com/guanghuihuang88/logicKV"
	"log"
	"net/http"
	"os"
)

var db *logicKV.DB

func init() {
	// 初始化 DB 实例
	var err error
	options := logicKV.DefaultOptions
	dir, err := os.MkdirTemp("", "logicKV-http")
	options.DirPath = dir
	db, err = logicKV.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}

	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put value in db: %v\n", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}

	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil && err != logicKV.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get key in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}

	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil && err != logicKV.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get key in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}

	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}

	_ = json.NewEncoder(writer).Encode(result)
}

func handleListStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	// 注册处理方法
	http.HandleFunc("/logicKV/put", handlePut)
	http.HandleFunc("/logicKV/get", handleGet)
	http.HandleFunc("/logicKV/delete", handleDelete)
	http.HandleFunc("/logicKV/listkeys", handleListKeys)
	http.HandleFunc("/logicKV/stat", handleListStat)
	// 启动http服务
	http.ListenAndServe("localhost:8080", nil)
}
