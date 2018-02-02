package main

import (
	"fmt"
	"os"
	"time"

	"github.com/tecbot/gorocksdb/test/rocksdb_test"
	"github.com/tecbot/gorocksdb/test/leveldb_test"
)

func TestCheckpoint(dbpath1, dbpath2 string) {
	db := &RocksDB_Test.MyRocksDB{}
	if err := db.Open(dbpath1); err != nil {
		fmt.Printf("Failed to opendb %s: %s \n", dbpath1, err)
		return
	}
	defer db.Close()

	key := []byte("key1")
	value := []byte("value1")

	// put value to db1
	if err := db.Put(key, value); err != nil {
		fmt.Printf("Failed to put: %s \n", err)
		return
	}

	// get value from db1
	if v, err := db.Get(key); err != nil {
		fmt.Printf("failed to get: %s \n", err)
		return
	} else {
		fmt.Printf("got value: %s \n", string(v))
	}

	// create db2 as checkpoint of db1
	if err := db.CreateCheckpoint(dbpath2); err != nil {
		fmt.Printf("Create checkpoint failed: %s", err)
		return
	}

	// del value from db1
	if err := db.Del(key); err != nil {
		fmt.Printf("Failed to del: %s \n", err)
		return
	}

	// get value from db1, should return nil
	if v, err := db.Get(key); err != nil {
		fmt.Printf("failed to get: %s \n", err)
		return
	} else {
		fmt.Printf("got value: %s \n", string(v))
	}

	// open db2
	db2 := &RocksDB_Test.MyRocksDB{}
	if err := db2.Open(dbpath2); err != nil {
		fmt.Printf("Failed to open db %s: %s", dbpath2, err)
	}
	defer db2.Close()

	// get value from db2
	if v, err := db2.Get(key); err != nil {
		fmt.Printf("failed to get: %s \n", err)
		return
	} else {
		fmt.Printf("got value: %s \n", string(v))
	}
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("test checkpoint | rocksdbPut | leveldbPut")
		return
	}

	switch os.Args[1] {
	case "checkpoint":
		TestCheckpoint("./test-db", "./test-db2")

	case "rocksdbPut":
		keycount := 1000
		startT := time.Now()
		RocksDB_Test.RockesDBPutBenchmark("test_rocksdb1", keycount)
		fmt.Printf("rockies put test: count: %d, time: %v \n", keycount, time.Since(startT))

	case "leveldbPut":
		keycount := 1000
		startT := time.Now()
		LevelDB_Test.LevelDBPutBenchmark("test_leveldb1", keycount)
		fmt.Printf("leveldb put test: count: %d, time: %v \n", keycount, time.Since(startT))

	default:
		fmt.Printf("unknown operation: %s", os.Args[1])
	}
}
