package main

import (
	"fmt"
	"os"

	"github.com/tecbot/gorocksdb/test/leveldb_test"
	"github.com/tecbot/gorocksdb/test/rocksdb_test"
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
		fmt.Println("test checkpoint | rocksdbPut | leveldbPut | rocksdbGet | leveldbGet")
		return
	}

	switch os.Args[1] {
	case "checkpoint":
		TestCheckpoint("./test-db", "./test-db2")

	case "rocksdbPut":
		keycount := 1000
		if err := RocksDB_Test.RocksDBPutBenchmark("test_rocksdb1", keycount); err != nil {
			fmt.Printf("rocksdbPut test failed: %s \n", err)
		}

	case "leveldbPut":
		keycount := 1000
		if err := LevelDB_Test.LevelDBPutBenchmark("test_leveldb1", keycount); err != nil {
			fmt.Printf("leveldbPut test failed: %s \n", err)
		}

	case "rocksdbGet":
		keycount := 1000
		if err := RocksDB_Test.RocksDBGetBenchmark("test_rocksdb1", keycount); err != nil {
			fmt.Printf("rocksdbGet test failed: %s \n", err)
		}

	case "leveldbGet":
		keycount := 1000
		if err := LevelDB_Test.LevelDBGetBenchmark("test_leveldb1", keycount); err != nil {
			fmt.Printf("leveldbGet test failed: %s \n", err)
		}

	case "rocksdbOverwrite":
	case "leveldbOverwrite":

	default:
		fmt.Printf("unknown operation: %s", os.Args[1])
	}
}
