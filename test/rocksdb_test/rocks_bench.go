package RocksDB_Test

import (
	"bytes"
	"fmt"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/tecbot/gorocksdb/test/utils"
)

func putDataBenchmark(db *MyRocksDB, keyprefix string, start, end int, value []byte) error {
	for i := start; i < end; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyprefix, i))

		// put value to db1
		if err := db.Put(key, value); err != nil {
			return fmt.Errorf("Failed to put: %s \n", err)
		}
	}

	return nil
}

func getDataBenchmark(db *MyRocksDB, keyprefix string, start, end int, targetValue []byte) error {
	for i := start; i < end; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyprefix, i))

		// put value to db1
		if v, err := db.Get(key); err != nil {
			return fmt.Errorf("Failed to put: %s \n", err)
		} else if targetValue != nil {
			if bytes.Compare(v, targetValue) != 0 {
				return errors.New("Get Data verify failed")
			}
		}
	}

	return nil
}

func RocksDBPutBenchmark(dbPath string, nrecord int) error {
	db := &MyRocksDB{}
	if err := db.Open(dbPath); err != nil {
		return fmt.Errorf("Failed to opendb %s: %s \n", dbPath, err)
	}
	defer db.Close()

	keyPrefix := string(utils.GetRandomBytes(60))
	value := utils.GetRandomBytes(256)

	startT := time.Now()
	if err := putDataBenchmark(db, keyPrefix, 0, nrecord, value); err != nil {
		return err
	}
	fmt.Printf("rocks put test: count: %d, time: %v \n", nrecord, time.Since(startT))

	return nil
}

func RocksDBGetBenchmark(dbPath string, nrecord int) error {
	db := &MyRocksDB{}
	if err := db.Open(dbPath); err != nil {
		return fmt.Errorf("Failed to opendb %s: %s \n", dbPath, err)
	}
	defer db.Close()

	keyPrefix := string(utils.GetRandomBytes(60))
	value := utils.GetRandomBytes(256)

	startT := time.Now()
	if err := putDataBenchmark(db, keyPrefix, 0, nrecord*3, value); err != nil {
		return err
	}
	fmt.Printf("rocks put test: count: %d, time: %v \n", nrecord*3, time.Since(startT))

	startT = time.Now()
	if err := getDataBenchmark(db, keyPrefix, nrecord, nrecord*2, nil); err != nil {
		return err
	}
	fmt.Printf("rocks Get test: count: %d, time: %v \n", nrecord, time.Since(startT))

	return nil
}
