package main

import (
	"fmt"
	"math/rand"
)

func RockesDBPutBenchmark(dbPath string, nrecord int) error {
	db := &TestRocksDB{}
	if err := db.Open(dbPath); err != nil {
		return fmt.Errorf("Failed to opendb %s: %s \n", dbPath, err)
	}
	defer db.close()

	keyPrefix := string(GetRandomBytes(60))
	value := GetRandomBytes(256)
	for i := 0; i < nrecord; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyPrefix, i))

		// put value to db1
		if err := db.Put(key, value); err != nil {
			return fmt.Errorf("Failed to put: %s \n", err)
		}
	}
	return nil
}

func GetRandomBytes(n int) []byte {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

