package RocksDB_Test

import (
	"fmt"

	"github.com/tecbot/gorocksdb/test/utils"
)

func RockesDBPutBenchmark(dbPath string, nrecord int) error {
	db := &MyRocksDB{}
	if err := db.Open(dbPath); err != nil {
		return fmt.Errorf("Failed to opendb %s: %s \n", dbPath, err)
	}
	defer db.Close()

	keyPrefix := string(utils.GetRandomBytes(60))
	value := utils.GetRandomBytes(256)
	for i := 0; i < nrecord; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyPrefix, i))

		// put value to db1
		if err := db.Put(key, value); err != nil {
			return fmt.Errorf("Failed to put: %s \n", err)
		}
	}
	return nil
}
