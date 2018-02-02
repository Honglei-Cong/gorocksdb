package LevelDB_Test

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tecbot/gorocksdb/test/utils"
)

func LevelDBPutBenchmark(dbPath string, nrecord int) error {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return err
	}

	defer db.Close()

	keyPrefix := string(utils.GetRandomBytes(60))
	value := utils.GetRandomBytes(256)
	for i := 0; i < nrecord; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyPrefix, i))

		// put value to db1
		if err := db.Put(key, value, nil); err != nil {
			return fmt.Errorf("Failed to put: %s \n", err)
		}
	}
	return nil
}
