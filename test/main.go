
package main

import (
	"github.com/tecbot/gorocksdb"
	"os"
	"path"
	"fmt"
	"time"
)

type TestRocksDB struct {
	DB *gorocksdb.DB
	cfhandle *gorocksdb.ColumnFamilyHandle
}

func (self *TestRocksDB) Open(dbPath string) error {

	err := os.MkdirAll(path.Dir(dbPath), 0755)
	if err != nil {
		panic(fmt.Sprintf("failed to making dir [%s]: %s", dbPath, err))
	}

	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()

	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	cfName := []string{"default"}
	var cfOpts []*gorocksdb.Options
	for range cfName {
		cfOpts = append(cfOpts, opts)
	}

	db, cfHandlers, err := gorocksdb.OpenDbColumnFamilies(opts, dbPath, cfName, cfOpts)
	if err != nil {
		panic(fmt.Sprintf("Failed to open db: %s", err))
	}

	self.DB = db
	self.cfhandle = cfHandlers[0]
	return nil
}

func (self *TestRocksDB) close() {
	self.cfhandle.Destroy()
	self.DB.Close()
}

func (self *TestRocksDB) Get(key []byte) ([]byte, error) {
	opt := gorocksdb.NewDefaultReadOptions()
	defer opt.Destroy()

	slice, err := self.DB.GetCF(opt, self.cfhandle, key)
	if err != nil {
		return nil, err
	}
	defer slice.Free()
	if slice.Data() == nil {
		return nil, nil
	}

	data := makeCopy(slice.Data())
	return data, nil
}

func (self *TestRocksDB) Put(key, value []byte) error {
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()

	opt.DisableWAL(true)
	return self.DB.PutCF(opt, self.cfhandle, key, value)
}

func (self *TestRocksDB) Del(key []byte) error {
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()

	opt.DisableWAL(true)
	return self.DB.DeleteCF(opt, self.cfhandle, key)
}

func (self *TestRocksDB) CreateCheckpoint(ckptPath string) error {
	if existed, err := dirExists(ckptPath); err != nil {
		return err
	} else if existed {
		return fmt.Errorf("Checkpoint path %s existed", ckptPath)
	}

	ckpt, err := self.DB.NewCheckpoint()
	if err != nil {
		return err
	}
	defer ckpt.Destroy()

	return ckpt.CreateCheckpoint(ckptPath, 0)
}

func (self *TestRocksDB) DelCheckpoint(ckptPath string) error {
	if existed, err := dirExists(ckptPath); err != nil {
		return err
	} else if !existed {
		return nil
	}

	// remove the directory
	return os.RemoveAll(ckptPath)
}

func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func makeCopy(src []byte) []byte {
	dest := make([]byte, len(src))
	copy(dest, src)
	return dest
}

func TestCheckpoint() {
	dbpath1 := "./test-db"
	dbpath2 := "./test-db2"

	db := &TestRocksDB{}
	if err := db.Open(dbpath1); err != nil {
		fmt.Printf("Failed to opendb %s: %s \n", dbpath1, err)
		return
	}
	defer db.close()

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
	db2 := &TestRocksDB{}
	if err := db2.Open(dbpath2); err != nil {
		fmt.Printf("Failed to open db %s: %s", dbpath2, err)
	}
	defer db2.close()

	// get value from db2
	if v, err := db2.Get(key); err != nil {
		fmt.Printf("failed to get: %s \n", err)
		return
	} else {
		fmt.Printf("got value: %s \n", string(v))
	}
}

func main() {
	// TestCheckpoint()

	startT := time.Now()
	RockesDBPutBenchmark("test_db1", 1000)
	fmt.Printf("time: %v \n", time.Since(startT))

}