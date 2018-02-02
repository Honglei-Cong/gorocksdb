package RocksDB_Test

import (
	"fmt"
	"os"
	"path"

	"github.com/tecbot/gorocksdb"
	"github.com/tecbot/gorocksdb/test/utils"
)

type MyRocksDB struct {
	DB       *gorocksdb.DB
	cfhandle *gorocksdb.ColumnFamilyHandle
}

func (self *MyRocksDB) Open(dbPath string) error {

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

func (self *MyRocksDB) Close() {
	self.cfhandle.Destroy()
	self.DB.Close()
}

func (self *MyRocksDB) Get(key []byte) ([]byte, error) {
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

	data := utils.MakeCopy(slice.Data())
	return data, nil
}

func (self *MyRocksDB) Put(key, value []byte) error {
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()

	opt.DisableWAL(true)
	return self.DB.PutCF(opt, self.cfhandle, key, value)
}

func (self *MyRocksDB) Del(key []byte) error {
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()

	opt.DisableWAL(true)
	return self.DB.DeleteCF(opt, self.cfhandle, key)
}

func (self *MyRocksDB) CreateCheckpoint(ckptPath string) error {
	if existed, err := utils.DirExists(ckptPath); err != nil {
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

func (self *MyRocksDB) DelCheckpoint(ckptPath string) error {
	if existed, err := utils.DirExists(ckptPath); err != nil {
		return err
	} else if !existed {
		return nil
	}

	// remove the directory
	return os.RemoveAll(ckptPath)
}
