package mongodbhelper

import (
	"sync"
	"bytes"
	"gopkg.in/mgo.v2"
)

var dbNameKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)

type Provider struct {
	db *DB
	dbHandles map[string]*DBHandle
	mux sync.Mutex
}

type DBHandle struct {
	DbName string
	Db *DB
}

func NewProvider(conf *Conf) *Provider{
	db := CreateDB(conf)
	db.Open()
        return &Provider{db, make(map[string]*DBHandle), sync.Mutex{}}
}

func (p *Provider) GetDBHandle(dbName string) *DBHandle{
	p.mux.Lock()
	defer p.mux.Unlock()
	dbHandle := p.dbHandles[dbName]
	if dbHandle == nil{
		dbHandle = &DBHandle{dbName, p.db}
		p.dbHandles[dbName] = dbHandle
	}
	return dbHandle
}

func (p *Provider) Close(){
	p.db.Close()
}

func (h *DBHandle) Get(key []byte)([]byte, error){
	return h.Db.Get(string(constructMongoKey(h.DbName, key)))


}

func (h *DBHandle) Put(key []byte, value []byte)(error){
	return h.Db.Put(string(constructMongoKey(h.DbName,key)), value)
}

func (h *DBHandle) Delete(key []byte)(error){
	return h.Db.Delete(string(constructMongoKey(h.DbName, key)))
}


func retrieveAppKey(levelKey []byte) []byte {
	return bytes.SplitN(levelKey, dbNameKeySep, 2)[1]
}
func constructMongoKey(dbName string, key []byte) []byte{
      return append(append([]byte(dbName),dbNameKeySep...), key...)
}

func (h *DBHandle) GetIterator(startKey []byte, endKey []byte, querylimit int , queryskip int) *mgo.Iter {
	sKey := constructMongoKey(h.DbName, startKey)
	eKey := constructMongoKey(h.DbName, endKey)
	if endKey == nil {
		// replace the last byte 'dbNameKeySep' by 'lastKeyIndicator'
		eKey[len(eKey)-1] = lastKeyIndicator
	}
	logger.Debugf("Getting iterator for range [%#v] - [%#v]", string(sKey), string(eKey))
	return h.Db.GetIterator(string(sKey), string(eKey), querylimit, queryskip)
}


