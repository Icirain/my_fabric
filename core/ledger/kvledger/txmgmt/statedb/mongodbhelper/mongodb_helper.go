package mongodbhelper

import (
	"gopkg.in/mgo.v2"
	"labix.org/v2/mgo/bson"
	"fmt"
	"github.com/hyperledger/fabric/common/flogging"
	"sync"
	//"encoding/json"
)

type Kv_pair struct {
	Key string
	Value *Value
}

type Value struct{
	Value_content interface{}
	Field_map map[string]string
	Attachments []byte

}

type Attachment struct {
	Name            string
	ContentType     string
	Length          uint64
	AttachmentBytes []byte
}

var logger = flogging.MustGetLogger("mongodbhelper")

type dbState int32

const(
	closed dbState = iota
	opened
)

type DB struct {
	conf *Conf
	dbstate dbState
	mux sync.Mutex
	session *mgo.Session
}

type Conf struct {
	Dialinfo *mgo.DialInfo
	Collection_name string
	Database_name string
}



func CreateDB(conf *Conf)*DB  {
	 return &DB{
		 dbstate: closed,
		 conf: conf}


}

func (dbInst *DB) Open(){
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if(dbInst.dbstate == opened){
		return
	}
	var err error
	dbInst.session ,err = mgo.DialWithInfo(dbInst.conf.Dialinfo)
	if err != nil{
		panic(fmt.Sprintf("Error while trying to activate the session to the configured database ： %s", err))
	}
	dbInst.dbstate = opened
}

func (dbInst *DB) Close(){
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.dbstate == closed{
		return
	}
	dbInst.session.Close()
	dbInst.dbstate = closed
}

func (dbInst *DB) Get(key string) ([]byte, error){
       collection_inst := dbInst.session.DB(dbInst.conf.Database_name).C(dbInst.conf.Collection_name)
       query_result := collection_inst.Find(bson.M{"key" : key})
       num, err := query_result.Count()
       var result Kv_pair

	if err != nil{
	       logger.Error(err)
	       return nil, err
       }

       if num == 0{
	       logger.Debugf("The corresponding value of this key: %s doesn't exist", key)
	       return nil, err
       }else if num != 1 {
	       logger.Error("This key:%s is repeated in the current collection", key)
	       return nil, fmt.Errorf("The key %s is repeated", key)

       }else{
	       logger.Infof("The corresponding value of this key: %s is extracted successfully", key)
       }
       query_result.One(&result)
       out , err := bson.Marshal(result.Value)
       if err != nil{
	       logger.Error("Error in marshal result %s", err.Error())
	       return nil, err
       }
       return out , nil



}

func (dbInst *DB) Put(key string, value []byte) (error) {
	collecion_inst := dbInst.session.DB(dbInst.conf.Database_name).C(dbInst.conf.Collection_name)
	var out Value
	err := bson.Unmarshal(value, &out)
	if err != nil{
		logger.Errorf("Error in unmarshaling the bytes of value: %s", err.Error())
		return err
	}
	err = collecion_inst.Insert(&Kv_pair{key, &out})
	if err != nil{
		logger.Errorf("Error in insert the content of key : %s, error : %s",key, err.Error())
		return err
	}
	return nil

}

func (dbInst *DB) Delete(key string) error{
	collection_inst := dbInst.session.DB(dbInst.conf.Database_name).C(dbInst.conf.Collection_name)
	err := collection_inst.Remove(bson.M{"key" : key})
	if err != nil{
		logger.Errorf("Error %s happened while delete key %s", err.Error(), key)
		return err
	}
	return nil
}


func (dbInst *DB) GetIterator(startkey string, endkey string, querylimit int , queryskip int) *mgo.Iter{
	collection_inst := dbInst.session.DB(dbInst.conf.Database_name).C(dbInst.conf.Collection_name)
	query_result := collection_inst.Find(bson.M{"key" : bson.M{"$gte" : startkey, "$lt" : endkey}})
	query_result = query_result.Skip(queryskip)
	query_result = query_result.Limit(querylimit)
	count, _ := query_result.Count()
        query_result.Sort("key")
	fmt.Println(count)
	return query_result.Iter()

}




