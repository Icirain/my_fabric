package statemongodb

import (
	"bytes"
	"errors"
	"gopkg.in/mgo.v2"
	"labix.org/v2/mgo/bson"
        "test_demo_go/mongodbhelper"
	"github.com/hyperledger/fabric/common/flogging"
	//"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"encoding/json"
	"strconv"
	//"github.com/hyperledger/fabric/common/configtx/tool/localconfig"
	"unicode/utf8"
	"fmt"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"

	//"reflect"
	//"github.com/hyperledger/fabric/gossip/state"
)


var logger = flogging.MustGetLogger("statemongodb")

var compositeKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)
var savePointKey = []byte{0x00}


var queryskip = 0

type VersionedDBProvider struct {
	dbProvider *mongodbhelper.Provider
}

type VersionedDB struct {
	db *mongodbhelper.DBHandle
	dbName string
}

func NewVersionedDBProvider() *VersionedDBProvider{
	logger.Debugf("Constrcuting MongDB provider")
	conf := GetMongoDBConf()
	provider := mongodbhelper.NewProvider(conf)
	return &VersionedDBProvider{dbProvider:provider}
}


func (provider *VersionedDBProvider) GetDBHandle(dbName string) (statedb.VersionedDB, error){
	return &VersionedDB{provider.dbProvider.GetDBHandle(dbName), dbName}, nil
}
func (provider *VersionedDBProvider) Close(){
	provider.dbProvider.Close()
}

func (vdb *VersionedDB) Close(){
	vdb.db.Db.Close()
}

func (vdb *VersionedDB)Open() error{
        vdb.db.Db.Open()
	return nil
}

func (vdb *VersionedDB) GetState(namespace string, key string)(*statedb.VersionedValue, error){
	logger.Debugf("GetState(). ns=%s, key = %s",namespace, key)
	compositeKey := constructCompositeKey(namespace, key)
	dbVal , err := vdb.db.Get(compositeKey)
	if err != nil{
		return nil, err
	}
	if dbVal == nil{
		return nil, nil
	}

	value := mongodbhelper.Value{}
	err = bson.Unmarshal(dbVal, &value)
	versioned_v := statedb.VersionedValue{Value:nil, Version:&version.Height{},}
	if err != nil{
		logger.Errorf("Error rises while unmarshal the value returned %s", err.Error())
		return nil, err
	}
	//fmt.Println(value)
	if value.Value_content == nil && value.Attachments != nil{
		versioned_v.Value = value.Attachments
		//fmt.Println(reflect.TypeOf(value.Field_map["blockNum"]))
		//fmt.Println(strconv.ParseUint(value.Field_map["blockNum"], 10, 64))
		versioned_v.Version.BlockNum, _ = strconv.ParseUint(value.Field_map["blockNum"],10, 64)
		versioned_v.Version.TxNum, _ = strconv.ParseUint(value.Field_map["txNum"], 10, 64)


	}else{

		tempbyte , _ := json.Marshal(value.Value_content)
		//fmt.Println(string(tempbyte))
		versioned_v.Value = []byte(string(tempbyte))

		versioned_v.Version.BlockNum, _ = strconv.ParseUint(value.Field_map["blockNum"],10, 64)
		versioned_v.Version.TxNum, _ = strconv.ParseUint(value.Field_map["txNum"], 10, 64)
	}
	return &versioned_v, nil
}

func (vdb *VersionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	vals := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		val, err := vdb.GetState(namespace, key)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

func (vdb *VersionedDB) ValidateKey(key string) error {
	if !utf8.ValidString(key) {
		return fmt.Errorf("Key should be a valid utf8 string: [%x]", key)
	}
	return nil
}

func (vdb *VersionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	compositeStartKey := constructCompositeKey(namespace, startKey)
	compositeEndKey := constructCompositeKey(namespace, endKey)
	querylimit := ledgerconfig.GetQueryLimit()
	if endKey == "" {
		compositeEndKey[len(compositeEndKey)-1] = lastKeyIndicator
	}
	dbItr := vdb.db.GetIterator(compositeStartKey, compositeEndKey, querylimit, queryskip)
	return newKVScanner(dbItr, namespace), nil
}

func (vdb *VersionedDB) ExecuteQuery(namespace string, query string)(statedb.ResultsIterator, error){
      return nil, errors.New("ExecuteQuery not yet implemented for mongo")
}

func(vdb VersionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error{
      namespaces := batch.GetUpdatedNamespaces()
      var out interface{}
      var err error
      for _, ns := range namespaces{
	  updates := batch.GetUpdates(ns)
	  for k, vv := range updates{
		  compositeKey := constructCompositeKey(ns, k)
		  logger.Debugf("Channel [%s]: Applying key=[%#v]", vdb.dbName, compositeKey)

		  if vv.Value == nil{
			  vdb.db.Delete(compositeKey)
		  }else{
			value := mongodbhelper.Value{Value_content:nil, Field_map:map[string]string{}, Attachments:nil}
			value.Field_map["blockNum"] = fmt.Sprintf("%v", vv.Version.BlockNum)
			value.Field_map["txNum"] = fmt.Sprintf("%v", vv.Version.TxNum)
			if !isJson(vv.Value){
			     logger.Infof("Not a json, write it to the attachment ")
			     value.Attachments = vv.Value
			     value.Value_content = nil
			}else{
				err = json.Unmarshal(vv.Value, &out)
				//fmt.Println(out, "in apply")
				if err != nil{
					logger.Errorf("Error rises while unmarshal the vv.value, error :%s", err.Error())
				}
				value.Value_content = out
			}
			value_to_put , err := bson.Marshal(&value)
			if err != nil{
				logger.Errorf("Error rises while marshal the value content, error :%s", err.Error())
			}
			  err = vdb.db.Put(compositeKey, value_to_put)
			if err != nil {
				logger.Errorf("Error during Commit(): %s\n", err.Error())

			}



		  }
	  }

      }

      err = vdb.recordSavepoint(height)
      if err != nil{
	      logger.Errorf("Error during recordSavepoint : %s\n", err.Error())
	      return err
      }
      return nil



}

func (vdb *VersionedDB) recordSavepoint(height *version.Height) error{
	err := vdb.db.Delete([]byte(savePointKey))
	if err != nil{
		logger.Errorf("Error during delete old savepoint , error : %s", err.Error())
	}
	value := mongodbhelper.Value{Value_content:nil, Field_map:map[string]string{}, Attachments:nil}
	value.Field_map["txNum"] = fmt.Sprintf("%v",height.TxNum)
	value.Field_map["blockNum"] = fmt.Sprintf("%v", height.BlockNum)
	out , err := bson.Marshal(&value)
	if err != nil{
		logger.Errorf("Error during marshal the savepoint, error : %s", err.Error())
		return err
	}
	err = vdb.db.Put([]byte(savePointKey), out)
	if err != nil{
		logger.Errorf("Error during update savepoint , error : $s", err.Error())
		return err
	}
	return err


}

func (vdb *VersionedDB) GetLatestSavePoint() (*version.Height, error){
	result, err := vdb.db.Get([]byte(savePointKey))
	if err != nil{
		logger.Errorf("Error during get latest save point key, error : %s", err.Error())
		return nil, err
	}
	if result == nil{
		return nil, nil
	}
	value := mongodbhelper.Value{}
	err = bson.Unmarshal(result, &value)
	if err != nil{
		logger.Errorf("Error during extract savepoint, error : %s", err.Error())
	}
	txNum, _ := strconv.ParseUint(value.Field_map["txNum"],10, 64)
	blockNum, _ := strconv.ParseUint(value.Field_map["blockNum"],10, 64)
	return version.NewHeight(blockNum, txNum), nil


}

type kvScanner struct {
	namespace string
	result *mgo.Iter
}

func (scanner *kvScanner) Next() (statedb.QueryResult, error) {
         kv_pair := mongodbhelper.Kv_pair{}
	 if !scanner.result.Next(&kv_pair){
		 return nil, nil
	 }
	 blockNum, _ := strconv.ParseUint(kv_pair.Value.Field_map["blockNum"],10, 64)
	 txNum, _ := strconv.ParseUint(kv_pair.Value.Field_map["txNum"],10, 64)
	 height := version.NewHeight(blockNum, txNum)
         _, key := splitCompositeKey([]byte(kv_pair.Key))
	 _, key = splitCompositeKey([]byte(key))
	 value := kv_pair.Value
	 value_content := []byte{}
	 if value.Value_content != nil{
		 value_content, _ = json.Marshal(value.Value_content)
	 }else{
		 value_content = value.Attachments
	 }

	 return &statedb.VersionedKV{
		CompositeKey:   statedb.CompositeKey{Namespace: scanner.namespace, Key: key},
		VersionedValue: statedb.VersionedValue{Value:value_content , Version: height}}, nil


}

func(scanner *kvScanner) Close(){
	err := scanner.result.Close()
	if err != nil{
		logger.Errorf("Error during close the iterator of scanner error : %s", err.Error())
	}

}

func newKVScanner(iter *mgo.Iter, namespace string) *kvScanner{
	return &kvScanner{namespace:namespace, result:iter}
}

func constructCompositeKey(ns string, key string) []byte {
	return append(append([]byte(ns), compositeKeySep...), []byte(key)...)
}

func splitCompositeKey(compositeKey []byte) (string, string) {
	split := bytes.SplitN(compositeKey, compositeKeySep, 2)
	return string(split[0]), string(split[1])
}

func isJson(value []byte) bool{
	var result interface{}
	err := json.Unmarshal(value, &result)
	return err == nil
}


