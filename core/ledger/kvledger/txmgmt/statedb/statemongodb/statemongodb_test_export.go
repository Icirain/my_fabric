package statemongodb

import (
	//"strings"
        "testing"
        "github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"

	"mgo-2"
//	"gopkg.in/mgo.v2/bson"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/mongodbhelper"
)


type TestDBEnv struct {
	t testing.TB
	DBProvider statedb.VersionedDBProvider
}

func NewTestDBEnv(t testing.TB) *TestDBEnv{
	t.Logf("Creating new TestDBEnv")
	dialinfo, _ := mgo.ParseURL("")
	conf := mongodbhelper.Conf{Dialinfo:dialinfo, Database_name:"mongotest", Collection_name:"test1"}
        provider := mongodbhelper.NewProvider(&conf)
	return &TestDBEnv{t:t, DBProvider:&VersionedDBProvider{dbProvider:provider}}

}

func (env *TestDBEnv) Cleanup(){
         session , err := mgo.Dial("")
	defer session.Close()
	if err != nil{
            panic("Error while link the mongo in test export")
	}
	 c := session.DB("mongotest").C("test1")
	c.DropCollection()
	env.DBProvider.Close()


}