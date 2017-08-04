package statemongodb

import (
	"testing"
	//"time"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"fmt"
	ledgertestutil "github.com/hyperledger/fabric/core/ledger/testutil"
)






func TestGetCouchDBDefinition(t *testing.T) {
	ledgertestutil.SetupCoreYAMLConfig()
	conf := GetMongoDBConf()
	testutil.AssertEquals(t, conf.Collection_name, "test_base")
	testutil.AssertEquals(t, conf.Database_name, "stateMongo_test")
	couch := couchdb.GetCouchDBDefinition()
	fmt.Println(couch)

}

