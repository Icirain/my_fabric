package mongodbhelper
import (
//	"fmt"
	"testing"
      //  "labix.org/v2/mgo/bson"
	"github.com/hyperledger/fabric/common/ledger/testutil"
)

func TestDBBasicWriteAndReads(t *testing.T){
	testDBBasicWriteAndReads(t, "db1", "db2", "")
}
func testDBBasicWriteAndReads(t *testing.T, dbNames ...string) {
	env := newTestProviderEnv(t, testDBPath)
	defer env.cleanup()
	p := env.provider

	for _, dbName := range dbNames {
		db := p.GetDBHandle(dbName)
		db.Put([]byte("key1" + dbName), easy_marshal("value1_"+dbName))
		db.Put([]byte("key2"+ dbName), easy_marshal("value2_"+dbName))
		db.Put([]byte("key3"+ dbName), easy_marshal("value3_"+dbName))
	}

	for _, dbName := range dbNames {
		db := p.GetDBHandle(dbName)
		val, err := db.Get([]byte("key1"+ dbName))
		testutil.AssertNoError(t, err, "")
		testutil.AssertEquals(t, easy_unmarshal(val), "value1_"+dbName)

		val, err = db.Get([]byte("key2"+ dbName))
		testutil.AssertNoError(t, err, "")
		testutil.AssertEquals(t, easy_unmarshal(val), "value2_"+dbName)

		val, err = db.Get([]byte("key3"+ dbName))
		testutil.AssertNoError(t, err, "")
		testutil.AssertEquals(t, easy_unmarshal(val), "value3_"+dbName)
	}

	for _, dbName := range dbNames {
		db := p.GetDBHandle(dbName)
		testutil.AssertNoError(t, db.Delete([]byte("key1"+ dbName)), "")
		val, err := db.Get([]byte("key1"))
		testutil.AssertNoError(t, err, "")
		testutil.AssertEquals(t, easy_unmarshal(val), "")

		testutil.AssertNoError(t, db.Delete([]byte("key2"+ dbName)), "")
		val, err = db.Get([]byte("key2"))
		testutil.AssertNoError(t, err, "")
		testutil.AssertEquals(t, easy_unmarshal(val), "")

		testutil.AssertNoError(t, db.Delete([]byte("key3"+ dbName)), "")
		val, err = db.Get([]byte("key3"))
		testutil.AssertNoError(t, err, "")
		testutil.AssertEquals(t, easy_unmarshal(val), "")
	}
}


