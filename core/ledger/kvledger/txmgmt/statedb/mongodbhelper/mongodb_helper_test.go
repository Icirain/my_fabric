package mongodbhelper

import (
	"testing"
	//"labix.org/v2/mgo"
	//"labix.org/v2/mgo/bson"
	"labix.org/v2/mgo/bson"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	//"fmt"
//	"fmt"
	"strconv"
)

func TestMongoDBHelperWriteWithoutOpen(t *testing.T) {
	env := newTestDBEnv(t, testDBPath)
	defer env.cleanup()
	db := env.db
	defer func() {
		if recover() == nil {
			t.Fatalf("A panic is expected when writing to db before opening")
		}
	}()
	db.Put("key", easy_marshal("value"))
}

func TestMongoDBHelperReadWithoutOpen(t *testing.T) {
	env := newTestDBEnv(t, testDBPath)
	defer env.cleanup()
	db := env.db
	defer func() {
		if recover() == nil {
			t.Fatalf("A panic is expected when writing to db before opening")
		}
	}()
	db.Get("key")
}

func TestLevelDBHelper(t *testing.T) {
	env := newTestDBEnv(t, testDBPath)
	defer env.cleanup()
	db := env.db
//        var result Kv_pair
	db.Open()
	// second time open should not have any side effect
	db.Open()
	db.Put("key1", easy_marshal("value1"))
	db.Put("key2", easy_marshal("value2"))
	db.Put("key3", easy_marshal("value3"))

	val, _ := db.Get("key2")
	testutil.AssertEquals(t, easy_unmarshal(val), "value2")

	db.Delete("key1")
	db.Delete("key2")
	db.Delete("key3")

	val1, err1 := db.Get("key1")
	testutil.AssertNoError(t, err1, "")
	testutil.AssertEquals(t, easy_unmarshal(val1), "")

	val2, err2 := db.Get("key2")
	testutil.AssertNoError(t, err2, "")
	testutil.AssertEquals(t, easy_unmarshal(val2), "")

	db.Close()
	// second time open should not have any side effect
	db.Close()


	db.Open()

	db.Put("key1", easy_marshal("value1"))
	db.Put("key2", easy_marshal("value2"))
	db.Put("key3", easy_marshal("value3"))


	val1, err1 = db.Get("key1")
	testutil.AssertNoError(t, err1, "")
	testutil.AssertEquals(t, easy_unmarshal(val1), "value1")

	val2, err2 = db.Get("key2")
	testutil.AssertNoError(t, err2, "")
	testutil.AssertEquals(t, easy_unmarshal(val2), "value2")

	val3, err3 := db.Get("key3")
	testutil.AssertNoError(t, err3, "")
	testutil.AssertEquals(t, easy_unmarshal(val3), "value3")

	//keys := []interface{}{}
	/*itr := db.GetIterator("a", "z", 0, 0)
	for itr.Next(&result) {
		fmt.Println(result.Value.Value_content)
		fmt.Println(result.Key)
	}*/
	//testutil.AssertEquals(t, keys, []string{"key1", "key2"})
}






func easy_marshal(v interface{}) []byte {
	value := &Value{Value_content:v, Field_map:map[string]string{}, Attachments:nil}
	value.Field_map["test"] = strconv.Itoa(123)
	out, err := bson.Marshal(value)
	if err != nil{
		panic(err)
		return nil
	}
	return out
}

func easy_unmarshal(out []byte) string{
	if out == nil{
		return ""
	}
	var return_value Value
	err := bson.Unmarshal(out, &return_value)
	if err != nil{
		panic(err)
	}
	//fmt.Println(return_value.Field_map["test"])

		return return_value.Value_content.(string)

}
