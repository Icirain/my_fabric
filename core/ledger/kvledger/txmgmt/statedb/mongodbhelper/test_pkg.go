package mongodbhelper

import (
	"testing"
	"os"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"gopkg.in/mgo.v2"
)



const testDBPath = ""

type testDBEnv struct {
	t    *testing.T
	path string
	db   *DB
}

type testDBProviderEnv struct {
	t        *testing.T
	path     string
	provider *Provider
}

func newTestDBEnv(t *testing.T, path string) *testDBEnv {
	testDBEnv := &testDBEnv{t: t, path: path}
	testDBEnv.cleanup()
	dialinfo, err := mgo.ParseURL(path)
	if err != nil{
		panic(err)
	}
	testDBEnv.db = CreateDB(&Conf{Dialinfo:dialinfo, Collection_name:"mongotest", Database_name: "test1"})
	return testDBEnv
}

func newTestProviderEnv(t *testing.T, path string) *testDBProviderEnv {
	testProviderEnv := &testDBProviderEnv{t: t, path: path}
	testProviderEnv.cleanup()
	dialinfo, err := mgo.ParseURL(path)
	if err != nil{
		panic(err)
	}
	testProviderEnv.provider = NewProvider(&Conf{Dialinfo:dialinfo, Collection_name:"mongotest", Database_name: "test1"})
	return testProviderEnv
}

func (dbEnv *testDBEnv) cleanup() {

	if dbEnv.db != nil {
		dbEnv.db.Close()
	}
	testutil.AssertNoError(dbEnv.t, os.RemoveAll(dbEnv.path), "")
}

func (providerEnv *testDBProviderEnv) cleanup() {
	if providerEnv.provider != nil {
		providerEnv.provider.Close()
	}
	testutil.AssertNoError(providerEnv.t, os.RemoveAll(providerEnv.path), "")
}

