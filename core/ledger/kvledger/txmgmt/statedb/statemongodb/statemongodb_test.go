package statemongodb

import (
	"testing"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/commontests"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
)

func TestBasicRW(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup()
	commontests.TestBasicRW(t, env.DBProvider)
}

func TestMultiDBBasicRW(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup()
	commontests.TestMultiDBBasicRW(t, env.DBProvider)
}

func TestDeletes(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup()
	commontests.TestDeletes(t, env.DBProvider)
}

func TestCompositeKey(t *testing.T) {
	testCompositeKey(t, "ledger1", "ns", "key")
	testCompositeKey(t, "ledger2", "ns", "")
}

func testCompositeKey(t *testing.T, dbName string, ns string, key string) {
	compositeKey := constructCompositeKey(ns, key)
	t.Logf("compositeKey=%#v", compositeKey)
	ns1, key1 := splitCompositeKey(compositeKey)
	testutil.AssertEquals(t, ns1, ns)
	testutil.AssertEquals(t, key1, key)
}

func TestEncodeDecodeValueAndVersion(t *testing.T) {
	testValueAndVersionEncodeing(t, []byte("value1"), version.NewHeight(1, 2))
	testValueAndVersionEncodeing(t, []byte{}, version.NewHeight(50, 50))
}

func testValueAndVersionEncodeing(t *testing.T, value []byte, version *version.Height) {
	encodedValue := statedb.EncodeValue(value, version)
	val, ver := statedb.DecodeValue(encodedValue)
	testutil.AssertEquals(t, val, value)
	testutil.AssertEquals(t, ver, version)
}

func TestIterator(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup()
	commontests.TestIterator(t, env.DBProvider)
}
