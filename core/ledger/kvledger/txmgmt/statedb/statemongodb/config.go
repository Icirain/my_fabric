package statemongodb

import ("test_demo_go/mongodbhelper"
	"github.com/spf13/viper"
	"gopkg.in/mgo.v2"
	"fmt"
)

func GetMongoDBConf() *mongodbhelper.Conf{
	conf := mongodbhelper.Conf{}
	url := viper.GetString("ledger.state.MongoDBConfig.url")
	username := viper.GetString("ledger.state.MongoDBConfig.username")
	password := viper.GetString("ledger.state.MongoDBConfig.password")
	collectionname := viper.GetString("ledger.state.mongoDBConfig.collection_name")
	databasename := viper.GetString("ledger.state.mongoDBConfig.database_name")
	fmt.Println(collectionname)
	fmt.Println("test : ",viper.GetString("ledger.state.couchDBConfig.couchDBAddress"))
	dialinfo, err := mgo.ParseURL(url)
	if err != nil{
		panic(err)
	}
	dialinfo.Username = username
	dialinfo.Password = password
	conf.Dialinfo = dialinfo
	conf.Database_name = databasename
	conf.Collection_name = collectionname
	return &conf

}