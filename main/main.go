package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/yottachain/yotta-analysis/cmd"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	ytanalysis "github.com/yottachain/yotta-analysis"
)

func main() {
	cmd.Execute()
}

func main0() {
	_, err := ytanalysis.FetchMinersInfo(context.Background(), &http.Client{}, "http://127.0.0.1:8080", 0, 0, 2)
	if err != nil {
		panic(err)
	}
}

func main1() {
	config := elasticsearch.Config{
		Addresses: []string{"https://elk1-nm.yottachain.net"},
		Username:  "elastic",
		Password:  "yotta_2021",
	}
	esClient, err := elasticsearch.NewClient(config)
	if err != nil {
		panic(err)
	}
	err = ytanalysis.AddResultToES(context.Background(), esClient, &ytanalysis.SPLog{MinerID: 1, Result: []*ytanalysis.SPResult{{Hash: "AAA", Pass: true}, {Hash: "BBB", Pass: false}}, Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		panic(err)
	} else {
		fmt.Println("success")
	}
}

func main2() {
	url := "mongodb://127.0.0.1:27017/?connect=direct"
	snid := 0
	isTest := false
	if len(os.Args) == 1 {
		panic("no SN ID")
	} else if len(os.Args) == 2 {
		var err error
		snid, err = strconv.Atoi(os.Args[1])
		if err != nil {
			panic(err)
		}
	} else if len(os.Args) == 3 {
		var err error
		url = os.Args[2]
		snid, err = strconv.Atoi(os.Args[1])
		if err != nil {
			panic(err)
		}
	} else if len(os.Args) == 4 {
		var err error
		url = os.Args[2]
		snid, err = strconv.Atoi(os.Args[1])
		if err != nil {
			panic(err)
		}
		isTest = true
	}
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(url))
	if err != nil {
		panic(err)
	}

	BASE1 := "metabase"
	BASE2 := "yotta"
	if isTest {
		BASE1 = fmt.Sprintf("metabase_%d", snid)
		BASE2 = fmt.Sprintf("yotta%d", snid)
	}
	collection := client.Database(BASE1).Collection("shards")
	collection2 := client.Database(BASE2).Collection("Node")
	limit, err := getLimit(client, BASE1, "boundary")
	if err != nil {
		panic(err)
	}
	field := fmt.Sprintf("uspaces.sn%d", snid)
	counter := 0
	m := make(map[int32]bool)
	cur, err := collection.Find(context.Background(), bson.M{"_id": bson.M{"$lt": limit}})
	if err != nil {
		panic(err)
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		shard := new(Shard)
		err := cur.Decode(shard)
		if err != nil {
			panic(err)
		}
		result, err := collection2.UpdateOne(context.Background(), bson.M{"_id": shard.NodeID}, bson.M{"$inc": bson.M{field: int64(1)}})
		if err != nil {
			panic(err)
		}
		if result.ModifiedCount == 0 {
			m[shard.NodeID] = true
		} else {
			counter++
			if counter%10000 == 0 {
				log.Printf("finished %d shards\n", counter)
			}
		}
	}
	log.Printf("\nminers failed to update:")
	for k := range m {
		fmt.Println(k)
	}
}

func getLimit(client *mongo.Client, dbname, colname string) (int64, error) {
	collection := client.Database(dbname).Collection(colname)
	boundary := &struct {
		ID primitive.ObjectID `bson:"_id"`
	}{}
	err := collection.FindOne(context.Background(), bson.M{}).Decode(boundary)
	if err != nil {
		return 0, err
	}
	bdyTime := boundary.ID.Timestamp().Unix()
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int32(bdyTime))
	binary.Write(buf, binary.BigEndian, int32(0))
	return int64(binary.BigEndian.Uint64(buf.Bytes())), nil
}

type Shard struct {
	ID     int64 `bson:"_id"`
	NodeID int32 `bson:"nodeId"`
}
