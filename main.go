package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	connString   string
	dbName       string
	timeInterval int
	col          []string
	tgtKey       []string
	lastRotation int
)

func dupCheck(ctx context.Context, db *mongo.Database) {
	// Fetch all collections as per col
	for i := range col {
		fmt.Println("[INFO] Validating collection", col[i])
		collection := db.Collection(col[i])

		cur, err := collection.Aggregate(ctx, []bson.M{
			{"$group": bson.M{"_id": "$" + tgtKey[i], "count": bson.M{"$sum": 1}}},
			{"$match": bson.M{"_id": bson.M{"$ne": nil}, "count": bson.M{"$gt": 1}}},
			{"$project": bson.M{tgtKey[i]: "$_id", "_id": 0}},
		})

		if err != nil {
			fmt.Println("[ERROR] Error during mongodb col.Find;", err)
			return
		}

		defer cur.Close(ctx)
		for cur.Next(ctx) {
			var doc map[string]any
			err := cur.Decode(&doc)
			if err != nil {
				fmt.Println("[ERROR] Error during mongodb cur.Decode;", err)
				continue
			}

			// Find the document
			id := doc[tgtKey[i]]

			if id == nil {
				fmt.Println("[ERROR] doc[tgtKey] is nil. Skipping")
				continue
			}

			docs, err := collection.Find(ctx, bson.M{tgtKey[i]: id})

			if err != nil {
				fmt.Println("[ERROR] Error during mongodb col.Find;", err)
				continue
			}

			// Find the protected element
			var findLimit int64 = 1

			findOptions := options.Find()
			findOptions.Limit = &findLimit
			findOptions.SetSort(bson.M{"_id": -1})

			protected, err := collection.Find(ctx, bson.M{
				tgtKey[i]: id,
			}, findOptions)

			defer protected.Close(ctx)

			var lastUpdated primitive.ObjectID

			for protected.Next(ctx) {
				var doc map[string]any
				err := protected.Decode(&doc)
				if err != nil {
					fmt.Println("[ERROR] Error during mongodb protected.Decode;", err)
					continue
				}

				lastUpdated = doc["_id"].(primitive.ObjectID)
				fmt.Println("[DEBUG] lastUpdated:", lastUpdated)
			}

			defer docs.Close(ctx)

			// Iterate over the documents
			//var ts []time.Time

			var dupCounter int

			for docs.Next(ctx) {
				var docDat map[string]any

				err := docs.Decode(&docDat)

				if err != nil {
					fmt.Println("[ERROR] Error during mongodb docs.Decode;", err)
					continue
				}

				objId, ok := docDat["_id"].(primitive.ObjectID)

				if !ok {
					fmt.Println("[ERROR] docDat[\"_id\"] is not an ObjectID. Skipping")
					continue
				}

				if objId == lastUpdated {
					fmt.Println("Got last updated")
				} else {
					fmt.Println(objId, "!=", lastUpdated, "| Deleting")
					delRes, err := collection.DeleteOne(ctx, bson.M{"_id": objId})
					if err != nil {
						fmt.Println("[ERROR] Error during mongodb collection.DeleteOne;", err)
						continue
					}
					fmt.Println("Mongo has removed", delRes.DeletedCount, "documents")
					dupCounter++
				}
			}
			fmt.Println("Removed", dupCounter, "duplicates from", col[i], "with id", id)
		}
	}
}

func handleMaintSignals() {
	ch := make(chan os.Signal, 1)
	go func() {
		for sig := range ch {
			switch sig {
			case syscall.SIGUSR1:
				tsl := time.Duration(int(time.Now().Unix())-lastRotation) * time.Second
				nextRotation := time.Duration(timeInterval)*time.Minute - tsl
				fmt.Println("[DEBUG] lastRotation:", lastRotation, "| Time since last rotation:", tsl, "| Estimated time till next rotation:", nextRotation)
			}
		}
	}()
	signal.Notify(ch, syscall.SIGUSR1, syscall.SIGUSR2)
}

func main() {
	handleMaintSignals()

	fmt.Println("DBTool: init")
	ctx := context.Background()

	var (
		colStr    string
		tgtKeyStr string
	)

	flag.StringVar(&connString, "conn", "mongodb://127.0.0.1:27017/infinity", "[This is required] MongoDB connection string")
	flag.StringVar(&dbName, "dbname", "infinity", "[This is required] DB name to connect to.")
	flag.IntVar(&timeInterval, "interval", 10, "[This is required if using act as watch] Interval for watcher to wait for (minutes)")
	flag.StringVar(&colStr, "col", "bots", "[This is required] What collections to check duplicates by. Comma separated for multiple.")
	flag.StringVar(&tgtKeyStr, "key", "botID", "The key on the document to filter analysis on. Comma separated for multiple.")

	flag.Parse()

	progName := os.Args[0]

	if tgtKeyStr == "" || colStr == "" {
		fmt.Println("No --key and/or --col found. Try running:", progName, "--help")
		os.Exit(-1)
	}

	col = strings.Split(strings.ReplaceAll(colStr, " ", ""), ",")
	tgtKey = strings.Split(strings.ReplaceAll(tgtKeyStr, " ", ""), ",")

	if len(col) != len(tgtKey) {
		fmt.Println("Number of --col and --key must be the same. Try running:", progName, "--help")
		os.Exit(-1)
	}

	fmt.Println("DBTool: Connecting to", connString)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))

	if err != nil {
		panic(err)
	}

	fmt.Println("Connected to mongoDB?")

	db := client.Database(dbName)

	colNames, err := db.ListCollectionNames(ctx, bson.D{})

	if err != nil {
		panic(err)
	}

	fmt.Println("Collections in DB: ", colNames)

	fmt.Println("DBTool: Connected to mongo successfully")

	func() {
		d := time.Duration(timeInterval) * time.Minute
		dupCheck(ctx, db)
		fmt.Println("Waiting for next rotation")
		lastRotation = int(time.Now().Unix())
		for x := range time.Tick(d) {
			fmt.Println("Autodup checker started at", x)

			dupCheck(ctx, db)
			fmt.Println("Waiting for next rotation")
			lastRotation = int(time.Now().Unix())
		}
	}()
}
