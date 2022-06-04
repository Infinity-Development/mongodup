# mongodup
Simple CLI for handling duplicate collections in mongoose

---

## Exampe Output
```
root@infinity:~/mongo-dup-remover# ./mongodup --col users,bots --key userID,botIDDBTool: init
DBTool: Connecting to mongodb://127.0.0.1:27017/infinity
Connected to mongoDB?
Collections in DB:  [packages staff_apps dev_apps users reviews transcripts sessions tickets oauths suggests bots votes]
DBTool: Connected to mongo successfully
[INFO] Validating collection users
[INFO] Validating collection bots
[DEBUG] lastUpdated: ObjectID("629b28f393456bb2bc0228f8")
ObjectID("629a6a677b9452be2b286383") != ObjectID("629b28f393456bb2bc0228f8") | Deleting
Mongo has removed 1 documents
Got last updated
Removed 1 duplicates from bots with id 721279531939397673
Waiting for next rotation
```
