var by = 'product',
    coll = 'spark.offers_by_'+by, 
    appDb = db.getSiblingDB("appdata"),
    appColl = 'app.products', 
    index = 0;

var cursor = db[coll].find();

while(cursor.hasNext()){
	var rec = cursor.next();
	if (rec[by]) {
		var prod = appDb[appColl].findOne({_id:ObjectId(rec[by])}, {displayName:1})
		prod._id = {$oid: prod._id.str}
		printjsononeline(prod);
	}
}