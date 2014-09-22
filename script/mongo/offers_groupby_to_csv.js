var by = 'customer',
    coll = 'spark.offers_by_'+by;

var cursor = db[coll].find();

while(cursor.hasNext()){
	var rec = cursor.next()
	if (rec[by]) {
		print(rec[by]+','+rec.scale)
	}
}