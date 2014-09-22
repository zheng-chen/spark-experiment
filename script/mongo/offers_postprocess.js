var threshold = 10,
    by = 'customer',
    coll = 'spark.offers_by_'+by, 
    appDb = db.getSiblingDB("appdata"),
    appColl = 'core.organizations', 
    index = 0;

var cursor = db[coll].find();

while(cursor.hasNext()){
	index++;
	var rec = cursor.next()
	
	// feature scaling
	var scale = 0;
	if (rec.results) {
		var wins = rec.results.win || 0;
		var total = wins + (rec.results.lose||0) + (rec.results.houseAccount||0);
//		print(wins+":"+total);
		if (total == 0) {
	      scale = 0
	    } else if (wins == 0) {  
	      if (total>=threshold) { // threshold to claim it's completely negative
	    	  scale = -1;
	      } else {
	    	  scale = - (total/threshold);
	      }
	    } else if (wins != total) {
	    	scale = wins / total
	    } else { // All wins
	    	if (wins>=threshold)
	    		scale = 1;
	    	else 
	    		scale = wins/threshold
	    }
	}
	var displayName = '';
	if (rec[by]) {
		var appCur = appDb[appColl].findOne({_id:ObjectId(rec[by])}, {displayName:1});
		if (appCur) {
			displayName = appCur.displayName
		}
	}
	
	db[coll].update({_id:rec._id}, {$set: {scale: scale, displayName: displayName}});
	
	if (index%100==0) print('Processed '+index);
}