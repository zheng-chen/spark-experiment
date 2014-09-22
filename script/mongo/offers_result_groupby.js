var res = "win", index = 0, by='customer',
	cubedb = db.getSiblingDB("cubedata"),
	collName = 'spark.offers_by_'+by;

db.app.offers.aggregate( [
  { $match : {'result.name' : res} }, 
  { $group : { _id : "$relationships."+by+".targets.key",count : { $sum : 1 } } }
  ], 
  {cursor: { batchSize: 50 }}).forEach(function (entry) {
  if (!entry || !entry._id || !entry._id[0])
    return;
  
  var prodId =  entry._id[0];
  var sparkData = cubedb[collName].findOne({product: prodId});
  var obj = {};
  obj[res] = entry.count;
  if (sparkData && !hasResult(sparkData.results, res)) {
    cubedb[collName].update({_id:sparkData._id}, {$push: {results: obj}})
  } else {
    cubedb[collName].insert({
      product: prodId,
      results: [ obj ]
    })
  }
  
  index++;
  if (index%100==0) print('Processed '+index);
});

function hasResult(results, searchFor) {
  var ret = false;
  if (results && results.length) {
    for (var i=0 ; i<results.length; i++) {
      if (results[i] && results[i][searchFor]) {
        ret = true
      }
    }
  }
  return ret;
}