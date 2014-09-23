package com.servicesource.spark.mongo;

import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.splitter.MongoCollectionSplitter;
import com.mongodb.hadoop.splitter.SplitFailedException;

import org.bson.types.ObjectId;

public class CustomCollectionSplitter extends MongoCollectionSplitter {

	private static final Log LOG = LogFactory
			.getLog(CustomCollectionSplitter.class);
	private static final String _ID = "_id";

	private DBObject _IDOBJ = BasicDBObjectBuilder.start().add(_ID, 1).get();

	// TODO: make it configurable
	private int chunk_size = 3000;

	public CustomCollectionSplitter() {
	}

	public CustomCollectionSplitter(final Configuration conf) {
		super(conf);
	}

	@Override
	public List<InputSplit> calculateSplits() throws SplitFailedException {
		init();
		// MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);

		DBObject query = MongoConfigUtil.getQuery(conf);
		DBCursor cur = getCursor(query, null);

		ObjectId prevId = null;
		List<InputSplit> splits = new ArrayList<InputSplit>();
		while (cur != null && cur.hasNext()) {
			final BasicDBObject row = (BasicDBObject) cur.next();
			ObjectId rowId = (ObjectId) row.get(_ID);
			addSplit(prevId, rowId, query, splits);
			cur.close();
			prevId = rowId;
			cur = getCursor(query, prevId);
		}
		if (prevId!=null) {
			addSplit(prevId, null, query, splits);
		}
		cur.close();

		return splits;
	}

	private void addSplit(ObjectId from, ObjectId to, DBObject query,
			List<InputSplit> splits) throws SplitFailedException {
		MongoInputSplit split = createSplit(from, to, query);
		LOG.info("Generated split " + splits.size() + ":" + split);
		splits.add(split);
	}

	private DBCursor getCursor(DBObject query, ObjectId previousId) {
		DBObject newQuery = new BasicDBObject();
		newQuery.putAll(query);
		if (previousId != null) {
			BasicDBObject rangeObj = new BasicDBObject();
			rangeObj.put("$gte", previousId);
			newQuery.put(_ID, rangeObj);
		}
		
		//TODO: customize hint ...
//		DBObject hint = BasicDBObjectBuilder.start().add("result.name", 1).add(_ID, 1).get();
		
		return inputCollection.find(newQuery, _IDOBJ).sort(_IDOBJ) //.hint(hint)
				.skip(chunk_size).limit(1);
	}

	private MongoInputSplit createSplit(ObjectId from, ObjectId to,
			DBObject query) throws SplitFailedException {
		if (from == null && to == null) {
			DBObject splitQuery = new BasicDBObject();
			splitQuery.putAll(query);
			MongoInputSplit split = new MongoInputSplit();
			split.setQuery(splitQuery);
			return split;
		}

		BasicDBObject rangeObj = new BasicDBObject();
		if (from != null) {
			rangeObj.put("$gte", from);
		}
		if (to != null) {
			rangeObj.put("$lt", to);
		}
		DBObject splitQuery = new BasicDBObject();
		splitQuery.putAll(query);
		splitQuery.put(_ID, rangeObj);
		MongoInputSplit split = new MongoInputSplit();
		split.setQuery(splitQuery);
		split.setInputURI(MongoConfigUtil.getInputURI(conf));
		split.setNoTimeout(true);
		return split;
	}

}
