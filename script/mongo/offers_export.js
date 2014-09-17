var cursor = db.app.offers.find({'result.name': {$in: ['win', 'lose', 'houseAccount']}}, 
	{'result.name':1, 'amount.normalizedAmount.amount':1, 'relationships.product.targets.key':1,
	'relationships.customer.targets.key':1}).hint('result.name_1__id_1');

cursor.addOption(DBQuery.Option.noTimeout)

while(cursor.hasNext()){
	var rec = cursor.next()
	rec._id = {$oid: rec._id.str}
    printjsononeline(rec);
}