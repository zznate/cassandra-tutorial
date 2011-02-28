package com.datastax.tutorial;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Like {@link GetIndexedSlicesForCityState} except that we show an example
 * of using bytes as the value in order to deal with multiple value types.
 * 
 * NOTE: you must first run the npanxx_index.txt script in order to execute
 * this query. 
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="get_indexed_slices_raw" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 * 
 */
public class GetIndexedSlicesHandleRawBytes extends TutorialCommand {

    private static final BytesArraySerializer bas = BytesArraySerializer.get();
    
    public GetIndexedSlicesHandleRawBytes(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<OrderedRows<String, String, byte[]>> execute() {
        IndexedSlicesQuery<String, String, byte[]> indexedSlicesQuery = 
            HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, bas);
        indexedSlicesQuery.setColumnFamily("Npanxx");
        indexedSlicesQuery.setColumnNames("city","state","lat","lng");
        indexedSlicesQuery.addEqualsExpression("state", stringSerializer.toBytes("TX"));
        indexedSlicesQuery.addEqualsExpression("city", stringSerializer.toBytes("Austin"));
        indexedSlicesQuery.addGteExpression("lat", stringSerializer.toBytes("30.30"));
        QueryResult<OrderedRows<String, String, byte[]>> result = indexedSlicesQuery.execute();
        
        String city = stringSerializer.fromBytes(result.get().iterator().next().getColumnSlice().getColumnByName("city").getValue());
        log.info("Decoded City: {}, not decoded: {}", city, result.get().iterator().next().getColumnSlice().getColumnByName("city").getValue());
        return result;
    }

}
