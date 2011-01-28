package com.datastax.tutorial;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Uses get_indexed_slices to stack index clauses on the indexed field 'state'
 * as well as the non-indexed fields. This can be done if and only if 
 * we include 'state' in the index clause list as an equals expression. For
 * more detail, see the API page below. 
 * 
 * NOTE: you must first run the npanxx_index.txt script in order to execute
 * this query. 
 * 
 * Thrift API: http://wiki.apache.org/cassandra/API#get_indexed_slices
 * 
 * Additional information: http://www.datastax.com/blog/whats-new-cassandra-07-secondary-indexes
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="get_indexed_slices" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 * 
 */
public class GetIndexedSlicesForCityState extends TutorialCommand {

    public GetIndexedSlicesForCityState(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<OrderedRows<String, String, String>> execute() {
        IndexedSlicesQuery<String, String, String> indexedSlicesQuery = 
            HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        indexedSlicesQuery.setColumnFamily("Npanxx");
        indexedSlicesQuery.setColumnNames("city","state","lat","lng");
        indexedSlicesQuery.addEqualsExpression("state", "TX");
        indexedSlicesQuery.addEqualsExpression("city", "Austin");
        indexedSlicesQuery.addGteExpression("lat", "30.30");
        QueryResult<OrderedRows<String, String, String>> result = indexedSlicesQuery.execute();
        
        return result;
    }

}
