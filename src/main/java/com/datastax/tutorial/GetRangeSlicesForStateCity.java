package com.datastax.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Get all the columns for a "range" of Npanxx rows. Range in this sense
 * is only meaningful if you are using OrderPreservingPartitioner. With
 * RandomPartitioner, the default, you are getting the token range (converted
 * from hashed keys).
 * For more information, see: http://wiki.apache.org/cassandra/FAQ#range_rp
 * 
 * Thrift API: http://wiki.apache.org/cassandra/API#get_range_slices
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="get_range_slices" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 * 
 */
public class GetRangeSlicesForStateCity extends TutorialCommand {

    public GetRangeSlicesForStateCity(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<OrderedRows<String,String,String>> execute() {
        RangeSlicesQuery<String, String, String> rangeSlicesQuery =
            HFactory.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        rangeSlicesQuery.setColumnFamily("Npanxx");
        rangeSlicesQuery.setColumnNames("city","state","lat","lng");        
        rangeSlicesQuery.setKeys("512202", "512205");
        rangeSlicesQuery.setRowCount(5);
        QueryResult<OrderedRows<String, String, String>> results = rangeSlicesQuery.execute();
        return results;
    }

}
