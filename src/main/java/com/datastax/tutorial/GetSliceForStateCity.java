package com.datastax.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * A get_slice call that shows demonstrates how to take advantage of the
 * ordering provided by a column families comparator. 
 * 
 * Thrift API: http://wiki.apache.org/cassandra/API#get_slice
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="get_slice_sc" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 * 
 */
public class GetSliceForStateCity extends TutorialCommand {

    public GetSliceForStateCity(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<ColumnSlice<Long,String>> execute() {
        SliceQuery<String, Long, String> sliceQuery = 
            HFactory.createSliceQuery(keyspace, stringSerializer, longSerializer, stringSerializer);
        sliceQuery.setColumnFamily("StateCity");
        sliceQuery.setKey("TX Austin");
        sliceQuery.setRange(202L, 204L, false, 5);
        QueryResult<ColumnSlice<Long, String>> result = sliceQuery.execute();

        return result;
    }

}
