package com.datastax.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Get all the columns for a single Npanxx row.
 * Shows the construction of a {@link SliceQuery} with the 
 * {@link StringSerializer} defined in the parent class.
 * 
 * Thrift API: http://wiki.apache.org/cassandra/API#get_slice
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="get_slice" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 * 
 */
public class GetSliceForNpanxx extends TutorialCommand {

    public GetSliceForNpanxx(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<ColumnSlice<String,String>> execute() {
        SliceQuery<String, String, String> sliceQuery = 
            HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        sliceQuery.setColumnFamily("Npanxx");
        sliceQuery.setKey("512202");
        // We only ever have these four columns on Npanxx
        sliceQuery.setColumnNames("city","state","lat","lng");
        // The following would do the exact same as the above
        // accept here we say get the first 4 columns according to comparator order
        // sliceQuery.setRange("", "", false, 4);
        
        QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
        return result;
    }
}
