package com.datastax.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * A get_slice query shows how to use the comparator for ranges of text queries
 * 
 * mvn -e exec:java -Dexec.args="get_slice_acc" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 * 
 * @author zznate
 */
public class GetSliceForAreaCodeCity extends TutorialCommand {

    public GetSliceForAreaCodeCity(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<ColumnSlice<String,String>> execute() {
        SliceQuery<String, String, String> sliceQuery = 
            HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        sliceQuery.setColumnFamily("AreaCode");
        sliceQuery.setKey("512");
        // change the order argument to 'true' to get the last 2 columns in descending order
        // gets the first 4 columns "between" Austin and Austin__204 according to comparator
        sliceQuery.setRange("Austin", "Austin__204", false, 5);

        QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();

        return result;
    }

}
