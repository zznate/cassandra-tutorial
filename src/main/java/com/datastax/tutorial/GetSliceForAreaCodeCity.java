package com.datastax.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * A get_slice query showing off 'limit' of columns for a given key.
 * 
 * @author zznate
 *
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
        sliceQuery.setRange("", "", false, 2);

        QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();

        return result;
    }

}
