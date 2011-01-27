package com.datastax.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

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
        // generates impossible slice and hangs reader thread!!!
        sliceQuery.setRange("Austin__", "Austin__999", false, 5);
        //sliceQuery.setRange("Austin__", "Austin__999", false, 5);
        //sliceQuery.setRange(1, 999, false, 5);

        QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();

        return result;
    }

}
