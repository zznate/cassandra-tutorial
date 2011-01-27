package com.datastax.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

public class GetSliceForStateCity extends TutorialCommand {

    public GetSliceForStateCity(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<ColumnSlice<Integer,String>> execute() {
        SliceQuery<String, Integer, String> sliceQuery = 
            HFactory.createSliceQuery(keyspace, stringSerializer, integerSerializer, stringSerializer);
        sliceQuery.setColumnFamily("StateCity");
        sliceQuery.setKey("TX Austin");

        sliceQuery.setRange(1, 999, false, 5);

        QueryResult<ColumnSlice<Integer, String>> result = sliceQuery.execute();

        return result;
    }

}
