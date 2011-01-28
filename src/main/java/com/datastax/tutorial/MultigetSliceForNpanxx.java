package com.datastax.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Like {@link GetSliceForNpanxx}, but uses the multiget_slice to 
 * apply the slice operation to mulitple keys
 * 
 * Thrift API: http://wiki.apache.org/cassandra/API#multiget_slice
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="multiget_slice" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 * 
 */
public class MultigetSliceForNpanxx extends TutorialCommand {

    public MultigetSliceForNpanxx(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<Rows<String,String,String>> execute() {
        MultigetSliceQuery<String, String, String> multigetSlicesQuery =
            HFactory.createMultigetSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        multigetSlicesQuery.setColumnFamily("Npanxx");
        multigetSlicesQuery.setColumnNames("city","state","lat","lng");        
        multigetSlicesQuery.setKeys("512202","512203","512204","512205");
        QueryResult<Rows<String, String, String>> results = multigetSlicesQuery.execute();
        return results;
    }

}
