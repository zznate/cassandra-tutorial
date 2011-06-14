package com.datastax.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Get a single column, 'City', for a specific NPA/NXX combination. 
 * Uses @{@link HFactory} to construct a simple {@link ColumnQuery}
 * 
 * Thrift API: http://wiki.apache.org/cassandra/API#get
 * API Column description: http://wiki.apache.org/cassandra/API#Column
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="get" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 * 
 * NOTE: in the raw Thrift API, the get method throws a NotFoundException
 * on a miss. Hector encapsulates this, instead returning NULL as 'get'
 * is the *only* API method that will throw an exception in non-error conditions. 
 * 
 */
public class GetCityForNpanxx extends TutorialCommand {

    
    public GetCityForNpanxx(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<HColumn<String,String>> execute() {        
        ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspace);
        columnQuery.setColumnFamily("Npanxx");
        columnQuery.setKey("512204");
        columnQuery.setName("city");
        QueryResult<HColumn<String, String>> result = columnQuery.execute();
        
        return result;
    }

    
}
