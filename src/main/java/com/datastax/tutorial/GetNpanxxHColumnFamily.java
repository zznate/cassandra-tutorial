package com.datastax.tutorial;

import me.prettyprint.cassandra.service.HColumnFamilyImpl;
import me.prettyprint.hector.api.HColumnFamily;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Uses the HColumnFamily simple API
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="get_hcol" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 * 
 */
public class GetNpanxxHColumnFamily extends TutorialCommand {

    public GetNpanxxHColumnFamily(Keyspace keyspace) {
        super(keyspace);

    }

    @Override
    public ResultStatus execute() {
        HColumnFamily<String, String> columnFamily = 
            new HColumnFamilyImpl<String, String>(keyspace, "Npanxx", stringSerializer, stringSerializer);
        columnFamily.addKey("512202");
        columnFamily.addColumnName("city")
        .addColumnName("state")
        .addColumnName("lat")
        .addColumnName("lng");
        
        // execution is fired as soon as the first accessor is called
        
        log.info("Results from HColumnFamily: city: {} state:{} lat:{} lng:{}",
                new Object[]{columnFamily.getString("city"),
                columnFamily.getString("state"),
                columnFamily.getString("lat"),
                columnFamily.getString("lng")});
        return columnFamily;
    }

}
