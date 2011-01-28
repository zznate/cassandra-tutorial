package com.datastax.tutorial;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for running the tutorials in this project.
 * This class simply looks at the provided args and attempts to find 
 * the matching sample class to execute. 
 * 
 * {@link QueryResult}s are logged to stdout (configured in log4j.properties). 
 * 
 * @author zznate
 */
public class TutorialRunner {
    private static Logger log = LoggerFactory.getLogger(TutorialRunner.class); 

    static Cluster tutorialCluster;
    static Keyspace tutorialKeyspace;
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        tutorialCluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        tutorialKeyspace = HFactory.createKeyspace("Tutorial", tutorialCluster);
        
        TutorialCommand command = loadCommand(args[0]);
        try {
        QueryResult<?> result = command.execute();

        log.info("Result executed in: {} microseconds against host: {}",
                result.getExecutionTimeMicro(), result.getHostUsed().getName());
        
        if ( result instanceof Rows ) {            
            Rows<?,?,?> rows = (Rows)result.get();
            for (Row row : rows) {
                log.info("Retrieved Row: {}", row);
            }
        } else {
            log.info("Details of result:\n{}", result.get());
        }
        
        
        } catch (Exception e) {
            log.error("Problem executing command:",e);
        }
        tutorialCluster.getConnectionManager().shutdown();
    }
    
    
    private static TutorialCommand loadCommand(String cmd) {
        if ( cmd.equalsIgnoreCase("get")) {
            return new GetCityForNpanxx(tutorialKeyspace);
        } else if ( cmd.equalsIgnoreCase("get_slice")) {
            return new GetSliceForNpanxx(tutorialKeyspace);
        } else if ( cmd.equalsIgnoreCase("get_range_slices")) {
            return new GetRangeSlicesForStateCity(tutorialKeyspace);
        } else if ( cmd.equalsIgnoreCase("get_slice_acc")) {
            return new GetSliceForAreaCodeCity(tutorialKeyspace);
        } else if ( cmd.equalsIgnoreCase("get_slice_sc")) {
            return new GetSliceForStateCity(tutorialKeyspace);
        } else if ( cmd.equalsIgnoreCase("multiget_slice")) {
            return new MultigetSliceForNpanxx(tutorialKeyspace);
        } else if ( cmd.equalsIgnoreCase("get_indexed_slices")) {
            return new GetIndexedSlicesForCityState(tutorialKeyspace);
        }
        return null;
    }

}
