package com.datastax.tutorial;

import java.io.IOException;
import java.util.Properties;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class TutorialBase {
    static Cluster tutorialCluster;
    static Keyspace tutorialKeyspace;
    static Properties properties;
    
    protected static void init() {
        properties = new Properties();
        try {
            properties.load(TutorialBase.class.getResourceAsStream("/tutorial.properties"));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        // To modify the default ConsistencyLevel of QUORUM, create a 
        // me.prettyprint.hector.api.ConsistencyLevelPolicy and use the overloaded form:
        // tutorialKeyspace = HFactory.createKeyspace("Tutorial", tutorialCluster, consistencyLevelPolicy);
        // see also me.prettyprint.cassandra.model.ConfigurableConsistencyLevelPolicy[Test] for details
        
        tutorialCluster = HFactory.getOrCreateCluster(properties.getProperty("cluster.name", "TutorialCluster"), 
                properties.getProperty("cluster.hosts", "127.0.0.1:9160"));
        ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
        ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
        tutorialKeyspace = HFactory.createKeyspace(properties.getProperty("tutorial.keyspace", "Tutorial"), tutorialCluster, ccl);
    }
}
