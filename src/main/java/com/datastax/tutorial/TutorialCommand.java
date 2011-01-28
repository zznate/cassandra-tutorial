package com.datastax.tutorial;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.query.QueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for individual tutorial classes to implement.
 * The {@link Serializer} implementations are defined here for 
 * convienience. 
 * 
 * @author zznate
 */
public abstract class TutorialCommand {
    
    protected Logger log = LoggerFactory.getLogger(TutorialCommand.class);
    
    protected Keyspace keyspace;
    
    public TutorialCommand(Keyspace keyspace) {
        this.keyspace = keyspace;
    }
    
    /**
     * To be implemented by tutorial classes. 
     * 
     * @return {@link QueryResult} which is typed quite differently 
     * depending on the implementation 
     */
    public abstract QueryResult<?> execute();
    
    static StringSerializer stringSerializer = StringSerializer.get();
    static LongSerializer longSerializer = LongSerializer.get();
        
}
