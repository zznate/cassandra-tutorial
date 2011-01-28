package com.datastax.tutorial;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.query.QueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TutorialCommand {
    
    protected Logger log = LoggerFactory.getLogger(TutorialCommand.class);
    
    protected Keyspace keyspace;
    
    public TutorialCommand(Keyspace keyspace) {
        this.keyspace = keyspace;
    }
    
    public abstract QueryResult<?> execute();
    

    static StringSerializer stringSerializer = StringSerializer.get();
    static LongSerializer longSerializer = LongSerializer.get();
        
}
