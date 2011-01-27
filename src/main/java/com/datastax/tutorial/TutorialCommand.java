package com.datastax.tutorial;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.query.QueryResult;

public abstract class TutorialCommand {
    
    protected Logger log = LoggerFactory.getLogger(TutorialCommand.class);
    
    protected Keyspace keyspace;
    
    public TutorialCommand(Keyspace keyspace) {
        this.keyspace = keyspace;
    }
    
    public abstract QueryResult<?> execute();
    

    static StringSerializer stringSerializer = StringSerializer.get();
    static IntegerSerializer integerSerializer = IntegerSerializer.get();
        
}
