package com.datastax.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;


/**
 * Uses batch_mutate to insert several rows into mutliple column families in 
 * one execution.
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="insert" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 * 
 * @author zznate
 */
public class InsertRowsForColumnFamilies extends TutorialCommand {

    public InsertRowsForColumnFamilies(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<?> execute() {
        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
        
        mutator.addInsertion("CA Burlingame", "StateCity", HFactory.createColumn(650L, "37.57x122.34",longSerializer,stringSerializer));
        mutator.addInsertion("650", "AreaCode", HFactory.createStringColumn("Burlingame__650", "37.57x122.34"));
        mutator.addInsertion("650222", "Npanxx", HFactory.createStringColumn("lat", "37.57"));
        mutator.addInsertion("650222", "Npanxx", HFactory.createStringColumn("lng", "122.34"));
        mutator.addInsertion("650222", "Npanxx", HFactory.createStringColumn("city", "Burlingame"));
        mutator.addInsertion("650222", "Npanxx", HFactory.createStringColumn("state", "CA"));                
        
        MutationResult mr = mutator.execute();
        return null;
    }

}
