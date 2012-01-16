package com.datastax.tutorial;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="static_comp_index" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 */
public class StaticCompositeIndex extends TutorialCommand {

  private static final CompositeSerializer cs = new CompositeSerializer(); 
  
  public StaticCompositeIndex(Keyspace keyspace) {
    super(keyspace);
  }

  @Override
  public ResultStatus execute() {
    Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
    
    HColumnImpl<Composite, String> column = new HColumnImpl<Composite, String>(cs, stringSerializer);
    column.setClock(keyspace.createClock());
    Composite dc = new Composite();
    dc.add(0, "Austin");
    dc.add(1, 7516L);
    dc.add(2, 225L);
    column.setName(dc);       
    column.setValue("TIME WARNER COMMUNICATIONS AXS OF AUSTIN, TX");
    mutator.addInsertion("TX:512", "StateNpaIndexStatic", column);
    
    column = new HColumnImpl<Composite, String>(cs, stringSerializer);
    column.setClock(keyspace.createClock());
    dc = new Composite();
    dc.add(0, "Austin");
    dc.add(1, 7516L);
    dc.add(2, 334L);
    column.setName(dc);       
    column.setValue("TIME WARNER COMMUNICATIONS AXS OF AUSTIN, TX");
    mutator.addInsertion("TX:512", "StateNpaIndexStatic", column);
   
    column = new HColumnImpl<Composite, String>(cs, stringSerializer);
    column.setClock(keyspace.createClock());
    dc = new Composite();
    dc.add(0, "Austin");
    dc.add(1, 7516L);
    dc.add(2, 439L);
    column.setName(dc);       
    column.setValue("TIME WARNER COMMUNICATIONS AXS OF AUSTIN, TX");
    mutator.addInsertion("TX:512", "StateNpaIndexStatic", column);
   
    column = new HColumnImpl<Composite, String>(cs, stringSerializer);
    column.setClock(keyspace.createClock());
    dc = new Composite();
    dc.add(0, "Austin");
    dc.add(1, 5830L);
    dc.add(2, 215L);
    column.setName(dc);       
    column.setValue("TIME WARNER COMMUNICATIONS AXS OF AUSTIN, TX");
    mutator.addInsertion("TX:512", "StateNpaIndexStatic", column);

        column = new HColumnImpl<Composite, String>(cs, stringSerializer);
    column.setClock(keyspace.createClock());
    dc = new Composite();
    dc.add(0, "Aardvark");
    dc.add(1, 5830L);
    dc.add(2, 215L);
    column.setName(dc);
    column.setValue("Aardvark telco");
    mutator.addInsertion("TX:512", "StateNpaIndexStatic", column);
    
    column = new HColumnImpl<Composite, String>(cs, stringSerializer);
    column.setClock(keyspace.createClock());
    dc = new Composite();
    dc.add(0, "Lockhart");
    dc.add(1, 9533L);
    dc.add(2, 227L);
    column.setName(dc);       
    column.setValue("SOUTHWESTERN BELL");
    mutator.addInsertion("TX:512", "StateNpaIndexStatic", column);
        
    mutator.execute();
    
    SliceQuery<String, Composite, String> sliceQuery = 
      HFactory.createSliceQuery(keyspace, stringSerializer, cs, stringSerializer);
    sliceQuery.setColumnFamily("StateNpaIndexStatic");
    sliceQuery.setKey("TX:512");

    Composite startRange = new Composite();
    startRange.add(0, "A");
    //startRange.add(1, 7516L);
    //startRange.addComponent(new Long(0), LongSerializer.get(), "LongType", AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL);
    //startRange.addComponent(439L, LongSerializer.get(), "LongType", AbstractComposite.ComponentEquality.EQUAL);
    //startRange.add(1, Long.MIN_VALUE);
    //startRange.add(2, 439L);
    
    
    Composite endRange = new Composite();
    //endRange.add(0, "Austin" + Character.MAX_VALUE);
    endRange.add(0, "B");
    // the following statement is effectively identical for the purposes of restricting to 'Austin'
    //endRange.addComponent("Austin", StringSerializer.get(), "UTF8Type", AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL);
    
    sliceQuery.setRange(startRange, endRange, false, 10);

    QueryResult<ColumnSlice<Composite, String>> result = sliceQuery.execute();
    ColumnSlice<Composite, String> cs = result.get();
    for ( HColumn<Composite, String> col: cs.getColumns() ) {
      System.out.println(col.getName().getComponents());
      System.out.println(col.getName().get(0, StringSerializer.get()));
    }
    return result;
    

  }

}
