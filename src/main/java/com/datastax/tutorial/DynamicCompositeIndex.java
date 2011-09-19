package com.datastax.tutorial;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Uses DynamicComparator to store two different indexes within the same
 * key: NPA first and City first.
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.args="dynamic_comp_index" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 */
public class DynamicCompositeIndex extends TutorialCommand {

  private static DynamicCompositeSerializer dcs = new DynamicCompositeSerializer(); 
  
  public DynamicCompositeIndex(Keyspace keyspace) {
    super(keyspace);
  }

  @Override
  public ResultStatus execute() {
 Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
    
    HColumnImpl<DynamicComposite, String> column = new HColumnImpl<DynamicComposite, String>(dcs, stringSerializer);
    column.setClock(keyspace.createClock());        
    DynamicComposite dc = new DynamicComposite();
    dc.add(0, "Austin");
    dc.add(1, 7516L);
    dc.add(2, 225L);
    column.setName(dc);       
    column.setValue("TIME WARNER COMMUNICATIONS AXS OF AUSTIN, TX");
    mutator.addInsertion("TX:512", "StateNpaIndexDynamic", column);
    
    column = new HColumnImpl<DynamicComposite, String>(dcs, stringSerializer);
    column.setClock(keyspace.createClock());
    dc = new DynamicComposite();
    dc.add(0, "225");
    //dc.add(1, 7516L);    
    column.setName(dc);       
    column.setValue("TIME WARNER COMMUNICATIONS AXS OF AUSTIN, TX");
    mutator.addInsertion("TX:512", "StateNpaIndexDynamic", column);
    
    column = new HColumnImpl<DynamicComposite, String>(dcs, stringSerializer);
    column.setClock(keyspace.createClock());
    dc = new DynamicComposite();
    dc.add(0, "Austin");
    dc.add(1, 7516L);
    dc.add(2, 334L);
    column.setName(dc);       
    column.setValue("TIME WARNER COMMUNICATIONS AXS OF AUSTIN, TX");
    mutator.addInsertion("TX:512", "StateNpaIndexDynamic", column);
    
    column = new HColumnImpl<DynamicComposite, String>(dcs, stringSerializer);
    column.setClock(keyspace.createClock());
    dc = new DynamicComposite();
    dc.add(0, "334");
    dc.add(1, 7516L);
    column.setName(dc);       
    column.setValue("TIME WARNER COMMUNICATIONS AXS OF AUSTIN, TX");
    mutator.addInsertion("TX:512", "StateNpaIndexDynamic", column);
   
    column = new HColumnImpl<DynamicComposite, String>(dcs, stringSerializer);
    column.setClock(keyspace.createClock());
    dc = new DynamicComposite();   
    dc.add(0, "Austin");
    dc.add(1, 7516L);
    dc.add(2, 439L);
    column.setName(dc);       
    column.setValue("TIME WARNER COMMUNICATIONS AXS OF AUSTIN, TX");
    mutator.addInsertion("TX:512", "StateNpaIndexDynamic", column);
   
    column = new HColumnImpl<DynamicComposite, String>(dcs, stringSerializer);
    column.setClock(keyspace.createClock());
   
    dc = new DynamicComposite();
    dc.add(0, "Austin");
    dc.add(1, 5830L);
    dc.add(2, 215L);
    column.setName(dc);       
    column.setValue("TIME WARNER COMMUNICATIONS AXS OF AUSTIN, TX");
    mutator.addInsertion("TX:512", "StateNpaIndexDynamic", column);
    
    column = new HColumnImpl<DynamicComposite, String>(dcs, stringSerializer);
    column.setClock(keyspace.createClock());
    dc = new DynamicComposite();    
    dc.add(0, "Lockhart");
    dc.add(1, 9533L);
    dc.add(2, 227L);
    column.setName(dc);       
    column.setValue("SOUTHWESTERN BELL");
    mutator.addInsertion("TX:512", "StateNpaIndexDynamic", column);
        
    mutator.execute();
    
    SliceQuery<String, DynamicComposite, String> sliceQuery = 
      HFactory.createSliceQuery(keyspace, stringSerializer, dcs, stringSerializer);
    sliceQuery.setColumnFamily("StateNpaIndexDynamic");
    sliceQuery.setKey("TX:512");

    DynamicComposite startRange = new DynamicComposite();
    //startRange.add(0, "225");
    startRange.add(0, "Austin");
    //startRange.addComponent(new Long(5830), LongSerializer.get(), "LongType", AbstractComposite.ComponentEquality.LESS_THAN_EQUAL);
    //startRange.add(1, 5830L);
    
    
    DynamicComposite endRange = new DynamicComposite();    
    //endRange.add(0, "225" + Character.MAX_VALUE);
    endRange.add(0, "Austin");
    endRange.addComponent(new Long(5830), LongSerializer.get(), "LongType", AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL);
    
    sliceQuery.setRange(startRange, endRange, false, 10);

    QueryResult<ColumnSlice<DynamicComposite, String>> result = sliceQuery.execute();
    return result;
  }
  
  // findByNpaOnly
  //   startRange.add(0, "225")
  
  // rangeToOneResult
  //  startRange.add(0, "Austin");
  //  endRange.add(0, "Austin");
  //  endRange.addComponent(new Long(5830), LongSerializer.get(), "LongType", AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL);
  
  // rangeToCityNpaMax

}
