package com.datastax.tutorial;

import java.util.List;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 *
 * To run this example from maven:
 * mvn -e exec:java -Dexec.args="comp_ranges_with_long" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
 */
public class CompositeRangesWithLong extends TutorialCommand {

  private static final CompositeSerializer cs = new CompositeSerializer();

  public CompositeRangesWithLong(Keyspace keyspace) {
    super(keyspace);
  }

  @Override
  public ResultStatus execute() {
    SliceQuery<String, Composite, String> sliceQuery =
      HFactory.createSliceQuery(keyspace, stringSerializer, cs, stringSerializer);
    sliceQuery.setColumnFamily("CompositeRangesWithLong");
    sliceQuery.setKey("x");

    Composite startRange = new Composite();
    startRange.addComponent(0,new Long(0), AbstractComposite.ComponentEquality.EQUAL);
    //startRange.addComponent(new Long(0), LongSerializer.get());

    Composite endRange = new Composite();
    endRange.addComponent(0, new Long(0), AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL);

    sliceQuery.setRange(startRange, endRange, false, 100);

    QueryResult<ColumnSlice<Composite,String>> r = sliceQuery.execute();
    ColumnSlice<Composite, String> slice = r.get();

    List<HColumn<Composite, String>> l = slice.getColumns();

    System.out.println("Querying Component1 as <= " + "1" + ". Result: # columns in slice: " + l.size() + ", Columns:");
    for (int i=0;i<l.size();i++) {
      System.out.println(" Component1: " + l.get(i).getName().get(0, LongSerializer.get()) +
        ", Component2: " + l.get(i).getName().get(1, LongSerializer.get()));
    }



    return null;
  }
  
}
/*
create column family CompositeRangesWithLong
with column_type = 'Standard'
and comparator = 'CompositeType(org.apache.cassandra.db.marshal.LongType,org.apache.cassandra.db.marshal.LongType)';
*/