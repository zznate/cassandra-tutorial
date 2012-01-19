package com.datastax.tutorial;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use the following create script:
 * use Tutorial;
 * create column family KeyIterableCf
 *   with comparator = 'LongType' and key_validation_class = 'UTF8Type' and default_validation_class = 'UTF8Type';
 *
 * mvn -e exec:java -Dexec.mainClass="com.datastax.tutorial.composite.CompositeDataLoader"
 * @author zznate
 */
public class KeyIteratorExample extends TutorialBase {

  private Logger log = LoggerFactory.getLogger(KeyIteratorExample.class);

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();
    init();
    ExecutorService exec = Executors.newFixedThreadPool(5);

    List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

    // request 50 invocation of RowInserter
    // each invocation creates 20 rows, thus we have 1000 rows of 50 columns
    for ( int x=0; x<50; x++ ) {
      futures.add(exec.submit(new KeyIteratorExample().new RowInserter()));
    }

    int total = 0;
    try {
      for ( Future<Integer> f : futures ) {
        total += f.get().intValue();
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    int keyCount = 0;
    KeyIterator<String> keyIterator = new KeyIterator<String>(tutorialKeyspace, "KeyIterableCf", StringSerializer.get());
    for ( String key : keyIterator ) {
      //xSystem.out.printf("Current key: %s \n",key);
      keyCount++;
    }
    System.out.printf("Iterated over %d keys \n", keyCount);
    exec.shutdown();
  }


  class RowInserter implements Callable<Integer> {

    /**
     * Each invocation creates 20 rows of 50 keys each
     * @return
     */
    public Integer call() {
      Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, StringSerializer.get());
      int count = 0;
      String myKey = TimeUUIDUtils.getTimeUUID(tutorialKeyspace.createClock()).toString();
      for (int x=0; x<1000; x++) {

        // assemble the insertions
        mutator.addInsertion(myKey,"KeyIterableCf", buildColumnFor(x));
        if ( x % 50 == 0 ) {
          myKey = TimeUUIDUtils.getTimeUUID(tutorialKeyspace.createClock()).toString();
          count++;
        }

      }
      mutator.execute();
      log.debug("Inserted {} rows", count);
      return Integer.valueOf(count);
    }
  }

  private HColumn<Long,String> buildColumnFor(int colName) {
    HColumn<Long,String> column = HFactory.createColumn(new Long(colName), RandomStringUtils.randomAlphanumeric(32),
      LongSerializer.get(), StringSerializer.get());
    return column;
  }
}
