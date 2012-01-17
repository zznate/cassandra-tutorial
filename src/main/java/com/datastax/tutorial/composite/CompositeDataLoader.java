package com.datastax.tutorial.composite;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.datastax.tutorial.TutorialBase;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-threaded bulk loader for geo data located in the data directory of the
 * project root.
 *
 * Execute this class by invoking the following at the project root:
 * mvn -e exec:java -Dexec.mainClass="com.datastax.tutorial.composite.CompositeDataLoader"
 * @author zznate
 */
public class CompositeDataLoader extends TutorialBase {
  private static Logger log = LoggerFactory.getLogger(CompositeDataLoader.class);

  private static ExecutorService exec;
  // key for static composite, First row of dynamic composite
  public static final String COMPOSITE_KEY = "ALL";
  public static final String COUNTRY_STATE_CITY_CF = "CountryStateCity";

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();
    init();
    String fileLocation = properties.getProperty("composites.geodata.file.location","data/geodata.txt");
    BufferedReader reader;
    exec = Executors.newFixedThreadPool(5);
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileLocation)));
      // read 1000 and hand off to worker

      List<String> lines = new ArrayList<String>(1000);
      String line = reader.readLine();

      List<Future<Integer>> sums = new ArrayList<Future<Integer>>();
      while(line != null) {

        lines.add(line);
        if ( lines.size() % 250 == 0 ) {
          doParse(lines, sums);
        }
        line = reader.readLine();
      }
      doParse(lines, sums);

      int total = 0;
      for (Future<Integer> future : sums) {
        // naive wait for completion
        total = total + future.get().intValue();
      }

      log.info("Inserted a total of {} over duration ms: {}", total, System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      log.error("Could not locate file",e);
    } finally {
      exec.shutdown();
    }
    tutorialCluster.getConnectionManager().shutdown();
  }

  private static void doParse(List<String> lines, List<Future<Integer>> sums) {
    Future<Integer> f = exec.submit(new CompositeDataLoader().new LineParser(new ArrayList(lines)));
    sums.add(f);
    lines.clear();
  }


  class LineParser implements Callable<Integer> {

    List<String> lines;
    LineParser(List<String> lines) {
      this.lines = lines;
    }

    public Integer call() throws Exception {
      int count = 0;
      GeoDataLine geoDataLine;
      Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, StringSerializer.get());

      for (String row : lines) {
        // parse
        geoDataLine = new GeoDataLine(row);
        // assemble the insertions
        mutator.addInsertion(COMPOSITE_KEY, COUNTRY_STATE_CITY_CF, geoDataLine.staticColumnFrom());
        
        count++;
      }
      mutator.execute();
      log.debug("Inserted {} columns", count);
      return Integer.valueOf(count);
    }

  }

  /**
   * This is probably overkill given the simplicity of the data, but is
   * good practice for separation of concerns and encapsulation
   */
  static class GeoDataLine {
    public static final char SEPARATOR_CHAR = ',';
    private String[] vals = new String[10];

    GeoDataLine(String line) {
      vals = StringUtils.split(StringEscapeUtils.unescapeCsv(line), SEPARATOR_CHAR);
      log.debug("array size: {} for row: {}", vals.length, line);
    }

    /**
     * Creates an HColumn with a column name composite of the form:
     *   ['country_code']:['state]:['city name'])
     * and a value of ['timezone']
     * @return
     */
    HColumn<Composite,String> staticColumnFrom() {

      Composite composite = new Composite();
      composite.addComponent(getCountryCode(), StringSerializer.get());
      composite.addComponent(getAdmin1Code(), StringSerializer.get());
      // extra un-escape to handle the case of "Washington, D.C." 
      composite.addComponent(StringEscapeUtils.unescapeCsv(getAsciiName()), StringSerializer.get());
      HColumn<Composite,String> col =
        HFactory.createColumn(composite, getTimezone(), new CompositeSerializer(), StringSerializer.get());
      return col;
    }


    String getCountryCode() {
      return vals[0];
    }
    String getAdmin1Code() {
      return vals[1];
    }
    String getAsciiName() {
      return vals[2];
    }
    String getTimezone() {
      return vals[3];
    }
  }
}
