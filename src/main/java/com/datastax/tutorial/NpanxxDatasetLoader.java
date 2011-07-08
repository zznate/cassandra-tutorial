package com.datastax.tutorial;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load the npanxx data from a file. An example of a multi-threaded file loader
 * using batch_mutate via Hector's Mutator
 * 
 * @author zznate
 */
public class NpanxxDatasetLoader extends TutorialBase {
    private static Logger log = LoggerFactory.getLogger(NpanxxDatasetLoader.class);
    
    private static ExecutorService exec;
    
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        init();
        String fileLocation = properties.getProperty("npanxx.file.location");
        BufferedReader reader;
        exec = Executors.newFixedThreadPool(10);
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileLocation)));
            // read 1000 and hand off to worker
            
            List<String> lines = new ArrayList<String>(1000);
            String line = reader.readLine();
            
            List<Future<Integer>> sums = new ArrayList<Future<Integer>>();
            while(line != null) {
                
                lines.add(line);
                if ( lines.size() % 1000 == 0 ) {                                        
                    doParse(lines, sums);
                }
                line = reader.readLine();
            }
            doParse(lines, sums);
            
            int total = 0;
            for (Future<Integer> future : sums) {
                total = total + future.get().intValue();
            }
            
            log.info("Found total: {}", total);
            
            log.info("duration in ms: {}",System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            log.error("Could not locate file",e);
        } finally {
            exec.shutdown();
        }
        tutorialCluster.getConnectionManager().shutdown();
    }

    private static void doParse(List<String> lines, List<Future<Integer>> sums) {
        Future<Integer> f = exec.submit(new NpanxxDatasetLoader().new LineParser(new ArrayList(lines)));
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
            NpanxxLine npanxxLine;
            Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, StringSerializer.get());
            for (String row : lines) {
                // parse
                npanxxLine = new NpanxxLine(row);
                // 
                String key = npanxxLine.getNpa()+npanxxLine.getNxx();
                mutator.addInsertion(key, "Npanxx", HFactory.createStringColumn("city", npanxxLine.getCity()));
                mutator.addInsertion(key, "Npanxx", HFactory.createStringColumn("lat", Double.toString(npanxxLine.getLat())));
                mutator.addInsertion(key, "Npanxx", HFactory.createStringColumn("lng", Double.toString(npanxxLine.getLng())));
                mutator.addInsertion(key, "Npanxx", HFactory.createStringColumn("state", npanxxLine.getState()));
                if ( count % 250 == 0 ) {
                    mutator.execute();
                    mutator.discardPendingMutations();
                }
                count++;
            }
            mutator.execute();
            log.info("found count {}", count);
            return Integer.valueOf(count);
        }
        
    }
    
    static class NpanxxLine {
        private String[] vals = new String[10];
        
        NpanxxLine(String line) {
            vals = line.split("\\s");
        }
        
        String getNpa() {
            return vals[0];            
        }
        String getNxx() {
            return vals[1]; 
        }
        double getLat() {
            return Double.parseDouble(vals[2]);
        }
        double getLng() {
            return Double.parseDouble(vals[3]);
        }
        String getState() {
            return vals[5];
        }
        String getCity() {
            StringBuilder cityName = new StringBuilder(56);
            for (int i = 6; i < vals.length; i++) {
                cityName.append(vals[i]).append(" ");
            }
            return cityName.toString();
        }
    }
}
