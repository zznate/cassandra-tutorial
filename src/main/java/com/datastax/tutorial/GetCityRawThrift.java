package com.datastax.tutorial;

import me.prettyprint.cassandra.connection.HClientPool;
import me.prettyprint.cassandra.connection.HThriftClient;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * For the curios... behold raw Thrift API. 
 * 
 * @author nate
 *
 */
public class GetCityRawThrift extends TutorialCommand {

  public GetCityRawThrift(Keyspace keyspace) {
    super(keyspace);
  }
  
  @Override
  public ResultStatus execute() {
    HClientPool pool = null;
    HThriftClient client = null;
    try {
      // ewww. Dont do this. 
      // ... maybe expose a 'getThriftClient' at a very high level somewhere?
      pool = TutorialBase.tutorialCluster.getConnectionManager().getActivePools().iterator().next();
      client = pool.borrowClient();
      
      Cassandra.Client cassandra = client.getCassandra();
      cassandra.set_keyspace("Tutorial");
      ColumnOrSuperColumn cosc = cassandra.get(stringSerializer.toByteBuffer("512204"), 
          new ColumnPath("Npanxx").setColumn(ByteBufferUtil.bytes("city")), ConsistencyLevel.ONE);
      log.info("ColumnOrSuperColumn from thrift: {}", cosc);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if ( pool != null)
        pool.releaseClient(client);
    }
    return null;
  }

  
}
