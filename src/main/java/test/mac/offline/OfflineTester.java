package test.mac.offline;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import test.mac.util.MiniUtils;

import java.util.Properties;

public class OfflineTester {

  MiniAccumuloCluster mac;

  public OfflineTester(MiniAccumuloCluster mac) {
    this.mac = mac;
  }

  /**
   * Work on getting accumulo to create a table with pre-computed splits and tablets
   * spread among the cluster.
   * <p>
   * - Create table
   * -- can an accumulo client connect to this???
   * - get table id
   * - read metadata
   * - take offline
   * - collect necessary netadata
   * - write split info to metadata
   * - bring online and spread across cluster
   */
  public void createTableWithMetadata()
      throws AccumuloSecurityException, AccumuloException, TableExistsException {

    MiniUtils.msg("Start createWithSplits...");
    String tableName = "test1";

    Properties props = MiniUtils.getProps();
    Connector conn = Connector.builder().usingProperties(props).build();
    conn.tableOperations().create(tableName);
    MiniUtils.printTableIdInfo(conn);
    //        SortedSet<Text> splits = new TreeSet<>();
    //        for (int i = 10_000; i < 20_000; i = i + 1000)
    //            splits.add(new Text(rowStr(i)));
    //        conn.tableOperations().addSplits(tableName, splits);
    //conn.tableOperations().offline(tableName);
    MiniUtils.pause();

  }
}
