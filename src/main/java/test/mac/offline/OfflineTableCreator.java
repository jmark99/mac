package test.mac.offline;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import com.google.common.base.Stopwatch;
import org.apache.hadoop.io.Text;
import test.mac.util.MiniUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

public class OfflineTableCreator {

  private MiniAccumuloCluster mac;

  public OfflineTableCreator(MiniAccumuloCluster mac) {
    this.mac = mac;
  }

  public void createOfflineTable()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {

    MiniUtils.pause("Started miniCluster with " + mac.getConfig().getNumTservers() + " "
        + "tservers...");

    Properties props = MiniUtils.getProps();
    Connector conn = Connector.builder().usingProperties(props).build();
    String splitfile = props.getProperty("split.file");
    MiniUtils.msg("Splitfile : " + splitfile);

    createTable(conn, "onlineTable", new NewTableConfiguration());

//    MiniUtils.printTableIdInfo(conn);
//    MiniUtils.pause("Created onlineTable");

    createTable(conn, "offlineTable", new NewTableConfiguration().createOffline());

    MiniUtils.printTableIdInfo(conn);
    MiniUtils.pause("Created offlineTable");


    // Read a file of splits and place them into a collection that will be passed to
    // newTableCreation
    SortedSet<Text> splits = new TreeSet<>();
    // Read splits from slit file into splits
    // Then withSplits will write the collecxtion into a
    // file on HDFS and use that file to parse through the split points.

    try (Stream<String> lines = Files.lines(Paths.get(splitfile), StandardCharsets.UTF_8)) {
      lines.forEachOrdered(split -> {
        splits.add(new Text(split));
      });
    } catch (IOException e) {
      e.printStackTrace();
    }


    createTable(conn, "splitTableOffline", new NewTableConfiguration().createOffline()
        .withSplits(splits));

//    MiniUtils.printTableIdInfo(conn);
//    MiniUtils.pause("Created splitTableOffline");
//
//    try {
//      MiniUtils.msg("call online for table...");
//      Stopwatch timer = Stopwatch.createUnstarted();
//      timer.start();
//      conn.tableOperations().online("splitTableOffline", true);
//      timer.stop();
//      MiniUtils.msg("splitTableOffline took " + timer + " to come online");
//    } catch (TableNotFoundException e) {
//      e.printStackTrace();
//    }
//
//    MiniUtils.pause("table now online..");
//
    createTable(conn, "splitTableOnline", new NewTableConfiguration().
        withSplits(splits), true);

    MiniUtils.printTableIdInfo(conn);
    MiniUtils.pause("Created splitTableOnline");

    MiniUtils.pause("exiting..");

  }


  public void createTable(Connector conn, final String tableName, NewTableConfiguration ntc)
      throws AccumuloSecurityException, AccumuloException, TableExistsException {
    createTable(conn, tableName, ntc, false);
  }


  public void createTable(Connector conn, final String tableName, NewTableConfiguration ntc,
      boolean timeit)
      throws AccumuloSecurityException, AccumuloException, TableExistsException {
    MiniUtils.msg("create table " + tableName);
    Stopwatch timer = Stopwatch.createUnstarted();
    timer.start();
    conn.tableOperations().create(tableName, ntc);
    if (timeit) {
      Map<String,String> idMap = conn.tableOperations().tableIdMap();
      String tableId = idMap.get(tableName);
      MiniUtils.msg("TableId = " + tableId);
      boolean hasBeenNonZero = false;
      timer.stop();
      MiniUtils.msg("Took " + timer + " to write metadata");
      timer.start();
      int i = 0;
      while(true) {
        int fcnt = 0;
        try (Scanner scan = conn.createScanner("accumulo.metadata", Authorizations.EMPTY)) {
          scan.setRange(Range.prefix(tableId));
          scan.fetchColumnFamily(new Text("future"));
          for(Map.Entry<Key,Value> entry : scan) {
            fcnt++;
          }
          MiniUtils.msg("FutureCount: " + fcnt);
        } catch (TableNotFoundException e) {
          e.printStackTrace();
        }
        if (fcnt > 0) hasBeenNonZero = true;
        if (fcnt == 0 && hasBeenNonZero == true) {
          break;
        }
        i++;
        if  (fcnt > 15000)
          MiniUtils.sleep(60000);
        else
          MiniUtils.sleep(10000);
      }
    }
    timer.stop();
    MiniUtils.msg("Creation of " + tableName + " took " + timer);
  }
}
