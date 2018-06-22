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

    MiniUtils.msg("Started miniCluster with " + mac.getConfig().getNumTservers() + " " + "tservers...");

    Properties props = MiniUtils.getProps();
    Connector conn = Connector.builder().usingProperties(props).build();
    String splitfile = props.getProperty("split.file");
    MiniUtils.msg("Splitfile : " + splitfile);

    createTable(conn, "onlineTable", new NewTableConfiguration());
    //MiniUtils.printTableIdInfo(conn);
    MiniUtils.msg("Created onlineTable");

    createTable(conn, "offlineTable", new NewTableConfiguration().createOffline());
    //MiniUtils.printTableIdInfo(conn);
    MiniUtils.msg("Created offlineTable");

    // Read a file of splits and place them into a collection that will be passed to
    // newTableCreation
    // Read splits from slit file into splits
    // Then withSplits will write the collecxtion into a
    // file on HDFS and use that file to parse through the split points.
    SortedSet<Text> splits = new TreeSet<>();
    try (Stream<String> lines = Files.lines(Paths.get(splitfile), StandardCharsets.UTF_8)) {
      lines.forEachOrdered(split -> {
        splits.add(new Text(split));
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
    MiniUtils.msg("Number of splits to add: " + splits.size());

    createTable(conn, "splitTableOffline", new NewTableConfiguration().createOffline().withSplits(splits));
    //MiniUtils.printTableIdInfo(conn);
    MiniUtils.msg("Created splitTableOffline");

    try {
      MiniUtils.msg("call online for table...");
      Stopwatch timer = Stopwatch.createUnstarted();
      timer.start();
      conn.tableOperations().online("splitTableOffline", true);
      timer.stop();
      MiniUtils.msg("splitTableOffline took " + timer + " to come online");
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }
    MiniUtils.msg("table now online..");

    createTable(conn, "splitTableOnline", new NewTableConfiguration().
        withSplits(splits), true);
    //MiniUtils.printTableIdInfo(conn);
    MiniUtils.msg("Created splitTableOnline");

    MiniUtils.pause("exiting..");
  }

  public void createTable(Connector conn, final String tableName, NewTableConfiguration ntc)
      throws AccumuloSecurityException, AccumuloException, TableExistsException {
    createTable(conn, tableName, ntc, false);
  }

  public void createTable(Connector conn, final String tableName, NewTableConfiguration ntc,
      boolean timeit) throws AccumuloSecurityException, AccumuloException, TableExistsException {
    MiniUtils.msg("create table " + tableName);
    Stopwatch timer = Stopwatch.createUnstarted();
    timer.start();
    conn.tableOperations().create(tableName, ntc);
    timer.stop();
    MiniUtils.msg("Call to create returned in " + timer);
    if (timeit)
      timeMetadataCreation(conn, tableName, ntc.getSplits().size(), timer);
  }

  private void timeMetadataCreation(Connector conn, String tableName, int splitSize, Stopwatch timer) {
    timer.start();
    int futureCnt = 0; int locCnt = 0;
    Scanner fScanner = null; Scanner lScanner = null;
    String tableId = getTableId(conn, tableName);
    int pCnt = 0;
    while (locCnt < splitSize) {
      try {
        lScanner = getColumnScammer(conn, tableId, "loc");
        locCnt = getEntryCount(lScanner);
      } finally {
        lScanner.close();
      }
      if (pCnt++ % 6 == 0)
        MiniUtils.msg("loc : " +  locCnt);
      MiniUtils.sleep(5000);
    }
    timer.stop();
    MiniUtils.msg("Metadata updates for " + tableName + " completed in " + timer);
  }

  private Scanner getColumnScammer(Connector conn, String tableId, String column) {
    Scanner scanner = null;
    try {
      scanner = conn.createScanner("accumulo.metadata", Authorizations.EMPTY);
      scanner.setRange(Range.prefix(tableId));
      scanner.fetchColumnFamily(new Text(column));
    } catch (TableNotFoundException e) {
      MiniUtils.msg("TableNotFoundException: " + e.getMessage());
    }
    return scanner;
  }

  private int getEntryCount(Scanner scanner) {
    int cnt = 0;
    for(Map.Entry<Key,Value> entry : scanner)
      cnt++;
    return cnt;
  }

  private String getTableId(Connector conn, String tableName) {
    Map<String,String> idMap = conn.tableOperations().tableIdMap();
    String tableId = idMap.get(tableName);
    return tableId;
  }
}
