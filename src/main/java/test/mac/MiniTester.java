package test.mac;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Stopwatch;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import test.mac.offline.OfflineSplitTester;
import test.mac.offline.OfflineTableCreator;
import test.mac.rfiles.CreateRfiles;
import test.mac.util.MiniUtils;

public class MiniTester {

  public static void main(String[] args) throws Exception {
    MiniTester tester = new MiniTester();
    //tester.unoRunner();
    tester.execute();
  }

  private void unoRunner()
      throws IOException, TableExistsException, AccumuloSecurityException, AccumuloException {
    Connector conn = null;
    conn = Connector.builder().
          usingProperties("/home/mark/dev/uno/install/accumulo-2.0.0-SNAPSHOT/conf/accumulo"
              + "-client.properties").build();

    Properties props = MiniUtils.readClientProperties();
    String splitfile = props.getProperty("split.file");
    MiniUtils.msg("Splitfile : " + splitfile);

    OfflineTableCreator otc = new OfflineTableCreator(null);
    //otc.createTable(conn, "test22", new NewTableConfiguration());
    //otc.createTable(conn, "offlineTable", new NewTableConfiguration().createOffline());
    otc.createTable(conn, "splitTableOffline", new NewTableConfiguration().createOffline()
        .withPartitions(splitfile));
    otc.createTable(conn, "splitTableOnline", new NewTableConfiguration().
        withPartitions(splitfile), true);
  }

  private void execute()
      throws IOException, InterruptedException, TableNotFoundException, AccumuloSecurityException,
      TableExistsException, AccumuloException {
    try {
      MiniUtils.startMiniCluster();

      // List testing classes below here

      // Work on creating splits offline at table startup
      //testAddSplits();

      // work on just creating an offline table
      testCreateOfflineTable();

      // Create rfiles for bulk import testing
      //testRFileCreation();

    } finally {
      MiniUtils.stopMiniCluster();
    }
  }

  private void testCreateOfflineTable()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    OfflineTableCreator creator = new OfflineTableCreator(MiniUtils.getMac());
    creator.createOfflineTable();
  }

  private void testRFileCreation() throws IOException {
    CreateRfiles creator = new CreateRfiles();
    creator.createRfiles(10);
  }

  private void testAddSplits()
      throws TableExistsException, AccumuloSecurityException, AccumuloException,
      TableNotFoundException {
    OfflineSplitTester tester = new OfflineSplitTester(MiniUtils.getMac());
    tester.createTableWithMetadata();

  }
}
