package test.mac;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

import test.mac.offline.OfflineSplitTester;
import test.mac.offline.OfflineTableCreator;
import test.mac.rfiles.CreateRfiles;
import test.mac.util.MiniUtils;

public class MiniTester {

  public static void main(String[] args) throws Exception {
    MiniTester tester = new MiniTester();
    tester.execute();
  }

  private void execute()
      throws IOException, InterruptedException, TableNotFoundException, AccumuloSecurityException,
      TableExistsException, AccumuloException {
    try {
      MiniUtils.startMiniCluster();

      // List testing classes below here

      // Work on creating splits offline at table startup
      testAddSplits();

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
