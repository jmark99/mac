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
    if (args.length > 0) {
      if (args[0].equals("cluster-only")) {
        tester.runClusterOnly();
      } else {
        tester.execute();
      }
    }
  }

  private void runClusterOnly() throws IOException, InterruptedException {
    try {
      MiniUtils.startMiniCluster();
      MiniUtils.pause();
    } finally {
      MiniUtils.stopMiniCluster();
    }
  }

  private void execute()
      throws IOException, InterruptedException, TableNotFoundException, AccumuloSecurityException,
      TableExistsException, AccumuloException {
    try {
      MiniUtils.startMiniCluster();
      testCreateOfflineTable();
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
