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
        MiniUtils.msg("Run cluster-only");
        tester.runClusterOnly();
      }
    } else {
        MiniUtils.msg("Create tables with splits.");
        tester.execute();
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
      throws IOException, InterruptedException, AccumuloSecurityException,
      TableExistsException, AccumuloException {
    try {
      MiniUtils.startMiniCluster();
      testCreateOfflineTables();
    } finally {
      MiniUtils.stopMiniCluster();
    }
  }

  private void testCreateOfflineTables()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    OfflineTableCreator creator = new OfflineTableCreator(MiniUtils.getMac());
    creator.createOfflineTable();
  }
}
