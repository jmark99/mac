package test.mac;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.accumulo.minicluster.MiniAccumuloConfig;

import test.mac.offline.OfflineTester;
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
      testCreateOfflineTable();

      // Create rfiles for bulk import testing
      //testRFileCreation();

    } finally {
      MiniUtils.stopMiniCluster();
    }
  }

  private void testRFileCreation() throws IOException {
    CreateRfiles creator = new CreateRfiles();
    creator.createRfiles(10);
  }

  private void testCreateOfflineTable()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    OfflineTester tester = new OfflineTester(MiniUtils.getMac());
    tester.createTableWithMetadata();

  }
}
