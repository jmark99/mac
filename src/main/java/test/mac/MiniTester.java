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

import test.mac.rfiles.CreateRfiles;

public class MiniTester {

  private MiniAccumuloConfig config;
  private MiniAccumuloCluster mac;
  private Properties props = new Properties();

  public static void main(String[] args) throws Exception {
    MiniTester tester = new MiniTester();
    tester.execute();
  }

  private void execute()
      throws IOException, InterruptedException, TableNotFoundException, AccumuloSecurityException,
      TableExistsException, AccumuloException {
    try {
      startMiniCluster();
      testCreateOfflineTable();
      //testRFileCreation();
    } finally {
      msg("Stopping mini cluster...");
      mac.stop();
    }
  }

  private void testRFileCreation() throws Exception {
    CreateRfiles creator = new CreateRfiles();
    creator.createRfiles(10);
  }

  private void startMiniCluster() throws IOException, InterruptedException {
    props = readClientProperties();
    Path fileSystem = setupPseudoFileSystem(Paths.get(props.getProperty("pseudo.file.system")));
    config = configureMiniCluster(fileSystem, props.getProperty("instance.name"),
        props.getProperty("auth.password"),
        Integer.parseInt(props.getProperty("num" + ".tservers")),
        Integer.parseInt(props.getProperty("zk.port")));
    mac = new MiniAccumuloCluster(config);
    mac.start();
    msg("Started mini cluster...");
    printClusterInfo();

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
  private void testCreateOfflineTable()
      throws AccumuloException, TableExistsException, TableNotFoundException,
      AccumuloSecurityException {
    msg("Start createWithSplits...");
    String tableName = "test1";

    //Connector.builder().usingProperties("")
    Connector conn = mac.getConnector("root", "secret");
    conn.tableOperations().create(tableName);
    printTableIdInfo(conn);
    //        SortedSet<Text> splits = new TreeSet<>();
    //        for (int i = 10_000; i < 20_000; i = i + 1000)
    //            splits.add(new Text(rowStr(i)));
    //        conn.tableOperations().addSplits(tableName, splits);
    //conn.tableOperations().offline(tableName);
    pause();
  }

  private Properties readClientProperties() throws IOException {
    Properties props = new Properties();
    FileInputStream in;
    in = new FileInputStream(System.getProperty("user.dir") + "/mac.properties");
    props.load(in);
    in.close();
    return props;
  }

  private void printClusterInfo() {
    msg("Instance Name:  " + config.getInstanceName());
    msg("Root Pwd:       " + config.getRootPassword());
    msg("Default Memory: " + config.getDefaultMemory());
    msg("ZK Port;        " + config.getZooKeeperPort());
    msg("Dir:            " + config.getDir());
    Map<String,String> siteConfig = config.getSiteConfig();
    msg("Num TServers:  " + config.getNumTservers());
    if (siteConfig.size() > 0) {
      msg("Site Config: ");
      siteConfig.forEach((key, value) -> {
        msg("\t---" + key + " - " + value);
      });
    }
  }

  private java.nio.file.Path setupPseudoFileSystem(java.nio.file.Path fileSystem)
      throws IOException {
    if (Files.exists(fileSystem)) {
      msg("deleting " + fileSystem);
      recursiveDelete(fileSystem.toFile());
    }
    fileSystem = Files.createDirectories(fileSystem);
    if (Files.exists(fileSystem)) {
      msg("created fresh fileSystem (" + fileSystem.toString() + ")");
    } else {
      throw new RuntimeException("Failed to create fileSystem: " + fileSystem.toString());
    }
    return fileSystem;
  }

  private boolean recursiveDelete(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        recursiveDelete(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

  private MiniAccumuloConfig configureMiniCluster(java.nio.file.Path fileSystem,
      final String instance, final String password, final int numTServers, final int zkPort)
      throws IOException {
    MiniAccumuloConfig config = new MiniAccumuloConfig(fileSystem.toFile(), password);
    config.setInstanceName(instance);
    config.setNumTservers(numTServers);
    config.setZooKeeperPort(zkPort);
    return config;
  }

  private void msg(String msg) {
    System.out.println(">>> " + msg);
  }

  private void printTableIdInfo(Connector conn) {
    Map<String,String> idMap = conn.tableOperations().tableIdMap();
    idMap.forEach((k, v) -> {
      //System.out.format("\t %20s : %s", k, v);
      System.out.println("\t" + k + " - " + v);
    });
  }

  void pause() {
    System.console().printf("\n=========================\n");
    System.console().readLine("press enter to continue..\n");
  }

}
