package test.mac.util;

import jdk.nashorn.internal.ir.RuntimeNode;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MiniUtils {

  private static MiniAccumuloCluster mac = null;
  private static MiniAccumuloConfig config = null;
  private static Properties props = null;

  public static MiniAccumuloCluster getMac() {
    if (mac == null) {
      throw new RuntimeException("Cluster not initialized and running...");
    }
    return mac;
  }

  public static MiniAccumuloConfig getConfig() {
    if (config == null) {
      throw new RuntimeException("config values not initialized...");
    }
    return config;
  }

  public static Properties getProps() {
    if (props == null) {
      throw new RuntimeException("Properties not read in...");
    }
    return props;
  }

  public static void printClusterInfo(final MiniAccumuloConfig config) {
    msg("Instance Name:  " + config.getInstanceName());
    msg("Root Pwd:       " + config.getRootPassword());
    msg("Default Memory: " + config.getDefaultMemory());
    msg("ZK Port;        " + config.getZooKeeperPort());
    msg("FS Dir:         " + config.getDir());
    Map<String,String> siteConfig = config.getSiteConfig();
    msg("Num TServers:  " + config.getNumTservers());
    if (siteConfig.size() > 0) {
      msg("Site Config: ");
      siteConfig.forEach((key, value) -> {
        msg("\t---" + key + " - " + value);
      });
    }
  }

  public static void startMiniCluster() throws IOException, InterruptedException {
    props = readClientProperties();
    Path fileSystem = setupPseudoFileSystem(Paths.get(props.getProperty("pseudo.file.system")));
    config = configureMiniCluster(fileSystem, props.getProperty("instance.name"),
        props.getProperty("auth.password"),
        Integer.parseInt(props.getProperty("num" + ".tservers")),
        Integer.parseInt(props.getProperty("zk.port")));
    mac = new MiniAccumuloCluster(config);
    mac.start();
    MiniUtils.msg("Started mini cluster...");
    MiniUtils.printClusterInfo(config);
  }

  public static void stopMiniCluster() throws IOException, InterruptedException {
    mac.stop();
    msg("Stopped minicluster...");
  }

  public static Properties readClientProperties() throws IOException {
    Properties props = new Properties();
    FileInputStream in;
    in = new FileInputStream(System.getProperty("user.dir") + "/mac.properties");
    props.load(in);
    in.close();
    return props;
  }

  private static java.nio.file.Path setupPseudoFileSystem(java.nio.file.Path fileSystem)
      throws IOException {
    if (Files.exists(fileSystem)) {
      MiniUtils.msg("deleting " + fileSystem);
      MiniUtils.recursiveDelete(fileSystem.toFile());
    }
    fileSystem = Files.createDirectories(fileSystem);
    if (Files.exists(fileSystem)) {
      MiniUtils.msg("created fresh fileSystem (" + fileSystem.toString() + ")");
    } else {
      throw new RuntimeException("Failed to create fileSystem: " + fileSystem.toString());
    }
    return fileSystem;
  }

  private static MiniAccumuloConfig configureMiniCluster(java.nio.file.Path fileSystem,
      final String instance, final String password, final int numTServers, final int zkPort) {
    MiniAccumuloConfig config = new MiniAccumuloConfig(fileSystem.toFile(), password);
    config.setInstanceName(instance);
    config.setNumTservers(numTServers);
    config.setZooKeeperPort(zkPort);
    return config;
  }
  public static void msg(String msg) {
    System.out.println(">>> " + msg);
  }

  public static void msg(String msg, boolean pause) {
    System.out.println(">>> " + msg);
    if (pause)
      System.console().readLine("press enter to continue..\n");
  }

  public static void pause() {
    System.console().printf("\n=========================\n");
    System.console().readLine("press enter to continue..\n");
  }

  public static void pause(final String msg) {
    System.console().printf("\n=========================\n");
    System.out.println(msg);
    System.console().readLine("press enter to continue..\n");
  }

  public static void printTableIdInfo(Connector conn) {
    Map<String,String> idMap = conn.tableOperations().tableIdMap();
    idMap.forEach((k, v) -> {
      //System.out.format("\t %20s : %s", k, v);
      System.out.println("\t" + k + " - " + v);
    });
  }

  public static Map<Key,Value> readMetadataFromTableId(final Connector conn, final String tableId) {
    Map<Key, Value> tableDataMap = new HashMap<>();

    MiniUtils.msg("Start readMetadataFromTableId...");
    try (Scanner scan = conn.createScanner("accumulo.metadata", Authorizations.EMPTY)) {
      Text row = new Text(tableId + "<");
      scan.setRange(new Range(row));
      for (Map.Entry<Key,Value> entry : scan) {
        tableDataMap.put(entry.getKey(), entry.getValue());
      }
    } catch (TableNotFoundException e) {
      MiniUtils.msg("Failed to read table");
      e.printStackTrace();
    }
    return tableDataMap;
  }

  public static String getFutureAndLocationCount(final Connector conn, final String tableId) {
    int fcnt  = 0;
    int lcnt  = 0;
    try (Scanner scan = conn.createScanner("accumulo.metadata", Authorizations.EMPTY)) {
      Text row = new Text(tableId + "<");
      scan.setRange(new Range(row));
      for (Map.Entry<Key,Value> entry : scan) {
        String colf = entry.getKey().getColumnFamily().toString();
        if (colf.contains("future")) {
          fcnt++;
        } else if (colf.contains("loc")) {
          lcnt++;
        }
      }
    } catch (TableNotFoundException e) {
      MiniUtils.msg("Failed to read table");
      e.printStackTrace();
    }
    return String.valueOf(fcnt) + ":" + String.valueOf(lcnt);
  }

  public static void sleep(final int millis) {
    try {Thread.sleep(millis); } catch (InterruptedException e) {;}
  }

  public static void printMetaData(Map<Key, Value> metadata) {
    MiniUtils.msg("Start printMetaData...");
    for (Map.Entry<Key, Value> entry : metadata.entrySet()) {
      MiniUtils.msg("rowID:    " + entry.getKey().getRow().toString());
      MiniUtils.msg("Fam:      " + entry.getKey().getColumnFamily().toString());
      MiniUtils.msg("Qual:     " + entry.getKey().getColumnQualifier().toString());
      MiniUtils.msg("Vis:      " + entry.getKey().getColumnVisibility().toString());
      MiniUtils.msg("Value:    " + entry.getValue().toString());
    }
  }

  public static boolean recursiveDelete(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        recursiveDelete(file);
      }
    }
    return directoryToBeDeleted.delete();
  }
}
