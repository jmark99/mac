package test.mac.util;

import jdk.nashorn.internal.ir.RuntimeNode;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

  private static Properties readClientProperties() throws IOException {
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
