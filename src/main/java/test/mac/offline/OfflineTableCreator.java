package test.mac.offline;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import test.mac.util.MiniUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OfflineTableCreator {

  private MiniAccumuloCluster mac;

  public OfflineTableCreator(MiniAccumuloCluster mac) {
    this.mac = mac;
  }

  public void createOfflineTable()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    MiniUtils.msg("Start createOffLineTable...");

    String tableName = "ontab";
    Properties props = MiniUtils.getProps();
    Connector conn = Connector.builder().usingProperties(props).build();
    NewTableConfiguration ntc = new NewTableConfiguration();
    MiniUtils.msg("create table " + tableName);
    conn.tableOperations().create(tableName, ntc);
    MiniUtils.printTableIdInfo(conn);

    MiniUtils.pause("should have created ontab");

    tableName = "offtab";
    ntc = new NewTableConfiguration();
    Map<String, String> tprops = new HashMap<>();
    tprops.put("table.create.offline", "true");
    ntc.setProperties(tprops);
    MiniUtils.msg("create table " + tableName);
    conn.tableOperations().create(tableName, ntc);
    MiniUtils.printTableIdInfo(conn);

    MiniUtils.pause("should have created offtab");

    MiniUtils.pause("exiting createOfflineTable");

  }
}
