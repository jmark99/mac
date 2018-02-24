package test.mac.offline;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import test.mac.util.MiniUtils;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

public class OfflineTester {

  private MiniAccumuloCluster mac;

  public OfflineTester(MiniAccumuloCluster mac) {
    this.mac = mac;
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
  public void createTableWithMetadata()
      throws AccumuloSecurityException, AccumuloException, TableExistsException,
      TableNotFoundException {

    MiniUtils.msg("Start createWithSplits...");
    String tableName = "test1";

    Properties props = MiniUtils.getProps();
    Connector conn = Connector.builder().usingProperties(props).build();
    MiniUtils.msg("create table " + tableName);
    conn.tableOperations().create(tableName);
    MiniUtils.printTableIdInfo(conn);
    MiniUtils.msg("take table offline...");
    conn.tableOperations().offline(tableName);
    MiniUtils.msg("get table Id, i.e., name...");
    String tableId = getTableId(conn, tableName);
    MiniUtils.msg("Table ID: " + tableId);
    MiniUtils.msg("readMetaData given Table ID");
    long start = System.currentTimeMillis();
    Map<Key,Value> metadata = readMetadataFromTableId(conn, tableId);
    long end = System.currentTimeMillis();
    MiniUtils.msg("reading metadata: " + (end - start)/1000.0);
    printMetaData(metadata);

    SortedSet<Text> splits = new TreeSet<>();
    splits.add(new Text("row01"));
    splits.add(new Text("row02"));

    MiniUtils.msg("write split data to metadata table...");
    writeSplitsToMetadataTable(conn, tableName, tableId, metadata, splits);

//    metadata.clear();
//    MiniUtils.msg("reread metadata table...");
//    start = System.currentTimeMillis();
//    metadata = readMetadataFromTableId(conn, tableId);
//    end = System.currentTimeMillis();
//    printMetaData(metadata);

    //MiniUtils.msg("bring table online...");
    //conn.tableOperations().online(tableName);
    MiniUtils.pause();
  }

  private void writeSplitsToMetadataTable(Connector conn, String tableName, String
      tableId, Map<Key,Value> metadata, SortedSet<Text> splits) throws TableNotFoundException,
      MutationsRejectedException {

    // get batch writer to metadata table
    BatchWriter writer = conn.createBatchWriter("accumulo.metadata", new BatchWriterConfig());

    // Get info from metadata Set
    String lock = null;
    String prevrow = null;
    String dir = null;
    String time = null;
    for (Map.Entry<Key, Value> map : metadata.entrySet()) {
      if (map.getKey().getColumnQualifier().toString().equals("lock")) {
        String updated = map.getValue().toString().replace("masters", "tservers");
        lock = updated;
      }
      if (map.getKey().getColumnQualifier().toString().equals("~pr")) {
        prevrow = map.getValue().toString();
      }
      if (map.getKey().getColumnQualifier().toString().equals("dir")) {
        dir = map.getValue().toString();
      }
      if (map.getKey().getColumnQualifier().toString().equals("time")) {
        time = map.getValue().toString();
      }
    }
    MiniUtils.msg("lock: " + lock);
    MiniUtils.msg("pr:   " + prevrow);
    MiniUtils.msg("dir:  " + dir);
    MiniUtils.msg("time: " + time);

    // for each split
    //   write new entry with tableId;rowname and exsiting metadata
    Iterator<Text> itr = splits.iterator();
    String rowId = "";
    String lastRow = "";
    String prev = "";

    final Charset CHARSET = ISO_8859_1;
    boolean first = true;
    while(itr.hasNext()) {
      // create new row name
      lastRow = itr.next().toString();
      rowId = tableId + ";" + lastRow;
      Mutation mutation = new Mutation(new Text(rowId.getBytes(CHARSET)));

      MiniUtils.msg("update - ");

      // write 'lock'
      Text colf = new Text("srv".getBytes(CHARSET));
      Text colq = new Text("lock".getBytes(CHARSET));
      Value val = new Value(lock.getBytes(CHARSET));
      mutation.put(colf, colq, val);
      //writer.addMutation(mutation);
      MiniUtils.msg(rowId + " " + colf.toString() + ":" + colq.toString() + " - " + val.toString());

      // write 'dir'
      colq = new Text("dir".getBytes(CHARSET));
      val = new Value(dir.getBytes(CHARSET));
      mutation.put(colf, colq, val);
      //writer.addMutation(mutation);
      MiniUtils.msg(rowId + " " +  colf.toString() + ":" + colq.toString() + " - " + val.toString());

      // write 'time'
      colq = new Text("time".getBytes(CHARSET));
      val = new Value(time.getBytes(CHARSET));
      //mutation.put(colf, colq, val);
      writer.addMutation(mutation);
      MiniUtils.msg(rowId + " " +  colf.toString() + ":" + colq.toString() + " - " + val.toString());

      colf = new Text("~tab".getBytes(CHARSET));
      colq = new Text("~pr".getBytes(CHARSET));
      if (first) {
        val = new Value(prevrow.getBytes(CHARSET));
        first = false;
      } else {
        val = new Value(prev.getBytes(CHARSET));
      }
      //mutation.put(colf, colq, val);
      writer.addMutation(mutation);
      MiniUtils.msg(rowId + " " +  colf.toString() + ":" + colq.toString() + " - " + val.toString());

      prev = lastRow;
    }

    rowId = tableId + "<";

    Mutation mutation = new Mutation(new Text(rowId.getBytes(CHARSET)));

    Text colf = new Text("srv".getBytes(CHARSET));
    Text colq = new Text("lock".getBytes(CHARSET));
    Value val = new Value(lock.getBytes(CHARSET));
    mutation.put(colf, colq, val);
    //writer.addMutation(mutation);
    MiniUtils.msg(rowId + " " +  colf.toString() + ":" + colq.toString() + " - " + val.toString());

    colq = new Text("dir".getBytes(CHARSET));
    val = new Value(dir.getBytes(CHARSET));
    mutation.put(colf, colq, val);
    //writer.addMutation(mutation);
    MiniUtils.msg(rowId + " " +  colf.toString() + ":" + colq.toString() + " - " + val.toString());

    colq = new Text("time".getBytes(CHARSET));
    val = new Value(time.getBytes(CHARSET));
    mutation.put(colf, colq, val);
    //writer.addMutation(mutation);
    MiniUtils.msg(rowId + " " +  colf.toString() + ":" + colq.toString() + " - " + val.toString());

    colf = new Text("~tab".getBytes(CHARSET));
    colq = new Text("~pr".getBytes(CHARSET));
    val = new Value(lastRow.getBytes(CHARSET));
    mutation.put(colf, colq, val);
    writer.addMutation(mutation);
    MiniUtils.msg(rowId + " " +  colf.toString() + ":" + colq.toString() + " - " + val.toString());

    writer.close();
  }

  private void printMetaData(Map<Key, Value> metadata) {
    for (Map.Entry<Key, Value> entry : metadata.entrySet()) {
      MiniUtils.msg("rowID:    " + entry.getKey().getRow().toString());
      MiniUtils.msg("Fam:      " + entry.getKey().getColumnFamily().toString());
      MiniUtils.msg("Qual:     " + entry.getKey().getColumnQualifier().toString());
      MiniUtils.msg("Vis:      " + entry.getKey().getColumnVisibility().toString());
      MiniUtils.msg("Value:    " + entry.getValue().toString());
    }
  }

  private Map<Key, Value> readMetadataFromTableId(final Connector conn, final String tableId) {
    Map<Key, Value> tableDataMap = new HashMap<>();
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

  private String getTableId(final Connector conn, final String tableName) {
    Map<String,String> tableIdMap = conn.tableOperations().tableIdMap();
    for (Map.Entry<String, String> entry : tableIdMap.entrySet()) {
      if (entry.getKey().equals(tableName)) {
        return entry.getValue();
      }
    }
    return "";
  }

  private void parseMetaDataRow(final Key key, final Value value) {
    Text row = key.getRow();
    String family = key.getColumnFamily().toString();
    String qualifier = key.getColumnQualifier().toString();
    System.out.print(row);
    System.out.print("\t" + family.toString());
    System.out.print(" : " + qualifier.toString());
    System.out.println(" = " + value.toString());
  }
}
