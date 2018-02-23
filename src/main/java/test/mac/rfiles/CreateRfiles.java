package test.mac.rfiles;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class CreateRfiles {


  File bulkDir;
  /**
   * Create Range to Rfiles Mapping for rows: row000000 -> row000999
   *
   * @param numOfFiles - number of rfiles containing the rows
   * @return Mapping of Range -> Files
   */
  public SortedMap<Range,List<File>> createRfiles(int numOfFiles) throws IOException {
    //FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:8020"), new Configuration());
    SortedMap<Range,List<File>> mapping = new TreeMap<>();
    List<RFileWriter> writers = new ArrayList<>();
    List<File> rFiles = new ArrayList<>();

    FileSystem fs = FileSystem.getLocal(new Configuration());
    bulkDir = new File(System.getProperty("user.dir") + "/target/rfiles");
    System.out.println("bulkDir: " + bulkDir.toString());
    bulkDir.mkdirs();

    for (int i = 0; i < numOfFiles; i++) {
      File rFile = new File(bulkDir, "test" + i + ".rf");
      RFileWriter writer = RFile.newWriter().to(rFile.getPath()).withFileSystem(fs).build();
      writers.add(writer);
      rFiles.add(rFile);
    }

    //write the rows to the rfiles
    Set<Map.Entry<Key,Value>> data = createTestData(1000, 1, 1).entrySet();
    int rowsPerFile = 1000 / numOfFiles;
    int count = 0;
    int fileIndex = 0;
    //System.out.println("Looping through " + data.size() + " rows and putting " + rowsPerFile + " per file.");
    for (Map.Entry<Key,Value> e : data) {
      if (count < rowsPerFile) {
        writers.get(fileIndex).append(e.getKey(), e.getValue());
        count++;
      } else {
        //System.out.println("Close writer for  " + fileIndex);
        writers.get(fileIndex).close();
        fileIndex++;
        count = 0;
      }
    }
    //System.out.println("Close writer for  " + fileIndex);
    writers.get(fileIndex).close();

    // verify the start and end rows
    Map.Entry<Key,Value> outerStart = null;
    Map.Entry<Key,Value> outerEnd = null;
    for (File r : rFiles) {
      Scanner rscanner = RFile.newScanner().from(r.getPath()).withFileSystem(fs).build();
      //System.out.println("Scanning rfile... " + r.getName());
      Map.Entry<Key,Value> start = null;
      Map.Entry<Key,Value> end = null;
      for (Map.Entry<Key,Value> e : rscanner) {
        if (start == null)
          start = e;
        end = e;
      }
      System.out.println("First Row: " + start.getKey() + " " + start.getValue());
      System.out.println(" Last Row: " + end.getKey() + " " + end.getValue());
      if (outerStart == null)
        outerStart = start;
      outerEnd = end;
    }
    mapping.put(new Range(new Key("row000000"), new Key("row000999")), rFiles);
    //mapping.put(new Range(outerStart.getKey(), outerEnd.getKey()), rFiles);
    return mapping;
  }

  private SortedMap<Key,Value> createTestData(int rows, int families, int qualifiers) {
    TreeMap<Key,Value> testData = new TreeMap<>();
    int startRow = 0;
    int startFamily = 0;

    for (int r = 0; r < rows; r++) {
      String row = rowStr(r + startRow);
      for (int f = 0; f < families; f++) {
        String fam = colStr(f + startFamily);
        for (int q = 0; q < qualifiers; q++) {
          String qual = colStr(q);
          Key k = new Key(row, fam, qual);
          testData.put(k, new Value("val" + r));
        }
      }
    }

    return testData;
  }

  String rowStr(int r) {
    return String.format("row%06d", r);
  }

  String colStr(int c) {
    return String.format("c%04d", c);
  }

  String rowStrHex(int r) {
    return String.format("%06x", r);
  }

  String colStrHex(int c) {
    return String.format("%04x", c);
  }

}
