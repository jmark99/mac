package test.mac;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.accumulo.minicluster.MiniAccumuloConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

public class MiniTester {

    static File bulkDir;
    private static MiniAccumuloConfig config;
    private static MiniAccumuloCluster mac;
    private static Properties props = new Properties();

    public static void main(String[] args) throws Exception {

        props = readClientProperties(args);
        Path fileSystem = setupPseudoFileSystem(Paths.get(props.getProperty("pseudo.file.system")));

        try {
            config = configureMiniCluster(fileSystem, "splitter", "secret", 3, 2181);
            mac = new MiniAccumuloCluster(config);
            mac.start();
            System.out.println("Started mini cluster...");
            printClusterInfo();

            //createWithSplits();
            createRangeFileMapping(10);

        } finally {
            System.out.println("Stopping mini cluster...");
            mac.stop();
        }
    }

    private static Properties readClientProperties(String[] args) throws IOException {
        Properties props = new Properties();
        String propsFile = "/home/mark/conf/accumulo-client.properties";
        FileInputStream in;
        if (args.length == 1)
            in = new FileInputStream(args[0]);
        else
            in = new FileInputStream(propsFile);
        props.load(in);
        in.close();
        return props;
    }

    private static void printClusterInfo() {
        System.out.println("Instance Name:  " + config.getInstanceName());
        System.out.println("Root Pwd:       " + config.getRootPassword());
        System.out.println("Default Memory: " + config.getDefaultMemory());
        System.out.println("ZK Port;        " + config.getZooKeeperPort());
        System.out.println("Dir:            " + config.getDir());
        Map<String,String> siteConfig = config.getSiteConfig();
        System.out.println("Site Config: " );
        siteConfig.forEach((key, value) -> {
            System.out.println("\t---" + key + " - " + value);
        });
        System.out.println("Num TServers:  " + config.getNumTservers());
    }

    private static java.nio.file.Path setupPseudoFileSystem(java.nio.file.Path fileSystem)
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

    private static boolean recursiveDelete(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                recursiveDelete(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    private static MiniAccumuloConfig configureMiniCluster(java.nio.file.Path fileSystem, final String instance,
        final String password, final int numTServers, final int zkPort) throws IOException {
        MiniAccumuloConfig config = new MiniAccumuloConfig(fileSystem.toFile(), password);
        config.setInstanceName(instance);
        config.setNumTservers(numTServers);
        config.setZooKeeperPort(zkPort);
        return config;
    }

    private static void msg(String msg) {
        System.out.println(">>> " + msg);
    }

    /**
     * Work on getting accumulo to create a table with pre-computed splits and tablets
     * spread among the cluster.
     *
     * - Create table
     * -- can an accumulo client connect to this???
     * - get table id
     * - read metadata
     * - take offline
     * - collect necessary netadata
     * - write split info to metadata
     * - bring online and spread across cluster
     */
    private static void createWithSplits()
        throws AccumuloSecurityException, AccumuloException, TableExistsException,
        TableNotFoundException {
        System.out.println("Start createWithSplits...");
        String tableName = "test1";

        //Connector.builder().usingProperties("")
        Connector conn = mac.getConnector("root", "secret");
        conn.tableOperations().create(tableName);
        printTableIdInfo(conn);
        SortedSet<Text> splits = new TreeSet<>();
        for (int i = 10_000; i < 20_000; i = i + 1000)
            splits.add(new Text(rowStr(i)));
        conn.tableOperations().addSplits(tableName, splits);
        //conn.tableOperations().offline(tableName);
        pause();
    }

    private static void printTableIdInfo(Connector conn) {
        Map<String,String> idMap = conn.tableOperations().tableIdMap();
        idMap.forEach((k,v) -> {
            //System.out.format("\t %20s : %s", k, v);
            System.out.println("\t" + k + " - " + v);
        });
    }


    static void pause() {
        System.console().printf("\n=========================\n");
        System.console().readLine("press enter to continue..\n");
    }








    /**
     * Create Range to Rfiles Mapping for rows: row000000 -> row000999
     *
     * @param numOfFiles - number of rfiles containing the rows
     * @return Mapping of Range -> Files
     */
    private static SortedMap<Range,List<File>> createRangeFileMapping(int numOfFiles)
        throws Exception {
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

    private static SortedMap<Key,Value> createTestData(int rows, int families, int qualifiers) {
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

    static String rowStr(int r) {
        return String.format("row%06d", r);
    }

    static String colStr(int c) {
        return String.format("c%04d", c);
    }

    static String rowStrHex(int r) {
        return String.format("%06x", r);
    }

    static String colStrHex(int c) {
        return String.format("%04x", c);
    }
}
