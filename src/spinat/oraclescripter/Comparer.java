package spinat.oraclescripter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import oracle.jdbc.OracleConnection;
import spinat.oraclescripter.SqlPlus.CodeInfo;
import spinat.oraclescripter.SqlPlus.Snippet;

public class Comparer {

    private static String padRight(String s, int n, char c) {
        if (s.length() >= n) {
            return s;
        }
        StringBuilder b = new StringBuilder(s);
        for (int i = 0; i < n - s.length(); i++) {
            b.append(c);
        }
        return b.toString();
    }

    private static String padLeft(String s, int n, char c) {
        if (s.length() >= n) {
            return s;
        }
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < n - s.length(); i++) {
            b.append(c);
        }
        b.append(s);
        return b.toString();
    }
    
    // main entry point for doing compare
    public static void mainx(String[] args) throws Exception {
        try {
            java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (SQLException e) {
            throw new RuntimeException("can not initialize Oracle JDBC", e);
        }
        if (!args[0].equalsIgnoreCase("compare")) {
            throw new RuntimeException("first argument must be 'compare'");
        }
        if (args.length == 2) {
            mainFile(args[1]);
        } else {

            String fileName = args[1];
            String connectDesc = args[2];
            final Path baseDir;
            if (args.length >= 4) {
                baseDir = Paths.get(args[3]).toRealPath().toAbsolutePath();
            } else {
                baseDir = Paths.get(".").toRealPath().toAbsolutePath();
            }
            Path filePath = Paths.get(fileName).toAbsolutePath();
            // use the current diretory, this is not well defefinde in Java

            SourceRepo repoDisk = loadSource(filePath, baseDir);

            OracleConnection c = ConnectionUtil.getConnection(connectDesc);

            String schema = ConnectionUtil.connectionUser(c);
            ConnectionUtil.ObjectCond conds = new ConnectionUtil.ObjectCond();
            conds.obj_where = "1=1";
            ArrayList<DBObject> dbObjects = ConnectionUtil.getDBObjects(c, schema, false, conds);
            SourceCodeGetter sc = new SourceCodeGetter(c, schema, false);
            sc.load(dbObjects);
            SourceRepo repoDB = sc.getSourceRepo();
            Path cmpDir = compareRepos(repoDisk, repoDB);
            System.out.println("dfferences ar in folder: " + cmpDir.toString());
        }
    }

    // invocatiobn with a properties file
    // the required properties:
    //   schemas: comma separated list of schemas
    //   connection: a connection descriptor string
    // per schema there must be a property 
    //   <SCHEMA>.start which names the Sql Plus file to start which loads the objects into
    //   the database
    static void mainFile(String fileName) throws Exception {
        Path realPath = Paths.get(fileName).toAbsolutePath();
        Properties props = Helper.loadProperties(realPath);
        String schemas = Helper.getProp(props, "schemas");
        String connectionDesc = Helper.getProp(props, "connection");
        OracleConnection connection = ConnectionUtil.getConnection(connectionDesc);
        String[] schema_list = schemas.split(",");
        // create the temporary directories for comparision
        Path tempDir = Files.createTempDirectory("changes");
        Path dbDir = tempDir.resolve("DB");
        Files.createDirectory(dbDir);
        Path diskDir = tempDir.resolve("DISK");
        Files.createDirectory(diskDir);
        for (String schema : schema_list) {
            String owner = schema.toUpperCase(Locale.ROOT);
            ConnectionUtil.ObjectCond conds = new ConnectionUtil.ObjectCond();
            conds.obj_where = "1=1";
            ArrayList<DBObject> dbObjects = ConnectionUtil.getDBObjects(connection, owner, true, conds);
            SourceCodeGetter sc = new SourceCodeGetter(connection, owner, true);
            sc.load(dbObjects);
            SourceRepo repoDB = sc.getSourceRepo();
            String startFile = Helper.getProp(props, schema + ".start");
            Path startPath = realPath.getParent().resolve(startFile).toAbsolutePath();
            Path baseDir = startPath.getParent();
            SourceRepo repoDisk = loadSource(startPath, baseDir);
            Path dbDir2 = Files.createDirectory(dbDir.resolve(owner));
            Path diskDir2 = Files.createDirectory(diskDir.resolve(owner));
            compareRepos(schema, repoDisk, diskDir2, repoDB, dbDir2);
        }
        System.out.println("--- done ---");
        System.out.printf("changes are in folder: " + tempDir);
        if (Helper.getProp(props, "usewinmerge", "N").equalsIgnoreCase("Y")) {
            Runtime rt = Runtime.getRuntime();
            Process pr = rt.exec(new String[]{"C:\\Program Files (x86)\\WinMerge\\WinMergeU.exe",
                //"/r",
                "/wl", "/wr", "/u", "/dl", "DB", "/dr", "DISK",
                dbDir.toString(),
                diskDir.toString()
            });
            pr.waitFor();
        }
    }

    static SourceRepo loadSource(Path filePath, Path baseDir) throws Exception {
        SqlPlus session = new SqlPlus(filePath, baseDir);
        ArrayList<Snippet> l = session.process();
        SourceRepo repo = new SourceRepo();
        for (Snippet sn : l) {
            if (sn.what.equals("code")) {
                CodeInfo ci = SqlPlus.analyzeCode(sn.text);
                DBObject dbo = new DBObject(ci.what, ci.name);
                if (repo.exists(dbo)) {
                    System.err.println("warning, object already in repo:" + dbo);
                }
                repo.add(dbo, ci.text);
            }
        }
        return repo;
    }

    static void compareRepos(String schema, SourceRepo repoDisk, Path dirDisk,
            SourceRepo repoDB, Path dirDB) throws IOException {
        Set<DBObject> objsDisk = repoDisk.getEntries(); // sort !
        Set<DBObject> objsDB = repoDB.getEntries();
        Set<DBObject> all = new HashSet<>();
        all.addAll(objsDB);
        all.addAll(objsDisk);
        DBObject[] allArray = all.toArray(new DBObject[0]);
        String encoding = System.getProperty("file.encoding");
        java.util.Arrays.sort(allArray, new Comparator<DBObject>() {
            @Override
            public int compare(DBObject o1, DBObject o2) {
                int x = o1.type.compareTo(o2.type);
                if (x == 0) {
                    return o1.name.compareTo(o2.name);
                } else {
                    return x;
                }
            }
        });
        for (DBObject dbo : allArray) {
            String fName = dbo.name + "-" + dbo.type + ".txt";
            if (objsDB.contains(dbo)) {
                String srcDB = repoDB.get(dbo);
                if (objsDisk.contains(dbo)) {
                    String srcDisk = repoDisk.get(dbo);
                    if (!srcDisk.equals(srcDB)) {
                        System.out.println(padLeft("different: ", 20, ' ') + padRight(schema, 30, ' ')
                                + " " + padRight(dbo.type, 20, ' ') + " " + dbo.name);
                        Helper.writeTextFilePlatformLineEnd(dirDisk.resolve(fName), srcDisk, encoding);
                        Helper.writeTextFilePlatformLineEnd(dirDB.resolve(fName), srcDB, encoding);
                    }
                } else {
                    System.out.println(padLeft("missing on Disk: ", 20, ' ') + padRight(schema, 30, ' ')
                            + " " + padRight(dbo.type, 20, ' ') + " " + dbo.name);
                    Helper.writeTextFilePlatformLineEnd(dirDisk.resolve(fName), srcDB, encoding);
                }
            } else {
                System.out.println(padLeft("missing in DB: ", 20, ' ') + padRight(schema, 30, ' ')
                        + " " + padRight(dbo.type, 20, ' ') + ", " + dbo.name);
                String srcDisk = repoDisk.get(dbo);
                Helper.writeTextFilePlatformLineEnd(dirDisk.resolve(fName), srcDisk, encoding);
            }
        }
    }

    static Path compareRepos(SourceRepo repoDisk, SourceRepo repoDB) throws Exception {
        Path tempDir = Files.createTempDirectory("changes");
        Path dbDir = tempDir.resolve("DB");
        Files.createDirectory(dbDir);
        Path diskDir = tempDir.resolve("DISK");
        Files.createDirectory(diskDir);
        compareRepos("", repoDisk, diskDir, repoDB, dbDir);
        return tempDir;
    }
}
