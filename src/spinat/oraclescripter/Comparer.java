package spinat.oraclescripter;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import javax.management.RuntimeErrorException;
import oracle.jdbc.OracleConnection;
import static spinat.oraclescripter.ConnectionUtil.getConnectionAndDesc;
import spinat.oraclescripter.SqlPlus.CodeInfo;
import spinat.oraclescripter.SqlPlus.Snippet;

public class Comparer {

    public static void mainx(String[] args) throws Exception {
        try {
            java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (Exception e) {
            throw new Exception("can not initialize Oracle JDBC\n" + e.toString());
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
            compareRepos(repoDisk, repoDB);
        }
    }

    static void mainFile(String fileName) throws Exception {
        Path realPath = Paths.get(fileName).toAbsolutePath();
        Properties props = Helper.loadProperties(realPath);
        String schemas = Helper.getProp(props, "schemas");
        String connectionDesc = Helper.getProp(props, "connection");
        OracleConnection connection = ConnectionUtil.getConnection(connectionDesc);
        String[] schema_list = schemas.split(",");
        Path tempDir = Files.createTempDirectory("changes");
        Path dbDir = tempDir.resolve("DB");
        Files.createDirectory(dbDir);
        Path diskDir = tempDir.resolve("DISK");
        Files.createDirectory(diskDir);
        for(String schema : schema_list) {
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
            compareRepos(repoDisk, diskDir2, repoDB, dbDir2);
        }
        System.out.printf("changes are in folder: " + tempDir);
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
    
    static void compareRepos(SourceRepo repoDisk, Path dirDisk,
                             SourceRepo repoDB, Path dirDB) throws IOException {
        Set<DBObject> objsDisk = repoDisk.getEntries();
        Set<DBObject> objsDB = repoDB.getEntries();
        for (DBObject dbo : objsDisk) {
            String srcDisk = repoDisk.get(dbo);
            String srcDB = repoDB.get(dbo);
            String fName = dbo.name + "-" + dbo.type + ".txt";
            if (objsDB.contains(dbo)) {
                if (!srcDisk.equals(srcDB)) {
                    System.out.println("different: " + dbo.type + ", " + dbo.name);
                    Helper.writeTextFile(dirDisk.resolve(fName), srcDisk, "UTF-8");
                    Helper.writeTextFile(dirDB.resolve(fName), srcDB,  "UTF-8");
                }
            } else {
                System.out.println("missing in DB: " + dbo.type + ", " + dbo.name);
                Helper.writeTextFile(dirDisk.resolve(fName), srcDisk, "UTF-8");
            }
        }
    }

    static void compareRepos(SourceRepo repoDisk, SourceRepo repoDB) throws Exception {
        Set<DBObject> objsDisk = repoDisk.getEntries();
        Set<DBObject> objsDB = repoDB.getEntries();
        Path tempDir = Files.createTempDirectory("changes");
        Path dbDir = tempDir.resolve("DB");
        Files.createDirectory(dbDir);
        Path diskDir = tempDir.resolve("DISK");
        Files.createDirectory(diskDir);
        compareRepos(repoDisk, diskDir, repoDB, dbDir);
        System.out.println("different files are in folder: " + tempDir);
    }
}
