package spinat.oraclescripter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Set;
import oracle.jdbc.OracleConnection;
import spinat.oraclescripter.SqlPlus.CodeInfo;
import spinat.oraclescripter.SqlPlus.Snippet;

public class Comparer {

    public static void mainx(String[] args) throws Exception {
        try {
            java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (Exception e) {
            throw new Exception("can not initialize Oracle JDBC\n" + e.toString());
        }

        String fileName = args[1];
        String connectDesc = args[2];
        Path filePath = Paths.get(fileName).toAbsolutePath();
        // use the current diretory, this is not well defefinde in Java
        Path baseDir = Paths.get(".").toAbsolutePath();
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

    static void compareRepos(SourceRepo repoDisk, SourceRepo repoDB) {
        Set<DBObject> objsDisk = repoDisk.getEntries();
        Set<DBObject> objsDB = repoDB.getEntries();
        for (DBObject dbo : objsDisk) {
            String srcDisk = repoDisk.get(dbo);
            String srcDB = repoDB.get(dbo);
            if (objsDB.contains(dbo)) {
                if (!srcDisk.equals(srcDB)) {
                    System.out.println("different: " + dbo.type + ", " + dbo.name);
                }
            } else {
                System.out.println("missing in DB: " + dbo.type + ", " + dbo.name);
            }
        }
    }

}
