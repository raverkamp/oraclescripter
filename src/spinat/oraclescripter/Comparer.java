package spinat.oraclescripter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import oracle.jdbc.OracleConnection;
import spinat.oraclescripter.SqlPlus.CodeInfo;
import spinat.oraclescripter.SqlPlus.Snippet;

public class Comparer {
  
    public static void mainx(String[] args) throws Exception {
        String fileName = args[0];
        String connectDesc = args[1];
        Path filePath = Paths.get(fileName).toAbsolutePath();
        // use the current diretory, this is not well defefinde in Java
        Path baseDir = Paths.get(".").toAbsolutePath();
        SourceRepo repo = loadSource(filePath, baseDir);
        
        OracleConnection c = ConnectionUtil.getConnection(connectDesc);
        
        String schema = ConnectionUtil.connectionUser(c);
        ConnectionUtil.ObjectCond conds = new ConnectionUtil.ObjectCond();
        conds.obj_where="1=1";
        ArrayList<DBObject> dbObjects = ConnectionUtil.getDBObjects(c, schema, true, conds);
        SourceCodeGetter sc = new SourceCodeGetter(c, schema, false);
        sc.load(dbObjects);
        
    }
    
    static SourceRepo loadSource(Path  filePath, Path baseDir) throws Exception {
         SqlPlus session = new SqlPlus(filePath, baseDir);
         ArrayList<Snippet> l = session.process();
         SourceRepo repo = new SourceRepo();
        for(Snippet sn : l) {
            if (sn.what.equals("code")) {
                CodeInfo ci = SqlPlus.analyzeCode(sn.text);
                DBObject dbo = new DBObject(ci.what,ci.name);
                if (repo.exists(dbo)) {
                    System.err.println("warning, object already in repo:" + dbo);
                }
                repo.add(dbo, ci.text);
            }
        }
        return repo;
    }

}
