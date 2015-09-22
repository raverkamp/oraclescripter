package spinat.oraclescripter;

import java.io.*;
import java.sql.*;
import java.util.*;

public class PLSQLDEVScripter extends BaseScripter {

    public void doit(java.util.Properties p) {
        try {
            Connection con = connectFromProperties(p);

            String where_clause = objectWhere(p);

            String directory = Helper.getProp(p, "directory");
            boolean unixLineEndStyle = Helper.getPropBool(p, "unix-lineend", false);
            ArrayList obs = getObjects(con, where_clause, true);
            IObjectWorker wo = new PLSQLDEVWorker(directory, unixLineEndStyle);

            IObjectScripter sc = new Scripter2(con, true,
                    Helper.getPropBool(p, "use-dbms-metadata", false));
            sc.scriptCode(obs, wo);
            con.close();
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
