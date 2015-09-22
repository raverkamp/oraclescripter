package spinat.oraclescripter;

import java.sql.*;
import java.util.*;

public class PFScripter extends BaseScripter {

    public void doit(java.util.Properties p) {
        try {
            Connection con = connectFromProperties(p);

            String where_clause = objectWhere(p);

            String directory = Helper.getProp(p, "directory");

            boolean unixLineEndStyle = Helper.getPropBool(p, "unix-lineend", false);
            boolean useDBMS_METADATA = Helper.getPropBool(p, "use-dbms-metadata", false);

            IObjectWorker wo = new PFWorker(directory, unixLineEndStyle);

            ArrayList obs = getObjects(con, where_clause, true);

            IObjectScripter sc = new Scripter2(con, true, useDBMS_METADATA);
            sc.scriptCode(obs, wo);
            con.close();
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
