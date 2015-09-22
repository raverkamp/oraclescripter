// -*- Mode: java -*-
package spinat.oraclescripter;

import java.io.*;
import java.sql.*;
import java.util.*;

public abstract class BaseScripter {

    protected Connection connectFromProperties(java.util.Properties p) {
        String dbhost = Helper.getProp(p, "dbhost");
        String dbsid = Helper.getProp(p, "dbsid");
        String dbport = Helper.getProp(p, "dbport", "1521");

        String user = Helper.getProp(p, "user");
        String pw = Helper.getProp(p, "pw");

        String dburl = "jdbc:oracle:thin:" + user + "/" + pw + "@" + dbhost + ":" + dbport + ":" + dbsid;
        System.out.println(dburl);
        try {
            Connection con = DriverManager.getConnection(dburl);
            return con;
        } catch (SQLException e) {
            throw new Error(e);
        }
    }

    public abstract void doit(java.util.Properties p);

    protected String objectWhere(java.util.Properties p) {
        String objs = Helper.getProp(p, "objects", null);
        String obj_where = Helper.getProp(p, "object-where", null);
        String obj_file = Helper.getProp(p, "object-file", null);

        if ((objs == null ? 0 : 1) + (obj_where == null ? 0 : 1)
                + (obj_file == null ? 0 : 1) != 1) {
            throw new Error("either property \"objects\", \"object-where\""
                    + " or \"object-file\" must be given");
        }
        System.out.println("" + objs + obj_where + obj_file);
        String where_clause;
        if (objs != null) {
            ArrayList a = Helper.objectsToArrayList(objs);
            where_clause = "object_name in " + Helper.arrayToInList(a);
        } else if (obj_where != null) {

            where_clause = obj_where;
        } else {
            ArrayList a = Helper.objectsFromFile(obj_file);
            where_clause = "object_name in " + Helper.arrayToInList(a);
        }
        return where_clause;
    }

    String separatedSelect = "select distinct object_name,object_type\n"
            + " from user_objects \n"
            + "where object_type in ('PACKAGE','PACKAGE BODY','PROCEDURE',"
            + " 'FUNCTION','VIEW','TRIGGER','TYPE','TYPE BODY')";

    String combinedSelect = "select distinct object_name,\n"
            + " replace(object_type,' BODY','') as object_type\n"
            + " from user_objects \n"
            + " where object_type in ('PACKAGE','PACKAGE BODY','PROCEDURE',"
            + " 'FUNCTION','VIEW','TRIGGER','TYPE','TYPE BODY')";

    protected ArrayList getObjects(Connection c, String where_clause, boolean specAndBodySplit) {
        try {
            PreparedStatement stmt = c.prepareStatement((specAndBodySplit ? separatedSelect : combinedSelect)
                    + " and " + where_clause);

            ResultSet rs = stmt.executeQuery();
            ArrayList obs = new ArrayList();
            while (rs.next()) {
                String name = rs.getString(1);
                String kind = rs.getString(2);
                if (specAndBodySplit) {
                    if (kind.equals("PACKAGE BODY")) {
                        kind = "PACKAGE-BODY";
                    }
                    if (kind.equals("PACKAGE")) {
                        kind = "PACKAGE-SPEC";
                    }
                    if (kind.equals("TYPE BODY")) {
                        kind = "TYPE-BODY";
                    }
                    if (kind.equals("TYPE")) {
                        kind = "TYPE-SPEC";
                    }
                }
                DBObject o = new DBObject(kind, name);
                obs.add(o);
            }

            rs.close();
            stmt.close();
            return obs;
        } catch (SQLException e) {
            throw new Error(e);
        }
    }

}
