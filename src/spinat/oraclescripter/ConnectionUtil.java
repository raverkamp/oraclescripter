package spinat.oraclescripter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import oracle.jdbc.OracleConnection;

final class ConnectionUtil {

    // only static methods, do not create an obejct not even in by a static method
    private ConnectionUtil() {
        throw new Error();
    }

    public static class ConnectionAndDesc {

        public final OracleConnection connection;
        public final spinat.oraclelogin.OraConnectionDesc connectionDesc;

        public ConnectionAndDesc(OracleConnection connection, spinat.oraclelogin.OraConnectionDesc connectionDesc) {
            this.connection = connection;
            this.connectionDesc = connectionDesc;
        }

    }

    static ConnectionAndDesc getConnectionAndDesc(String desc) throws ParseException {
        spinat.oraclelogin.OraConnectionDesc cd = spinat.oraclelogin.OraConnectionDesc.fromString(desc);
        if (!cd.hasPwd()) {
            if (System.console() == null) {
                Helper.abort("No password was given and there is no input console to enter it.");
            }
            char[] pw = System.console().readPassword("Password for " + cd.display() + ":");
            cd.setPwd(new String(pw));
        }
        try {
            return new ConnectionAndDesc(cd.getConnection(), cd);
        } catch (SQLException e) {
            Helper.abort("cannot get connection described by " + desc
                    + "\n" + e.toString());
            // abort aborts, so never reached
            throw new Error("");
        }
    }

    static OracleConnection getConnection(String desc) throws ParseException {
        return getConnectionAndDesc(desc).connection;
    }

    static boolean hasDBAViews(Connection c) throws SQLException {
        // if we have access to dba_objects we assume that we have access to the rest as well
        // we get an exception if we do not have access
        //  but we check that the exception has nothing to do with the connection
        try (PreparedStatement stm = c.prepareStatement("select 1 as a from dba_objects where 1=2");
                ResultSet rs = stm.executeQuery()) {
            return true;
        } catch (SQLException ex) {

            if (c.isValid(5)) {
                return false;
            } else {
                throw ex;
            }
        }
    }

    static boolean userExists(Connection c, String owner) throws SQLException {
        try (PreparedStatement stm = c.prepareStatement("select username from dba_users where username=?")) {
            stm.setString(1, owner);
            try (ResultSet rs = stm.executeQuery()) {
                if (rs.next() && rs.getString(1).equals(owner)) {
                    return true;
                } else {
                    return false;
                }
            }
        }
    }

    public static String connectionUser(OracleConnection c) throws SQLException {
        //java.lang.AbstractMethodError, heh?
        // return c.getSchema();
        return c.getUserName();
    }

    public static class ObjectCond {

        public String objs = null;
        public String obj_where = null;
        public String obj_file = null;
    }

    // return the list of objects to script, this list is sorted!
    public static ArrayList<DBObject> getDBObjects(
            Connection c,
            String owner,
            boolean useDBAViews,
            ObjectCond cond,
            boolean includeTables)
            throws SQLException {
        String objs = cond.objs;
        String obj_where = cond.obj_where;
        String obj_file = cond.obj_file;

        if ((objs == null ? 0 : 1) + (obj_where == null ? 0 : 1)
                + (obj_file == null ? 0 : 1) != 1) {
            throw new RuntimeException("either property \"objects\", \"object-where\""
                    + " or \"object-file\" must be given");
        }
        String where_clause;
        if (objs != null) {
            ArrayList<String> a = Helper.objectsToArrayList(objs);
            where_clause = "object_name in " + Helper.arrayToInList(a);
        } else if (obj_where != null) {
            where_clause = obj_where;
        } else {
            ArrayList<String> a = Helper.objectsFromFile(obj_file);
            where_clause = "object_name in " + Helper.arrayToInList(a);
        }
        String objectView = useDBAViews ? "dba_objects" : "all_objects";
        String typeView = useDBAViews ? "dba_types" : "all_types";
        final String tabs = includeTables ? ", 'TABLE'" : "";

        try (PreparedStatement stm = c.prepareStatement("select distinct object_name,object_type\n"
                + "from " + objectView + "\n"
                + "where object_type in ('PACKAGE','PROCEDURE',\n"
                + "'FUNCTION','VIEW','TRIGGER','TYPE', 'JAVA SOURCE'" + tabs + ")\n"
                + " and owner = ? "
                + " and (not (object_type = 'TABLE' and object_name like '%==')) "
                // get rid of types generated by PL/SQL for pipelined table functions
                + " and ( object_type <>'TYPE' or object_name in (select type_name from " + typeView + " where owner=?)) "
                + " and (" + where_clause + " ) "
                // triggers are last, views are next to last, this is important
                // for running the script with sqlplus, you can only create an
                // instead of trigger on a viwe if the view exists
                + " order by case object_type when 'TRIGGER' then 'Z' else object_type end,object_name ")) {
            stm.setString(1, owner);
            stm.setString(2, owner);
            try (ResultSet rs = stm.executeQuery()) {
                ArrayList<DBObject> res = new ArrayList<>();
                while (rs.next()) {
                    String name = rs.getString(1);
                    String type = rs.getString(2);
                    res.add(new DBObject(type, name));
                }
                return res;
            }
        }
    }

    public static ArrayList<DBObject> getDBObjects(
            Connection c,
            String owner,
            boolean useDBAViews,
            ObjectCond cond)
            throws SQLException {
        return getDBObjects(c, owner, useDBAViews, cond, false);
    }

}
