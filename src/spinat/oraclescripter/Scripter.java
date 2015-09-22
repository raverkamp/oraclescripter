package spinat.oraclescripter;

import java.sql.*;

public class Scripter /*implements IObjectScripter*/ {

    private Connection _connection;

    private boolean _withSlash;

    public Scripter(Connection c, boolean withSlash) {
        this._connection = c;
        this._withSlash = withSlash;
    }

    public Scripter(Connection c) {
        this(c, true);
    }

    public void scriptCode(java.util.ArrayList os,
            IObjectWorker w) {
        int size = os.size();
        for (int j = 0; j < size; j++) {
            DBObject o = (DBObject) os.get(j);
            w.work(o, getCode(o));
        }

    }

    ;


    /*DBMS_METADATA.GET_DDL (
object_type     IN VARCHAR2,
name            IN VARCHAR2,
schema          IN VARCHAR2 DEFAULT NULL,
    */

    private String kindToType(String kind) {
        if (kind.equals("PACKAGE-BODY")) {
            return "PACKAGE_BODY";
        }
        if (kind.equals("PACKAGE-SPEC")) {
            return "PACKAGE_SPEC";
        }
        if (kind.equals("PROCEDURE")) {
            return "PROCEDURE";
        }
        if (kind.equals("FUNCTION")) {
            return "FUNCTION";
        }
        if (kind.equals("VIEW")) {
            return "VIEW";
        }
        if (kind.equals("TRIGGER")) {
            return "TRIGGER";
        }
        if (kind.equals("TABLE")) {
            return "TABLE";
        }

        throw new Error("unknown kind: " + kind);
    }

    private String getCode(DBObject o) {
        try {
            PreparedStatement s = _connection.prepareStatement("select dbms_metadata.get_ddl(?,?),user from dual");
            s.setString(2, o.name);
            s.setString(1, kindToType(o.kind));
            ResultSet rs = s.executeQuery();
            rs.next();
            String code = rs.getString(1);
            String user = rs.getString(2);
            code = Helper.replaceObjectname(Helper.removeLeadingWS(code), o.name, user);
            rs.close();
            s.close();
            return code + (_withSlash ? "\n/" : "");
        } catch (SQLException e) {
            throw new Error(e);
        }
    }

}
