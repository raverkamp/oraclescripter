package spinat.oraclescripter;

import java.sql.*;

public class Scripter2 implements IObjectScripter {

    private Connection _connection;

    private boolean _withSlash;

    private boolean _use_dbms_metadata = false;

    private String _schema = null;

    public Scripter2(Connection c, boolean withSlash, boolean use_dbms_metadata) {
        try {
            this._connection = c;
            this._withSlash = withSlash;
            this._use_dbms_metadata = use_dbms_metadata;
            Statement s = c.createStatement();
            ResultSet rs = s.executeQuery("select user from dual");
            rs.next();
            _schema = rs.getString(1);
            rs.close();
            s.close();
        } catch (SQLException e) {
            throw new Error(e);
        }
    }

    /*
     public Scripter2(Connection c) {
     this(c,true);
     }
     */
    public void scriptCode(java.util.ArrayList os,
            IObjectWorker w) {
        int size = os.size();
        for (int j = 0; j < size; j++) {
            DBObject o = (DBObject) os.get(j);
            w.work(o, getCode(o));
        }
    }
    ;

    // the type used by dbms_metadata 
    private StringMap kindToTypeMap
            = new StringMap(new String[]{
                "PACKAGE-BODY", "PACKAGE_BODY",
                "PACKAGE-SPEC", "PACKAGE_SPEC",
                "PROCEDURE", "PROCEDURE",
                "FUNCTION", "FUNCTION",
                "VIEW", "VIEW",
                "TRIGGER", "TRIGGER",
                "TABLE", "TABLE",
                "TYPE-SPEC", "TYPE_SPEC",
                "TYPE-BODY", "TYPE_BODY"});

    // the type used in user_objects
    private StringMap kindToSourceTypeMap
            = new StringMap(new String[]{
                "PACKAGE-BODY", "PACKAGE BODY",
                "PACKAGE-SPEC", "PACKAGE",
                "PROCEDURE", "PROCEDURE",
                "FUNCTION", "FUNCTION",
                "VIEW", "VIEW",
                "TRIGGER", "TRIGGER",
                "TABLE", "TABLE",
                "TYPE-SPEC", "TYPE",
                "TYPE-BODY", "TYPE BODY"});

    private String kindToType(String kind) {
        String res = kindToTypeMap.getString(kind);
        if (res == null) {
            throw new Error("unknown kind: " + kind);
        } else {
            return res;
        }
    }

    private String kindToSourceType(String kind) {
        String res = kindToSourceTypeMap.getString(kind);
        if (res == null) {
            throw new Error("unknown kind: " + kind);
        } else {
            return res;
        }
    }

    private String getCode(DBObject o) {
        String code;
        if (_use_dbms_metadata) {
            code = new MetaDataCodeGetter().getCode(_connection, kindToType(o.kind), o.name, _schema);
        } else {
            SourceCodeGetter scg = new SourceCodeGetter();
            if (o.kind.equals("PACKAGE")) {
                code = scg.getCode(_connection, "PACKAGE", o.name, _schema);
                code = code + "\n/\n" + scg.getCode(_connection, "PACKAGE BODY", o.name, _schema);
            } else if (o.kind.equals("TYPE")) {
                code = scg.getCode(_connection, "TYPE", o.name, _schema);
                code = code + "\n/\n" + scg.getCode(_connection, "TYPE BODY", o.name, _schema);
            } else {
                code = scg.getCode(_connection, kindToSourceType(o.kind), o.name, _schema);
            }
        }
        return code + (_withSlash ? "\n/" : "");
    }

}
