package spinat.oraclescripter;

import java.util.*;
import java.sql.*;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;

public class SourceCodeGetter {

    private static final String[] source_stuff = new String[]{
        "PACKAGE", "PACKAGE BODY", "TYPE", "TYPE BODY", "FUNCTION", "PROCEDURE", "JAVA SOURCE"};

    private final OracleConnection con;
    private final String owner;
    private final boolean useDBAViews;

    public SourceCodeGetter(OracleConnection c, String owner, boolean useDBAViews) {
        this.con = c;
        this.owner = owner;
        this.useDBAViews = useDBAViews;
    }

    private HashMap<String, String> usersources = new HashMap<>();
    private HashMap<String, String> view_sources = new HashMap<>();
    private HashMap<String, String> view_tab_columns = new HashMap<>();
    private HashMap<String, String> trigger_sources = new HashMap<>();
    private final SourceRepo source_repo = new SourceRepo();

    void loadUserSource(ArrayList<String> objectList) throws SQLException {
        String sourceView = useDBAViews ? "dba_source" : "all_source";
        String sql = "select name,type,line,text from " + sourceView + "\n"
                + " where (type,name) in "
                + "  (select substr(column_value,1,instr(column_value,',')-1),"
                + "         substr(column_value,instr(column_value,',')+1)"
                + " from table(?))"
                + " and owner = ?"
                + " order by name,type,line,text";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(sql)) {
            ps.setFetchSize(10000);
            String[] arg = objectList.toArray(new String[0]);
            oracle.sql.ARRAY a = (oracle.sql.ARRAY) con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setARRAY(1, a);
            ps.setString(2, this.owner);
            String key = null;
            DBObject currDbo = null;
            StringBuilder b = new StringBuilder();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    String type = rs.getString(2);
                    String txt = rs.getString(4);
                    String k2 = type + "," + name;
                    if (!k2.equals(key)) {
                        if (key != null) {
                            this.usersources.put(key, b.toString());
                            this.source_repo.add(currDbo, b.toString());
                        }
                        b = new StringBuilder();
                        key = k2;
                        currDbo = new DBObject(type, name);
                    }
                    b.append(txt);
                }
                this.usersources.put(key, b.toString());
                this.source_repo.add(currDbo, b.toString());
            }
        }
    }

    void loadTriggerSource(ArrayList<String> triggers) throws SQLException {
        String triggerView = useDBAViews ? "dba_triggers" : "all_triggers";
        try (
                OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement("select "
                        + "TRIGGER_NAME,\n"
                        + "TRIGGER_TYPE,\n"
                        + "TRIGGERING_EVENT,\n"
                        + "TABLE_OWNER,\n"
                        + "BASE_OBJECT_TYPE,\n"
                        + "TABLE_NAME,\n"
                        + "COLUMN_NAME,\n"
                        + "REFERENCING_NAMES,\n"
                        + "WHEN_CLAUSE,\n"
                        + "STATUS,\n"
                        + "DESCRIPTION,\n"
                        + "ACTION_TYPE,\n"
                        + "TRIGGER_BODY,\n"
                        + "owner\n"
                        + " from " + triggerView + " where trigger_name in (select column_value from table(?)) "
                        + " and owner = ?")) {
            ps.setFetchSize(10000);
            String[] arg = triggers.toArray(new String[0]);
            oracle.sql.ARRAY a = (oracle.sql.ARRAY) con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setARRAY(1, a);
            ps.setString(2, this.owner);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String trigger_name = rs.getString(1);
                    String trigger_type = rs.getString(2);
                    String trigger_event = rs.getString(3);
                    String table_owner = rs.getString(4);
                    String table_name = rs.getString(5);
                    String column_name = rs.getString(7);
                    String referencing_names = rs.getString(8);
                    String when_clause = rs.getString(9);
                    String status = rs.getString(10);
                    String description = rs.getString(11);
                    String action_type = rs.getString(12);
                    String trigger_body = rs.getString(13);
                    //String owner = rs.getString(14);

                    // column_name != null is not implemented,
                    // I think this can be used with inline tables
                    if (column_name != null) {
                        throw new RuntimeException("Trigger " + trigger_name + ": "
                                + " column_name in user_triggers is not null");
                    }

                    String desc2 = removeUser(description, owner);

                    String src = "create or replace trigger " + desc2
                            + ((when_clause == null) ? "" : " when (" + when_clause + ")\n")
                            + trigger_body;
                    trigger_sources.put(trigger_name, src);
                    String src2 = "trigger " + desc2
                            + ((when_clause == null) ? "" : " when (" + when_clause + ")\n")
                            + trigger_body;

                    this.source_repo.add(new DBObject("TRIGGER", trigger_name), src2);
                }
            }
        }
    }

    void loadViewSource(ArrayList<String> views) throws SQLException {
        String viewsView = useDBAViews ? "dba_views" : "all_views";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(
                "select view_name,text from " + viewsView + "\n"
                + " where view_name in (select column_value from table(?))\n"
                + "and owner = ?")) {
            ps.setFetchSize(10000);
            String[] arg = views.toArray(new String[0]);
            oracle.sql.ARRAY a = (oracle.sql.ARRAY) con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setARRAY(1, a);
            ps.setString(2, this.owner);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    String txt = rs.getString(2);
                    this.view_sources.put(name, txt);
                    this.source_repo.add(new DBObject("VIEW", name), "view " + Helper.maybeOracleQuote(name)
                            + " (" + view_tab_columns.get(name) + ") as \n" + txt);
                }
            }
        }

    }

    void loadViewColumns(ArrayList<String> views) throws SQLException {
        String columnsView = useDBAViews ? "dba_tab_columns" : "all_tab_columns";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(
                "select table_name, column_name from " + columnsView + "\n"
                + "where table_name in (select column_value from table(?))"
                + " and owner = ?"
                + " order by table_name,column_id")) {
            ps.setFetchSize(10000);
            String[] arg = views.toArray(new String[0]);
            oracle.sql.ARRAY a = (oracle.sql.ARRAY) con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setARRAY(1, a);
            ps.setString(2, this.owner);
            String old_table_name = null;
            StringBuilder b = new StringBuilder();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String table_name = rs.getString(1);
                    String column_name = rs.getString(2);
                    if (old_table_name != null && !old_table_name.equals(table_name)) {
                        this.view_tab_columns.put(old_table_name, b.toString().substring(2));
                    }
                    if (!table_name.equals(old_table_name)) {
                        b = new StringBuilder();
                        old_table_name = table_name;
                    }
                    b.append(", ").append(Helper.maybeOracleQuote(column_name));
                }
                if (b.length() >= 2 && old_table_name != null) {
                    this.view_tab_columns.put(old_table_name, b.toString().substring(2));
                }
            }
        }
    }

    public void load(ArrayList<DBObject> objects) throws SQLException {
        this.usersources = new HashMap<>();
        ArrayList<String> l = new ArrayList<>();
        ArrayList<String> views = new ArrayList<>();
        ArrayList<String> triggers = new ArrayList<>();
        for (DBObject o : objects) {
            if (o.type.equals("VIEW")) {
                views.add(o.name);
                continue;
            }
            if (o.type.equals("TRIGGER")) {
                triggers.add(o.name);
                continue;
            }
            if (o.type.equals("PACKAGE")) {
                l.add("PACKAGE BODY," + o.name);
            }
            if (o.type.equals("TYPE")) {
                l.add("TYPE BODY," + o.name);
            }
            l.add(o.type + "," + o.name);
        }

        this.loadUserSource(l);
        this.loadTriggerSource(triggers);
        this.loadViewColumns(views);
        this.loadViewSource(views);

        String columnsView = useDBAViews ? "dba_tab_columns" : "all_tab_columns";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(
                "select table_name, column_name from " + columnsView + "\n"
                + "where table_name in (select column_value from table(?))"
                + " and owner = ?"
                + " order by table_name,column_id")) {
            ps.setFetchSize(10000);
            String[] arg = views.toArray(new String[0]);
            oracle.sql.ARRAY a = (oracle.sql.ARRAY) con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setARRAY(1, a);
            ps.setString(2, this.owner);
            String old_table_name = null;
            StringBuilder b = new StringBuilder();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String table_name = rs.getString(1);
                    String column_name = rs.getString(2);
                    if (old_table_name != null && !old_table_name.equals(table_name)) {
                        this.view_tab_columns.put(old_table_name, b.toString().substring(2));
                    }
                    if (!table_name.equals(old_table_name)) {
                        b = new StringBuilder();
                        old_table_name = table_name;
                    }
                    b.append(", ").append(Helper.maybeOracleQuote(column_name));
                }
                if (b.length() >= 2 && old_table_name != null) {
                    this.view_tab_columns.put(old_table_name, b.toString().substring(2));
                }
            }
        }

    }

    public String getCode(String objectType, String objectName) {
        if (Helper.arrayIndexOf(source_stuff, objectType) >= 0) {
            return getUserSourceCode(objectName, objectType);
        }
        if ("VIEW".equals(objectType)) {
            return getViewSourceCode(objectName);
        }
        if ("TRIGGER".equals(objectType)) {
            return getTriggerSourceCode(objectName);
        }
        throw new RuntimeException("unknown kind of object " + objectType);
    }

    private String getUserSourceCode(String objectName, String objectType) {
        String key = objectType + "," + objectName;
        String s = this.usersources.getOrDefault(key, null);

        if (s == null || s.trim().isEmpty()) {
            return null;
        }
        if (objectType.equals("JAVA SOURCE")) {
            return "create or replace java source \"" + objectName + "\" as \n" + s;
        } else {
            return "create or replace " + s;
        }
    }

    private String getViewSourceCode(String view) {

        String code = this.view_sources.get(view);
        return "create or replace force view " + Helper.maybeOracleQuote(view)
                + " (" + view_tab_columns.get(view) + ") as \n"
                + code;
    }

    // we search for "user." case insensitive or "\"user\"."  case sensitive
    // if they are found we remove them 
    // the risk of changing comments is ignored
    private String removeUser(String txt, String user) {
        String txtu = txt.toUpperCase(Locale.US);
        String useru = user.toUpperCase(Locale.US);
        int p = txtu.indexOf(useru + ".");
        String txt2;
        if (p < 0) {
            txt2 = txt;
        } else {
            txt2 = txt.substring(0, p)
                    + txt.substring(p + user.length() + 1);
        }

        int p2 = txt2.indexOf("\"" + user + "\".");
        String txt3;
        if (p2 < 0) {
            txt3 = txt2;
        } else {
            txt3 = txt2.substring(0, p2)
                    + txt2.substring(p2 + user.length() + 3);
        }
        if (txt3.equals(txt)) {
            return txt;
        } else {
            return removeUser(txt3, user);
        }
    }

    private String getTriggerSourceCode(String trigger) {
        return trigger_sources.get(trigger);
    }

    public SourceRepo getSourceRepo() {
        return this.source_repo;
    }
}
