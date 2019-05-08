package spinat.oraclescripter;

import java.util.*;
import java.sql.*;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;
import spinat.plsqlparser.Ast;
import spinat.plsqlparser.Seq;

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
    private HashMap<String, String> table_sources = new HashMap<>();

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
            java.sql.Array a = (java.sql.Array) con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
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
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
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
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
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
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
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

    void einpacken(String table_name,
            boolean temporary,
            boolean commitPreserveRows,
            List<TableModel.ColumnModel> columns,
            List<TableModel.ConstraintModel> checkConstraints,
            TableModel.PrimaryKeyModel primaryKey) {
        TableModel m = new TableModel(table_name, temporary, commitPreserveRows, columns, checkConstraints, primaryKey);
        String source = m.ConvertToCanonicalString();
        this.table_sources.put(table_name, source);
        this.source_repo.add(new DBObject("TABLE", table_name), source);
    }

    void getKeyConstraints(ArrayList<String> tables, Map<String, TableModel.PrimaryKeyModel> primaryKeys,
            Map<String, ArrayList<TableModel.ConstraintModel>> constraints) throws SQLException {

        String constraintView = useDBAViews ? "dba_constraints" : "all_constraints";
        String constraintColView = useDBAViews ? "dba_cons_columns" : "all_cons_columns";
        String sql = "select c.table_name,c.constraint_name, c.constraint_type,ck.COLUMN_NAME\n"
                + " from " + constraintView + " c\n"
                + " inner join " + constraintColView + " ck\n"
                + "   on ck.OWNER = c.owner\n"
                + "   and ck.CONSTRAINT_NAME = c.CONSTRAINT_NAME\n"
                + " where c.constraint_type in ('U', 'P') "
                + " and c.table_name in (select column_value from table(?))"
                + " and c.owner = ?"
                + " order by c.owner, c.table_name, c.constraint_name, ck.position";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(sql)) {
            String[] arg = tables.toArray(new String[0]);
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
            ps.setString(2, this.owner);
            ArrayList<String> columns = null;
            String constraint_name = null;
            String table_name = null;
            String constraint_type = null;
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String cnew = rs.getString("CONSTRAINT_NAME");
                    if (!cnew.equals(constraint_name)) {
                        if (constraint_name != null) {
                            if (constraint_type.equals("P")) {
                                primaryKeys.put(table_name, new TableModel.PrimaryKeyModel(constraint_name, columns));
                            } else {
                                if (!constraints.containsKey(table_name)) {
                                    constraints.put(table_name, new ArrayList<>());
                                }
                                constraints.get(table_name).add(new TableModel.UniqueKeyModel(constraint_name, columns));
                            }
                        }
                        constraint_name = cnew;
                        table_name = rs.getString("TABLE_NAME");
                        constraint_type = rs.getString("CONSTRAINT_TYPE");
                        columns = new ArrayList<>();
                    }
                    columns.add(rs.getString("COLUMN_NAME"));
                }
                if (constraint_name != null) {
                    if (constraint_type.equals("P")) {
                        primaryKeys.put(table_name, new TableModel.PrimaryKeyModel(constraint_name, columns));
                    } else {
                        if (!constraints.containsKey(table_name)) {
                            constraints.put(table_name, new ArrayList<>());
                        }
                        constraints.get(table_name).add(new TableModel.UniqueKeyModel(constraint_name, columns));
                    }
                }
            }
        }
    }

    void getForeignKeyConstraints(ArrayList<String> tables, Map<String, ArrayList<TableModel.ConstraintModel>> constraints) throws SQLException {
        String constraintView = useDBAViews ? "dba_constraints" : "all_constraints";
        String constraintColView = useDBAViews ? "dba_cons_columns" : "all_cons_columns";
        String sql = "select sr.CONSTRAINT_NAME, sr.TABLE_NAME, src.COLUMN_NAME,\n"
                + "sp.CONSTRAINT_NAME as rCONSTRAINT_NAME, sp.owner as rowner, sp.TABLE_NAME as rtable_name, spc.COLUMN_NAME as rcolumn_name\n"
                + "from $constraints sr inner join $constraints sp\n"
                + "on sr.r_owner = sp.owner and sr.r_constraint_name = sp.constraint_name\n"
                + "inner join $cons_columns src on src.CONSTRAINT_NAME = sr.CONSTRAINT_NAME\n"
                + "and src.OWNER = sr.OWNER\n"
                + "inner join $cons_columns spc on spc.CONSTRAINT_NAME = sp.CONSTRAINT_NAME\n"
                + "and spc.OWNER = sp.OWNER\n"
                + "and spc.POSITION = src.POSITION\n"
                + "where sr.CONSTRAINT_TYPE = 'R'\n"
                + " and sr.table_name in (select column_value from table(?))"
                + " and sr.owner = ?"
                + " order by sr.constraint_name, src.position";
        sql = sql.replace("$constraints", constraintView);
        sql = sql.replace("$cons_columns", constraintColView);
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(sql)) {
            String[] arg = tables.toArray(new String[0]);
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
            ps.setString(2, this.owner);
            ArrayList<String> columns = null;
            ArrayList<String> rcolumns = null;
            String constraint_name = null;
            String table_name = null;
            String rtable_name = null;
            String rowner = null;

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String cnew = rs.getString("CONSTRAINT_NAME");
                    if (!cnew.equals(constraint_name)) {
                        if (constraint_name != null) {

                            if (!constraints.containsKey(table_name)) {
                                constraints.put(table_name, new ArrayList<>());
                            }
                            constraints.get(table_name).add(new TableModel.ForeignKeyModel(constraint_name, rowner, rtable_name, columns, rcolumns));
                        }

                        constraint_name = cnew;
                        table_name = rs.getString("TABLE_NAME");
                        rtable_name = rs.getString("RTABLE_NAME");
                        rowner = rs.getString("ROWNER");
                        if (rowner.equals(this.owner)) {
                            rowner = null;
                        }

                        columns = new ArrayList<>();
                        rcolumns = new ArrayList<>();
                    }
                    columns.add(rs.getString("COLUMN_NAME"));
                    rcolumns.add(rs.getString("rCOLUMN_NAME"));
                }

            }
            if (constraint_name != null) {

                if (!constraints.containsKey(table_name)) {
                    constraints.put(table_name, new ArrayList<>());
                }
                constraints.get(table_name).add(new TableModel.ForeignKeyModel(constraint_name, rowner, rtable_name, columns, rcolumns));
            }
        }
    }

    Map<String, ArrayList<TableModel.ConstraintModel>> getCheckConstraints(ArrayList<String> tables,
            Map<String, ArrayList<TableModel.ConstraintModel>> constraints) throws SQLException {
        String constraintView = useDBAViews ? "dba_constraints" : "all_constraints";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(
                "select constraint_name, table_name, search_condition \n"
                + " from " + constraintView + " c\n"
                + " where c.CONSTRAINT_TYPE = 'C'\n"
                + " and generated = 'USER NAME'"
                + " and c.table_name in (select column_value from table(?))"
                + " and c.owner = ?"
                + " order by table_name, constraint_name")) {
            ps.setFetchSize(10000);
            String[] arg = tables.toArray(new String[0]);
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
            ps.setString(2, this.owner);
            String old_table_name = null;
            ArrayList<TableModel.ConstraintModel> constraintList = null;
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (!tableName.equals(old_table_name)) {
                        if (constraintList != null) {
                            constraints.put(old_table_name, constraintList);
                        }
                        old_table_name = tableName;
                        constraintList = new ArrayList<>();
                    }
                    String constraintName = rs.getString("CONSTRAINT_NAME");
                    String s = rs.getString("SEARCH_CONDITION");
                    String canonicalSource = AstHelper.toCanonicalString(s);
                    constraintList.add(new TableModel.CheckConstraintModel(constraintName, canonicalSource));
                }
                if (constraintList != null) {
                    constraints.put(old_table_name, constraintList);
                }

            }
            return constraints;
        }
    }

    void loadTableSources(ArrayList<String> tables) throws SQLException {

        Map<String, ArrayList<TableModel.ConstraintModel>> constraints = new HashMap<>();
        Map<String, TableModel.PrimaryKeyModel> primaryKeys = new HashMap<>();
        getCheckConstraints(tables, constraints);
        getKeyConstraints(tables, primaryKeys, constraints);
        getForeignKeyConstraints(tables, constraints);

        String columnsView = useDBAViews ? "dba_tab_columns" : "all_tab_columns";
        String tableView = useDBAViews ? "dba_tables" : "all_tables";

        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(
                "select t.table_name, t.duration, column_name, data_type,char_length, "
                + " char_used, data_precision, data_scale, nullable \n"
                + " from " + tableView + " t inner join " + columnsView + " c on c.table_name = t.table_name and c.owner = t.owner \n"
                + " where t.table_name in (select column_value from table(?))"
                + " and t.owner = ?"
                + " order by table_name,column_id")) {
            ps.setFetchSize(10000);
            String[] arg = tables.toArray(new String[0]);
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
            ps.setString(2, this.owner);
            String old_table_name = null;
            StringBuilder b = new StringBuilder();

            try (ResultSet rsx = ps.executeQuery()) {
                ArrayList<Record> l = OraUtil.resultSetToList(rsx);
                ArrayList<ArrayList<Record>> gl = Record.group(l, new String[]{"TABLE_NAME"});
                for (ArrayList<Record> rl : gl) {
                    Record r0 = rl.get(0);
                    String table_name = r0.getString("TABLE_NAME");
                    String duration = r0.getString("DURATION");
                    ArrayList<TableModel.ColumnModel> columns = new ArrayList<>();
                    for (Record r : rl) {
                        String columnName = r.getString("COLUMN_NAME");
                        String dataType = r.getString("DATA_TYPE");
                        boolean nullable = r.getString("NULLABLE").equals("Y");
                        final String type;
                        if (dataType.equals("VARCHAR2")) {
                            int charLength = r.getInteger("CHAR_LENGTH");
                            String charUsed = r.getString("CHAR_USED");
                            String charOrByte = charUsed.equals("B") ? "BYTE" : "CHAR";
                            type = "VARCHAR2(" + charLength + " " + charOrByte + ")";
                        } else if (dataType.equals("NUMBER")) {
                            Integer dp = r.getInteger("DATA_PRECISION");
                            Integer ds = r.getInteger("DATA_SCALE");
                            if (dp == null) {
                                type = "NUMBER";
                            } else {
                                type = "NUMBER(" + dp + "," + ds + ")";
                            }
                        } else {
                            type = dataType;
                        }
                        columns.add(new TableModel.ColumnModel(columnName, type, nullable));
                    }
                    einpacken(table_name, duration != null,
                            "SYS$SESSION".equals(duration),
                            columns,
                            constraints.getOrDefault(old_table_name, new ArrayList<>()),
                            primaryKeys.get(old_table_name)
                    );
                }
            }
        }
    }

    public void load(ArrayList<DBObject> objects) throws SQLException {
        ArrayList<String> l = new ArrayList<>();
        ArrayList<String> views = new ArrayList<>();
        ArrayList<String> triggers = new ArrayList<>();
        ArrayList<String> tables = new ArrayList<>();
        for (DBObject o : objects) {
            if (o.type.equals("VIEW")) {
                views.add(o.name);
                continue;
            }
            if (o.type.equals("TRIGGER")) {
                triggers.add(o.name);
                continue;
            }
            if (o.type.equals("TABLE")) {
                tables.add(o.name);
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
        this.loadTableSources(tables);

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
        if ("TABLE".equals(objectType)) {
            return getTableSourceCode(objectName);
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
            return "create or replace and compile java source named \"" + objectName + "\" as \n" + s;
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

    private String getTableSourceCode(String table) {
        return table_sources.get(table);
    }

    public SourceRepo getSourceRepo() {
        return this.source_repo;
    }
}
