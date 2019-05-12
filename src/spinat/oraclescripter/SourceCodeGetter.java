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
                + " order by c.table_name, c.constraint_name, ck.position";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(sql)) {
            String[] arg = tables.toArray(new String[0]);
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
            ps.setString(2, this.owner);
            try (ResultSet rsx = ps.executeQuery()) {
                ArrayList<Record> l = OraUtil.resultSetToList(rsx);
                ArrayList<ArrayList<Record>> rll = Record.group(l, new String[]{"TABLE_NAME", "CONSTRAINT_NAME"});
                for (ArrayList<Record> rl : rll) {
                    Record r0 = rl.get(0);

                    String constraint_name = r0.getString("CONSTRAINT_NAME");
                    String table_name = r0.getString("TABLE_NAME");
                    String constraint_type = r0.getString("CONSTRAINT_TYPE");
                    ArrayList<String> columns = new ArrayList<>();
                    for (Record r : rl) {
                        columns.add(r.getString("COLUMN_NAME"));
                    }
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

            try (ResultSet rsx = ps.executeQuery()) {
                ArrayList<Record> l = OraUtil.resultSetToList(rsx);
                ArrayList<ArrayList<Record>> rll = Record.group(l, new String[]{"TABLE_NAME", "CONSTRAINT_NAME"});
                for (ArrayList<Record> rl : rll) {
                    Record r0 = rl.get(0);
                    String constraint_name = r0.getString("CONSTRAINT_NAME");
                    String table_name = r0.getString("TABLE_NAME");
                    String rtable_name = r0.getString("RTABLE_NAME");
                    String rowner = r0.getString("ROWNER");
                    ArrayList<String> columns = new ArrayList<>();
                    ArrayList<String> rcolumns = new ArrayList<>();

                    for (Record r : rl) {
                        columns.add(r.getString("COLUMN_NAME"));
                        rcolumns.add(r.getString("RCOLUMN_NAME"));
                    }
                    if (!constraints.containsKey(table_name)) {
                        constraints.put(table_name, new ArrayList<>());
                    }
                    constraints.get(table_name).add(new TableModel.ForeignKeyModel(constraint_name, rowner, rtable_name, columns, rcolumns));
                }
            }
        }
    }

    void getCheckConstraints(ArrayList<String> tables,
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
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (!constraints.containsKey(tableName)) {
                        constraints.put(tableName, new ArrayList<>());
                    }
                    String constraintName = rs.getString("CONSTRAINT_NAME");
                    String s = rs.getString("SEARCH_CONDITION");
                    String canonicalSource = AstHelper.toCanonicalString(s);

                    constraints.get(tableName).add(new TableModel.CheckConstraintModel(constraintName, canonicalSource));
                }
            }
        }
    }

    Map<String, ArrayList<TableModel.IndexModel>> getIndexes(ArrayList<String> tables) throws SQLException {
        String indexView = this.useDBAViews ? "DBA_INDEXES" : "ALL_INDEXES";
        String indexColumnsView = this.useDBAViews ? "DBA_IND_COLUMNS" : "ALL_IND_COLUMNS";
        String indexColumnsExpressions = this.useDBAViews ? "DBA_IND_EXPRESSIONS" : "ALL_IND_EXPRESSIONS";

        String constraintView = this.useDBAViews ? "dba_constraints" : "all_constraints";

        String sql = "select i.index_name, i.index_type, i.table_name, i.uniqueness, c.COLUMN_NAME,e.COLUMN_EXPRESSION, c.COLUMN_POSITION "
                + "from " + indexView + " i "
                + " inner join " + indexColumnsView + " c "
                + "on c.index_owner = i.owner "
                + " and c.index_name = i.INDEX_NAME "
                + " left join " + indexColumnsExpressions + " e "
                + " on e.INDEX_NAME = c.INDEX_NAME "
                + " and e.INDEX_OWNER = c.INDEX_OWNER "
                + " and e.COLUMN_POSITION = c.COLUMN_POSITION "
                + " where i.table_name in (select column_value from table(?))"
                + " and i.table_owner = ?"
                + " and (i.owner, i.index_name) not in (select con.owner, con.constraint_name from " + constraintView + " con  where con.constraint_type in ('P', 'U')) "
                + " order by index_name, i.owner, c.column_position";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(sql)) {
            ps.setFetchSize(10000);
            String[] arg = tables.toArray(new String[0]);
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
            ps.setString(2, this.owner);
            try (ResultSet rs = ps.executeQuery()) {
                ArrayList<Record> l = OraUtil.resultSetToList(rs);
                ArrayList<ArrayList<Record>> gl = Record.group(l, new String[]{"INDEX_NAME"});
                Map<String, ArrayList<TableModel.IndexModel>> res = new HashMap<>();
                for (ArrayList<Record> lr : gl) {
                    Record r0 = lr.get(0);
                    String indexName = r0.getString("INDEX_NAME");
                    String tableName = r0.getString("TABLE_NAME");
                    boolean unqiue = r0.getString("UNIQUENESS").equals("UNIQUE");
                    ArrayList<String> columns = new ArrayList<>();
                    for (Record r : lr) {
                        String ce = r.getString("COLUMN_EXPRESSION");
                        if (ce == null || ce.isEmpty()) {
                            columns.add(r.getString("COLUMN_NAME"));
                        } else {
                            String cec = AstHelper.toCanonicalString(r.getString("COLUMN_EXPRESSION"));
                            columns.add(cec);
                        }
                    }
                    if (!res.containsKey(tableName)) {
                        res.put(tableName, new ArrayList<>());
                    }
                    res.get(tableName).add(new TableModel.IndexModel(indexName, columns, unqiue));
                }
                return res;

            }
        }

    }

    Map<String, String> loadTableComments(ArrayList<String> tables) throws SQLException {
        String tableView = useDBAViews ? "dba_tab_comments" : "all_tab_comments";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(
                "select t.table_name, t.comments "
                + " from " + tableView + " t "
                + " where t.table_name in (select column_value from table(?))"
                + " and t.owner = ?"
                + " and t.comments is not null")) {
            ps.setFetchSize(10000);
            String[] arg = tables.toArray(new String[0]);
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
            ps.setString(2, this.owner);
            try (ResultSet rs = ps.executeQuery()) {
                Map<String, String> res = new HashMap<>();
                while (rs.next()) {
                    res.put(rs.getString("TABLE_NAME"), rs.getString("COMMENTS"));
                }
                return res;
            }
        }
    }

    Map<String, String> loadColumnComments(ArrayList<String> tables) throws SQLException {
        String colView = useDBAViews ? "dba_col_comments" : "all_col_comments";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(
                "select t.table_name, t.column_name, t.comments "
                + " from " + colView + " t "
                + " where t.table_name in (select column_value from table(?))"
                + " and t.owner = ?"
                + " and t.comments is not null")) {
            ps.setFetchSize(10000);
            String[] arg = tables.toArray(new String[0]);
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
            ps.setString(2, this.owner);
            try (ResultSet rs = ps.executeQuery()) {
                Map<String, String> res = new HashMap<>();
                while (rs.next()) {
                    String key = rs.getString("TABLE_NAME") + "\n" + rs.getString("COLUMN_NAME");
                    res.put(key, rs.getString("COMMENTS"));
                }
                return res;
            }
        }
    }

    Map<String, TableModel.ExternalTableData> getExternalTableData(ArrayList<String> tables) throws SQLException {
        String extTableView = this.useDBAViews ? "dba_external_tables" : "all_external_tables";
        String extLocations = this.useDBAViews ? "dba_external_locations" : "all_external_locations";
        Map<String, TableModel.ExternalTableData> res = new HashMap<>();
        String sql = "select t.owner, t.table_name, type_name, default_directory_name, l.location\n"
                + "from " + extTableView + " t \n"
                + "left join " + extLocations + " l \n"
                + "on l.table_name = t.table_name \n"
                + "and l.owner = t.owner\n"
                + " where t.table_name in (select column_value from table(?))"
                + " and t.owner = ?"
                + "order by owner, table_name, location";
        try (OraclePreparedStatement ps = (OraclePreparedStatement) con.prepareStatement(sql)) {
            String[] arg = tables.toArray(new String[0]);
            java.sql.Array a = con.createARRAY("DBMSOUTPUT_LINESARRAY", arg);
            ps.setArray(1, a);
            ps.setString(2, this.owner);
            try (ResultSet rsx = ps.executeQuery()) {
                ArrayList<Record> l = OraUtil.resultSetToList(rsx);
                ArrayList<ArrayList<Record>> gl = Record.group(l, new String[]{"TABLE_NAME"});
                for (ArrayList<Record> lr : gl) {
                    Record r0 = lr.get(0);
                    String tableName = r0.getString("TABLE_NAME");
                    String typeName = r0.getString("TYPE_NAME");
                    String defaultDirectory = r0.getString("DEFAULT_DIRECTORY_NAME");
                    ArrayList<String> locations = new ArrayList<>();
                    if (r0.getString("LOCATION") != null) {
                        for (Record r : lr) {
                            locations.add(r.getString("LOCATION"));
                        }
                    }
                    res.put(tableName, new TableModel.ExternalTableData(defaultDirectory, typeName, locations));

                }
            }
        }
        return res;
    }

    void loadTableSources(ArrayList<String> tables) throws SQLException {

        Map<String, ArrayList<TableModel.ConstraintModel>> constraints = new HashMap<>();
        Map<String, TableModel.PrimaryKeyModel> primaryKeys = new HashMap<>();
        getCheckConstraints(tables, constraints);
        getKeyConstraints(tables, primaryKeys, constraints);
        getForeignKeyConstraints(tables, constraints);

        Map<String, String> tabComments = loadTableComments(tables);
        Map<String, String> colComments = loadColumnComments(tables);

        String columnsView = useDBAViews ? "dba_tab_columns" : "all_tab_columns";
        String tableView = useDBAViews ? "dba_tables" : "all_tables";

        Map<String, ArrayList<TableModel.IndexModel>> indexes = getIndexes(tables);
        Map<String, TableModel.ExternalTableData> externalTableData = getExternalTableData(tables);

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
                        String key = table_name + "\n" + columnName;
                        columns.add(new TableModel.ColumnModel(columnName, type, nullable, colComments.get(key)));
                    }

                    TableModel m = new TableModel(table_name,
                            duration != null,
                            "SYS$SESSION".equals(duration),
                            columns,
                            constraints.getOrDefault(table_name, new ArrayList<>()),
                            primaryKeys.get(table_name),
                            tabComments.get(table_name),
                            indexes.get(table_name),
                            externalTableData.get(table_name));
                    String source = m.convertToCanonicalString();
                    this.table_sources.put(table_name, source);
                    this.source_repo.add(new DBObject("TABLE", table_name), source);

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
