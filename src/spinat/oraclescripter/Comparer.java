package spinat.oraclescripter;

import spinat.sqlplus.SqlPlus;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import oracle.jdbc.OracleConnection;
import spinat.plsqlparser.Ast;
import spinat.plsqlparser.Ast.RelationalProperty;
import spinat.plsqlparser.ParseException;
import spinat.plsqlparser.Parser;
import spinat.plsqlparser.Res;
import spinat.plsqlparser.Scanner;
import spinat.plsqlparser.Seq;
import spinat.sqlplus.CodeInfo;
import spinat.sqlplus.Snippet;

public class Comparer {

    private static String padRight(String s, int n, char c) {
        if (s.length() >= n) {
            return s;
        }
        StringBuilder b = new StringBuilder(s);
        for (int i = 0; i < n - s.length(); i++) {
            b.append(c);
        }
        return b.toString();
    }

    private static String padLeft(String s, int n, char c) {
        if (s.length() >= n) {
            return s;
        }
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < n - s.length(); i++) {
            b.append(c);
        }
        b.append(s);
        return b.toString();
    }

    // main entry point for doing compare
    public static void mainx(String[] args) throws Exception {
        try {
            java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (SQLException e) {
            Helper.abort("can not initialize Oracle JDBC\n" + e.toString());
        }
        if (!args[0].equalsIgnoreCase("compare")) {
            throw new RuntimeException("BUG: first argument must be 'compare'");
        }
        if (args.length == 2) {
            mainFile(args[1]);
        } else {
            Helper.abort("expecting two arguments for compare mode, the first must be \"compare\".\n"
                    + "The second must be the name of a properties file.");
        }
    }

    private static String stripSemicolon(String s) {
        s = s.trim();
        if (s.endsWith(";")) {
            return s.substring(0, s.length() - 1);
        } else {
            return s;
        }
    }

    // compare two pieces of code
    // ignore spaces and case differences in leading create or replace ...
    private static boolean equalCode(String what, String s1, String s2) {
        s1 = Helper.normalizeLineEnd(s1);
        s2 = Helper.normalizeLineEnd(s2);
        final Pattern startPattern;
        switch (what) {
            case "TRIGGER":
                startPattern = Pattern.compile("trigger\\s+", Pattern.CASE_INSENSITIVE);
                break;
            case "VIEW":
                startPattern = Pattern.compile("view\\s+", Pattern.CASE_INSENSITIVE);
                s1 = stripSemicolon(s1);
                s2 = stripSemicolon(s2);
                break;
            case "TYPE":
                startPattern = Pattern.compile("type\\s+", Pattern.CASE_INSENSITIVE);
                s1 = stripSemicolon(s1);
                s2 = stripSemicolon(s2);
                break;
            case "TYPE BODY":
                startPattern = Pattern.compile("type\\s+body\\s+", Pattern.CASE_INSENSITIVE);
                break;
            case "PACKAGE":
                startPattern = Pattern.compile("package\\s+", Pattern.CASE_INSENSITIVE);
                break;
            case "PACKAGE BODY":
                startPattern = Pattern.compile("package\\s+body\\s+", Pattern.CASE_INSENSITIVE);
                break;
            case "PROCEDURE":
                startPattern = Pattern.compile("procedure\\s+", Pattern.CASE_INSENSITIVE);
                break;
            case "FUNCTION":
                startPattern = Pattern.compile("function\\s+", Pattern.CASE_INSENSITIVE);
                break;
            default:
                startPattern = Pattern.compile("\\s*", Pattern.CASE_INSENSITIVE);
        }

        Matcher m1 = startPattern.matcher(s1);
        Matcher m2 = startPattern.matcher(s2);
        if (m1.lookingAt() && m2.lookingAt()) {
            s1 = s1.substring(m1.end());
            s2 = s2.substring(m2.end());
        }
        return s1.equals(s2);
    }

    private static class CompareRec {

        public final String schema;
        public final SourceRepo repoDB;
        public final SourceRepo repoDisk;

        public CompareRec(String schema, SourceRepo repoDB, SourceRepo repoDisk) {
            this.schema = schema;
            this.repoDB = repoDB;
            this.repoDisk = repoDisk;
        }
    }

    // invocation with a properties file
    // the required properties:
    //   schemas: comma separated list of schemas
    //   connection: a connection descriptor string
    // per schema there must be a property
    //   <SCHEMA>.start which names the Sql Plus file to start which loads the objects into
    //   the database
    static void mainFile(String fileName) throws Exception {
        Path realPath = Paths.get(fileName).toAbsolutePath();
        Properties props = Helper.loadProperties(realPath);
        String schemas = Helper.getProp(props, "schemas");
        boolean includeTables = Helper.getPropBool(props, "includetables", false);
        String connectionDesc = Helper.getProp(props, "connection");
        boolean ignoreDBOnly = Helper.getPropBool(props, "ignoredbonly", false);
        String charsetStr = Helper.getProp(props, "charset", "ISO-8859-1");
        SortedMap<String, Charset> sm = Charset.availableCharsets();
        if (!sm.containsKey(charsetStr)) {
            StringBuilder sb = new StringBuilder();
            for (String x : sm.keySet()) {
                sb.append(x).append(" ");
            }
            Helper.abort("the given charset is not defined: " + charsetStr + ", possible values\n" + sb.toString());
            return;
        }
        Charset charset = sm.get(charsetStr);

        OracleConnection connection = ConnectionUtil.getConnection(connectionDesc);
        String[] schema_list = schemas.split(",");
        final boolean useDBAViews;
        if (schema_list.length == 1) {
            useDBAViews = false;
        } else {
            if (!ConnectionUtil.hasDBAViews(connection)) {
                Helper.abort("the connection needs access to dba views, the view like dba_objects and etc.");

                return; // abort aborts
            }
            useDBAViews = true;
        }

        // create the temporary directories for comparision
        Path tempDir = Files.createTempDirectory("changes");
        Path dbDir = tempDir.resolve("DB");
        Files.createDirectory(dbDir);
        Path diskDir = tempDir.resolve("DISK");
        Files.createDirectory(diskDir);
        ArrayList<CompareRec> cmpRecs = new ArrayList<>();
        for (String schema : schema_list) {
            String owner = schema.toUpperCase(Locale.ROOT);
            ConnectionUtil.ObjectCond conds = new ConnectionUtil.ObjectCond();
            conds.obj_where = "1=1";
            ArrayList<DBObject> dbObjects = ConnectionUtil.getDBObjects(connection, owner, useDBAViews, conds, includeTables);
            SourceCodeGetter sc = new SourceCodeGetter(connection, owner, useDBAViews);
            sc.load(dbObjects);
            SourceRepo repoDB = sc.getSourceRepo();
            String startFile = Helper.getProp(props, schema + ".start");
            Path startPath = realPath.getParent().resolve(startFile).toAbsolutePath();
            Path baseDir = startPath.getParent();
            SourceRepo repoDisk = loadSource(startPath, baseDir, charset);
            CompareRec cr = new CompareRec(schema, repoDB, repoDisk);
            cmpRecs.add(cr);
            System.out.println("loaded schema: " + schema);
        }
        boolean hasDifferenes = false;
        for (CompareRec r : cmpRecs) {
            String owner = r.schema.toUpperCase(Locale.ROOT);
            Path dbDir2 = Files.createDirectory(dbDir.resolve(owner));
            Path diskDir2 = Files.createDirectory(diskDir.resolve(owner));
            boolean x = compareRepos(r.schema, r.repoDisk, diskDir2, r.repoDB, dbDir2, ignoreDBOnly);
            hasDifferenes = x || hasDifferenes;
        }
        System.out.println("--- done ---");
        if (hasDifferenes) {
            System.out.println("changes are in folder: " + tempDir);
            String winmerge_cmd = Helper.getProp(props, "winmerge_cmd", "#");
            if (!winmerge_cmd.equalsIgnoreCase("#")) {
                if (winmerge_cmd == null || winmerge_cmd.trim().isEmpty()) {
                    winmerge_cmd = "C:\\Program Files (x86)\\WinMerge\\WinMergeU.exe";
                }
                ProcessBuilder pb = new ProcessBuilder(new String[]{winmerge_cmd,
                    "/wl", "/wr", "/u", "/dl", "DB", "/dr", "DISK",
                    "DB",
                    "DISK"});
                pb.directory(tempDir.toFile());
                try {
                    Process pr = pb.start();
                    pr.waitFor();
                } catch (java.io.IOException e) {
                    System.err.println("can no start winmerge:" + e.getMessage());
                    System.err.println("winmerge command is: " + winmerge_cmd);
                }
            }
            if (Helper.getProp(props, "usemeld", "N").equalsIgnoreCase("Y")) {
                ProcessBuilder pb = new ProcessBuilder(new String[]{"meld",
                    "DB",
                    "DISK"});
                pb.directory(tempDir.toFile());
                Process pr = pb.start();
                pr.waitFor();
            }
        } else {
            System.out.println("there are no differences");

        }
    }

    static String dataTypeToString(Ast.DataType dt) {
        if (dt instanceof Ast.NamedType) {
            Ast.NamedType nt = (Ast.NamedType) dt;
            // only one?
            return nt.idents.get(0).val;
        }
        if (dt instanceof Ast.ParameterizedType) {
            Ast.ParameterizedType pt = (Ast.ParameterizedType) dt;
            String name = pt.ident.val;
            if (name.equals("VARCHAR2")) {
                int len = pt.var1;
                final String s;
                if (pt.charOrByte == null) {
                    s = "";
                } else {
                    s = " " + pt.charOrByte.toUpperCase();
                }
                return "VARCHAR2(" + len + s + ")";
            }
            if (name.equals("NUMBER")) {
                int prec = pt.var1;
                Integer a = pt.var2;
                int scale = a == null ? 0 : a;
                return "NUMBER(" + prec + "," + scale + ")";
            }
            return "?";
        }
        if (dt instanceof Ast.TimestampWithTimezone) {
            Ast.TimestampWithTimezone dtz = (Ast.TimestampWithTimezone) dt;
            int size = dtz.size == null ? 6 : dtz.size;
            final String tz;
            if (dtz.hasTimeZone) {
                tz = " WITH" + (dtz.localTimeZone ? " LOCAL" : "") + " TIME ZONE";
            } else {
                tz = "";
            }
            return "TIMESTAMP(" + size + ")" + tz;
        }
        return "?";
    }

    static TableModel.ConstraintModel astConstraintToModelConstraint(String source, Ast.ConstraintDefinition rp) {

        if (rp instanceof Ast.CheckConstraintDefinition) {
            // fixme: this is a hack to get the underlying sequence of the constraint condition
            Ast.CheckConstraintDefinition cd = (Ast.CheckConstraintDefinition) rp;
            Ast.Expression ex = cd.expression;
            String constraintSource = source.substring(ex.getStart(), ex.getEnd());
            String canonicalSource = AstHelper.toCanonicalString(constraintSource);
            TableModel.CheckConstraintModel cm = new TableModel.CheckConstraintModel(cd.name.val, canonicalSource);
            return cm;
        }
        if (rp instanceof Ast.PrimaryKeyDefinition) {
            Ast.PrimaryKeyDefinition pk = (Ast.PrimaryKeyDefinition) rp;
            String pkname = pk.name.val;
            List<String> columns = pk.columns.stream().map(x -> x.val).collect(Collectors.toList());

            return new TableModel.PrimaryKeyModel(pkname, columns);

        }
        if (rp instanceof Ast.UniqueKeyDefinition) {
            Ast.UniqueKeyDefinition uk = (Ast.UniqueKeyDefinition) rp;
            String ukname = uk.name.val;
            List<String> columns = uk.columns.stream().map(x -> x.val).collect(Collectors.toList());
            return new TableModel.UniqueKeyModel(ukname, columns);
        }
        if (rp instanceof Ast.ForeignKeyDefinition) {
            Ast.ForeignKeyDefinition fk = (Ast.ForeignKeyDefinition) rp;
            String fkname = fk.name.val;
            List<String> columns = fk.columns.stream().map(x -> x.val).collect(Collectors.toList());
            List<String> rcolumns = fk.rcolumns.stream().map(x -> x.val).collect(Collectors.toList());
            String rtableOwner = fk.rtable.owner == null ? null : fk.rtable.owner.val;
            return new TableModel.ForeignKeyModel(
                    fkname,
                    rtableOwner,
                    fk.rtable.name.val,
                    columns,
                    rcolumns);
        };
        throw new RuntimeException("unknwon constraint type: " + rp.getClass().toString());
    }

    static TableModel tableModelFromSources(ArrayList<String> l) {
        try {
            // the create table statement must be first!
            String createTabSource = l.get(0);
            Seq s = Scanner.scanToSeq(createTabSource);
            spinat.plsqlparser.Parser p = new spinat.plsqlparser.Parser();
            Res<spinat.plsqlparser.Ast.CreateTable> r = p.pCreateTable.pa(s);
            if (r == null) {
                System.out.println(createTabSource);
            }
            String tableName = r.v.name.name.val;
            String tableComment = null;
            Map<String, String> columnComments = new HashMap<>();
            ArrayList<TableModel.IndexModel> indexes = new ArrayList<>();
            ArrayList<TableModel.ConstraintModel> consModels = new ArrayList<>();
            TableModel.PrimaryKeyModel primaryKey = null;

            for (int i = 1; i < l.size(); i++) {
                Seq sx = Scanner.scanToSeq(l.get(i));
                Res<Ast.CommentOnTable> ct = p.pCommentOnTable.pa(sx);
                if (ct != null) {
                    tableComment = ct.v.comment.val;
                    continue;
                }
                Res<Ast.CommentOnColumn> cc = p.pCommentOnColumn.pa(sx);
                if (cc != null) {
                    columnComments.put(cc.v.column.val, cc.v.comment.val);
                    continue;
                }
                Res<Ast.CreateIndex> ci = p.pCreateIndex.pa(sx);
                if (ci != null) {
                    String indexName = ci.v.indexName.name.val;
                    boolean unique = ci.v.unique;
                    ArrayList<String> columns = new ArrayList<>();
                    for (Ast.Expression e : ci.v.columns) {
                        String se = l.get(i).substring(e.getStart(), e.getEnd());
                        columns.add(AstHelper.toCanonicalString(se));
                    }
                    indexes.add(new TableModel.IndexModel(indexName, columns, unique));
                    continue;
                }
                Res<Ast.AlterTableAddConstraint> rc = p.pAlterTableAddConstraint.pa(sx);
                if (rc != null) {
                    TableModel.ConstraintModel cm = astConstraintToModelConstraint(l.get(i), rc.v.constraintDefinition);
                    if (cm instanceof TableModel.PrimaryKeyModel) {
                        if (primaryKey == null) {
                            primaryKey = (TableModel.PrimaryKeyModel) cm;
                        } else {
                            System.err.println("defined primary key on table again: " + tableName);
                        }
                    } else {
                        consModels.add(cm);
                    }
                    continue;
                }
            }

            ArrayList<TableModel.ColumnModel> cms = new ArrayList<>();
            for (RelationalProperty rp : r.v.relationalProperties) {

                if (rp instanceof Ast.ConstraintDefinition) {
                    TableModel.ConstraintModel cm = astConstraintToModelConstraint(createTabSource, (Ast.ConstraintDefinition) rp);
                    if (cm instanceof TableModel.PrimaryKeyModel) {
                        if (primaryKey != null) {
                            System.err.println("primary key for table already given: " + tableName);
                        } else {
                            primaryKey = (TableModel.PrimaryKeyModel) cm;
                        }
                    } else {
                        consModels.add(cm);
                    }
                }

                if (rp instanceof Ast.ColumnDefinition) {
                    Ast.ColumnDefinition cd = (Ast.ColumnDefinition) rp;
                    String columnName = cd.name.val;
                    boolean nullable = cd.nullable;
                    String typeName = dataTypeToString(cd.datatype);
                    TableModel.ColumnModel cm = new TableModel.ColumnModel(columnName, typeName, nullable, columnComments.get(columnName));
                    cms.add(cm);
                }
            }
            final TableModel.ExternalTableData extTableData;
            Ast.OrganizationExternal e = r.v.organisationExternal;
            if (e == null) {
                extTableData = null;
            } else {
                extTableData = new TableModel.ExternalTableData(e.defaultDirectory.val, e.type, e.location.stream().map(x -> x.val).collect(Collectors.toList()));

            }

            return new TableModel(tableName,
                    r.v.temporary,
                    r.v.onCommitRows.equals(Ast.OnCommitRows.PRESERVE),
                    cms,
                    consModels,
                    primaryKey,
                    tableComment,
                    indexes,
                    extTableData);

        } catch (Exception e) {
            System.err.println(e);
            return null;
        }
    }

    static SourceRepo loadSource(Path filePath, Path baseDir, Charset charset) throws Exception {
        Map<String, ArrayList<String>> tableSources = new HashMap<>();

        Parser p = new Parser();
        SqlPlus session = new SqlPlus(filePath, baseDir, charset);
        ArrayList<Snippet> l = session.process();
        SourceRepo repo = new SourceRepo();
        for (Snippet sn : l) {
            switch (sn.what) {
                case CODE: {
                    CodeInfo ci = SqlPlus.analyzeCode(sn.text);
                    DBObject dbo = new DBObject(ci.what, ci.name);
                    if (repo.exists(dbo)) {
                        System.err.println("warning, object already in repo:" + dbo);
                    }
                    repo.add(dbo, ci.text);
                }
                break;
                case CREATE_TABLE: {
                    // tables are different, their definition might be
                    // distributed over several statements
                    CodeInfo ci = SqlPlus.analyzeCreateTable(sn.text);
                    System.out.println("doing table " + ci.name);
                    if (tableSources.containsKey(ci.name)) {
                        System.out.println("error tablesSourcres for table not empty for: " + ci.name);
                    } else {
                        tableSources.put(ci.name, new ArrayList<>());
                        tableSources.get(ci.name).add(sn.text);
                    }
                }
                break;
                case CREATE_INDEX: {
                    Seq s = Scanner.scanToSeq(sn.text);

                    Res<Ast.CreateIndex> ri = p.pCreateIndex.pa(s);
                    if (ri != null) {
                        String tableName = ri.v.tableName.name.val;
                        tableSources.get(tableName).add(sn.text);
                    }
                    break;
                }
                case OTHER: {
                    Seq s = Scanner.scanToSeq(sn.text);
                    Res<Ast.CommentOnColumn> rc = p.pCommentOnColumn.pa(s);
                    if (rc != null) {
                        String tableName = rc.v.tableName.name.val;
                        tableSources.get(tableName).add(sn.text);
                    } else {
                        Res<Ast.CommentOnTable> rt = p.pCommentOnTable.pa(s);
                        if (rt != null) {
                            String tableName = rt.v.tableName.name.val;
                            tableSources.get(tableName).add(sn.text);
                        }
                    }
                    break;
                }

                case ALTER_TABLE: {
                    Res<Ast.AlterTableAddConstraint> rc = null;
                    try {
                        Seq s = Scanner.scanToSeq(sn.text);
                        rc = p.pAlterTableAddConstraint.pa(s);
                    } catch (Exception ex) {
                        System.out.println(sn.text);
                        System.out.println(ex);

                    }
                    if (rc != null) {
                        String tableName = rc.v.tableName.name.val;
                        tableSources.get(tableName).add(sn.text);
                    }
                    break;
                }
            }
        }

        for (ArrayList<String> tl
                : tableSources.values()) {
            TableModel tm = tableModelFromSources(tl);
            if (tm != null) {
                String canoicalSource = tm.convertToCanonicalString();
                DBObject dbo = new DBObject("TABLE", tm.name);
                repo.add(dbo, canoicalSource);
            }
        }
        return repo;
    }

    static boolean compareRepos(
            String schema,
            SourceRepo repoDisk,
            Path dirDisk,
            SourceRepo repoDB,
            Path dirDB,
            boolean ignoredbonly) throws IOException {
        Set<DBObject> objsDisk = repoDisk.getEntries(); // sort !
        Set<DBObject> objsDB = repoDB.getEntries();
        Set<DBObject> all = new HashSet<>();
        all.addAll(objsDB);
        all.addAll(objsDisk);
        DBObject[] allArray = all.toArray(new DBObject[0]);
        String encoding = System.getProperty("file.encoding");
        java.util.Arrays.sort(allArray, new Comparator<DBObject>() {
            @Override
            public int compare(DBObject o1, DBObject o2) {
                int x = o1.type.compareTo(o2.type);
                if (x == 0) {
                    return o1.name.compareTo(o2.name);
                } else {
                    return x;
                }
            }
        });
        boolean hasDifferences = false;
        for (DBObject dbo : allArray) {
            String fName = dbo.name.toLowerCase().replace("/", "-") + "-" + dbo.type + ".txt";
            if (objsDB.contains(dbo)) {
                String srcDB = repoDB.get(dbo);
                if (objsDisk.contains(dbo)) {
                    String srcDisk = repoDisk.get(dbo);
                    if (!equalCode(dbo.type, srcDisk, srcDB)) {
                        hasDifferences = true;
                        System.out.println(padLeft("different: ", 20, ' ') + padRight(schema, 30, ' ')
                                + " " + padRight(dbo.type, 20, ' ') + " " + dbo.name);
                        Helper.writeTextFilePlatformLineEnd(dirDisk.resolve(fName), srcDisk, encoding);
                        Helper.writeTextFilePlatformLineEnd(dirDB.resolve(fName), srcDB, encoding);
                    }
                } else {
                    // object is not on disc
                    if (!ignoredbonly) {
                        hasDifferences = true;
                        System.out.println(padLeft("missing on Disk: ", 20, ' ') + padRight(schema, 30, ' ')
                                + " " + padRight(dbo.type, 20, ' ') + " " + dbo.name);
                        Helper.writeTextFilePlatformLineEnd(dirDB.resolve(fName), srcDB, encoding);
                    }
                }
            } else {
                hasDifferences = true;
                System.out.println(padLeft("missing in DB: ", 20, ' ') + padRight(schema, 30, ' ')
                        + " " + padRight(dbo.type, 20, ' ') + " " + dbo.name);
                String srcDisk = repoDisk.get(dbo);
                Helper.writeTextFilePlatformLineEnd(dirDisk.resolve(fName), srcDisk, encoding);
            }
        }
        return hasDifferences;
    }
}
