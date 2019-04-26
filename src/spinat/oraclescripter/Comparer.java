package spinat.oraclescripter;

import spinat.sqlplus.SqlPlus;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import oracle.jdbc.OracleConnection;
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
        String connectionDesc = Helper.getProp(props, "connection");
        boolean ignoreDBOnly = Helper.getPropBool(props, "ignoredbonly", false);
        OracleConnection connection = ConnectionUtil.getConnection(connectionDesc);
        if (!ConnectionUtil.hasDBAViews(connection)) {
            Helper.abort("the connection needs access to dba views, the view like dba_objects and etc.");
        }
        String[] schema_list = schemas.split(",");
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
            ArrayList<DBObject> dbObjects = ConnectionUtil.getDBObjects(connection, owner, true, conds);
            SourceCodeGetter sc = new SourceCodeGetter(connection, owner, true);
            sc.load(dbObjects);
            SourceRepo repoDB = sc.getSourceRepo();
            String startFile = Helper.getProp(props, schema + ".start");
            Path startPath = realPath.getParent().resolve(startFile).toAbsolutePath();
            Path baseDir = startPath.getParent();
            SourceRepo repoDisk = loadSource(startPath, baseDir);
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
            System.out.printf("changes are in folder: " + tempDir);
            if (Helper.getProp(props, "usewinmerge", "N").equalsIgnoreCase("Y")) {
                ProcessBuilder pb = new ProcessBuilder(new String[]{"C:\\Program Files (x86)\\WinMerge\\WinMergeU.exe",
                    //"/r",
                    "/wl", "/wr", "/u", "/dl", "DB", "/dr", "DISK",
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

    static SourceRepo loadSource(Path filePath, Path baseDir) throws Exception {
        SqlPlus session = new SqlPlus(filePath, baseDir);
        ArrayList<Snippet> l = session.process();
        SourceRepo repo = new SourceRepo();
        for (Snippet sn : l) {
            if (sn.what.equals("code")) {
                CodeInfo ci = SqlPlus.analyzeCode(sn.text);
                DBObject dbo = new DBObject(ci.what, ci.name);
                if (repo.exists(dbo)) {
                    System.err.println("warning, object already in repo:" + dbo);
                }
                repo.add(dbo, ci.text);
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
