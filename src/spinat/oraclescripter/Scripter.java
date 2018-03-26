package spinat.oraclescripter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import oracle.jdbc.OracleConnection;
import spinat.oraclescripter.ConnectionUtil.ConnectionAndDesc;
import spinat.oraclescripter.ConnectionUtil.ObjectCond;
import static spinat.oraclescripter.ConnectionUtil.getConnection;
import static spinat.oraclescripter.ConnectionUtil.getConnectionAndDesc;
import static spinat.oraclescripter.ConnectionUtil.getDBObjects;
import static spinat.oraclescripter.ConnectionUtil.hasDBAViews;
import static spinat.oraclescripter.ConnectionUtil.userExists;

public class Scripter {

    static Map<String, String> suffixMap = Helper.mkStringMap(
            new String[]{
                "package_body", "pkb",
                "package_spec", "pks",
                "package", "pkg",
                "type_body", "tpb",
                "type_spec", "tps",
                "type", "typ",
                "procedure", "prc",
                "function", "fun",
                "view", "vw",
                "trigger", "trg",
                "java_source", "java"});

    private static Path saveObject(Path baseDir, java.util.Properties props,
            String objectType, String objectName, String src)
            throws IOException {
        String encoding = Helper.getProp(props, "encoding", "UTF-8");
        String type;
        if (objectType.equals("PACKAGE BODY")) {
            type = "package_body";
        } else if (objectType.equals("PACKAGE SPEC")) {
            type = "package_spec";
        } else if (objectType.equals("TYPE BODY")) {
            type = "type_body";
        } else if (objectType.equals("TYPE SPEC")) {
            type = "type_spec";
        } else if (objectType.equals("JAVA SOURCE")) {
            type = "java_source";
        } else {
            type = objectType.toLowerCase();
        }
        String dir = props.getProperty("dir." + type, "");
        String suffix = props.getProperty("suffix." + type, suffixMap.get(type));
        String filename = objectName.toLowerCase().replace("/", "-") + "." + suffix;
        final Path fileRelative;
        if (dir.equals("")) {
            fileRelative = Paths.get(filename);
        } else {
            fileRelative = Paths.get(dir, filename);
        }
        {
            final Path realBaseDir;
            if (!dir.equals("")) {
                realBaseDir = baseDir.resolve(dir);
            } else {
                realBaseDir = baseDir;
            }
            if (!Files.exists(realBaseDir)) {
                Files.createDirectories(realBaseDir);
            }
        }

        Path file = baseDir.resolve(fileRelative);
        Helper.writeTextFile(file, src, encoding);
        return file;
    }

    static void prepareBaseDir(Path p, boolean doGit) throws IOException {
        if (!Files.exists(p)) {
            Path pd = Files.createDirectories(p);
            if (doGit) {
                GitHelper.createRepoInDir(pd.toFile());
            }
        } else {
            if (!Files.isDirectory(p)) {
                throw new RuntimeException("this is not a directory: " + p);
            }
            if (doGit) {
                GitHelper.checkForRepoInDir(p.toFile());
            }
        }
        Helper.deleteDirectoryContents(p);
    }

    static String appendSlash(String s) {
        int l = s.length() - 1;
        while (l >= 0 && Character.isWhitespace(s.charAt(l))) {
            l--;
        }
        s = s.substring(0, l + 1);
        return s + "\n/\n";
    }

    static Path writePrivateSynonyms(OracleConnection con, Path baseDir, String encoding, boolean useDBAViews, String owner)
            throws IOException, SQLException {
        Path synPath = baseDir.resolve("private-synonyms.sql");
        StringBuilder b = new StringBuilder();
        String synonymView;
        if (useDBAViews) {
            synonymView = "dba_synonyms";
        } else {
            synonymView = "all_synonyms";
        }
        try (PreparedStatement stm = con.prepareStatement("select synonym_name,table_owner,table_name,db_link from " + synonymView + " where owner=? order by synonym_name")) {
            stm.setString(1, owner);
            try (ResultSet rs = stm.executeQuery()) {
                while (rs.next()) {
                    String sname = rs.getString(1);
                    String schema = rs.getString(2);
                    String obj = rs.getString(3);
                    String s = "create or replace synonym "
                            + Helper.maybeOracleQuote(sname) + " for "
                            + Helper.maybeOracleQuote(schema) + "."
                            + Helper.maybeOracleQuote(obj);
                    String li = rs.getString(4);
                    if (li != null) {
                        s = s + "@" + li;
                    }
                    s = s + ";";
                    b.append(s);
                    b.append("\n");
                }
            }
        }
        Helper.writeTextFile(synPath, b.toString(), encoding);
        return synPath;
    }

    static Path writeSequences(Connection con, Path baseDir, String encoding, boolean useDBAViews, String owner)
            throws IOException, SQLException {
        Path synPath = baseDir.resolve("sequences.sql");
        StringBuilder b = new StringBuilder();
        String sequenceView;
        if (useDBAViews) {
            sequenceView = "dba_sequences";
        } else {
            sequenceView = "all_sequences";
        }
        String query = "select sequence_name,increment_by from " + sequenceView + " where sequence_owner=? order by sequence_name";
        try (PreparedStatement stm = con.prepareStatement(query)) {
            stm.setString(1, owner);
            try (ResultSet rs = stm.executeQuery()) {
                while (rs.next()) {
                    String s = "create sequence " + Helper.maybeOracleQuote(rs.getString(1));
                    String incby = rs.getString(2);
                    if (!incby.equals("1")) {
                        s = s + " increment by " + incby;
                    }
                    s = s + ";";
                    b.append(s);
                    b.append("\n");
                }
            }
            Helper.writeTextFile(synPath, b.toString(), encoding);
            return synPath;
        }
    }

    public static void mainx(String[] args) throws Exception {
        try {
            java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (Exception e) {
            Helper.abort("can not initialize Oracle JDBC\n" + e.toString());
        }

        if (args.length < 1) {
            Helper.abort("Expecting at least one Argument: the name of the property file,\n"
                    + "the optional second argument is a connection description");
        }
        Path propertiesPath = Paths.get(args[0]).toAbsolutePath();
        final java.util.Properties props = Helper.loadProperties(propertiesPath);

        Path relBaseDir = Paths.get(Helper.getProp(props, "directory"));
        Path baseDir = propertiesPath.getParent().resolve(relBaseDir);

        final String connectionDesc;
        if (args.length == 2) {
            connectionDesc = args[1];
        } else {
            connectionDesc = Helper.getProp(props, "connection", null);
        }

        final String encoding = Helper.getProp(props, "encoding", "UTF-8");

        boolean usegit = Helper.getPropBool(props, "usegit", false);
        prepareBaseDir(baseDir, usegit);
        String schemas = Helper.getProp(props, "schemas", "");

        if (!schemas.equals("")) {
            String[] schema_list = schemas.split(",");

            if (connectionDesc != null) {
                final ConnectionAndDesc cad = getConnectionAndDesc(connectionDesc);
                try (OracleConnection c = cad.connection) {
                    if (!hasDBAViews(c)) {
                        Helper.abort("if multiple schemas, one connection given, but without access to dba views");
                    }
                    for (String schema : schema_list) {
                        if (!userExists(c, schema.trim().toUpperCase(Locale.ROOT))) {
                            Helper.abort("the user " + schema.trim() + " does not exist.");
                        }
                    }
                } // the conenction is closed

                ExecutorService pool = Executors.newFixedThreadPool(schema_list.length);
                ArrayList<FutureTask<Object>> tasks = new ArrayList<>();
                for (int i = 0; i < schema_list.length; i++) {
                    String schema = schema_list[i].trim();
                    final String owner = schema.toUpperCase(Locale.ROOT);
                    final Path schemaBaseDir = baseDir.resolve(schema.toLowerCase());
                    Path pd = Files.createDirectories(schemaBaseDir);
                    Callable<Object> callable = new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            try (OracleConnection c = cad.connectionDesc.getConnection()) {
                                exportToDir(schemaBaseDir, c, owner, true, props, encoding);
                            }
                            return "";
                        }
                    };
                    FutureTask<Object> t = new FutureTask<>(callable);
                    tasks.add(t);
                    pool.execute(t);
                }
                for (FutureTask<Object> t : tasks) {
                    System.out.print(t.get());
                }
                pool.shutdown();
            } else {
                // in the first step make sure we get all connections
                // only then start scription
                // note that getConnection might ask for a password
                final OracleConnection[] connection_list = new OracleConnection[schema_list.length];
                for (int i = 0; i < schema_list.length; i++) {
                    String schema = schema_list[i].trim();
                    String desc = Helper.getProp(props, schema + ".connection");
                    OracleConnection con = getConnection(desc);
                    connection_list[i] = con;
                }
                ExecutorService pool = Executors.newFixedThreadPool(schema_list.length);
                ArrayList<FutureTask<Object>> tasks = new ArrayList<>();
                for (int i = 0; i < schema_list.length; i++) {
                    String schema = schema_list[i].trim();
                    final String owner = schema.toUpperCase(Locale.ROOT);
                    final Path schemaBaseDir = baseDir.resolve(schema.toLowerCase());
                    Path pd = Files.createDirectories(schemaBaseDir);
                    final OracleConnection con = connection_list[i];
                    Callable<Object> callable = new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            // this way the conenction is closed automatically
                            try (OracleConnection c = con) {
                                exportToDir(schemaBaseDir, c, owner, false, props, encoding);
                            }
                            return "";
                        }
                    };
                    FutureTask<Object> t = new FutureTask<>(callable);
                    tasks.add(t);
                    pool.execute(t);
                }
                for (FutureTask<Object> t : tasks) {
                    // return value is "" , avoid warning that value is not used
                    System.out.print(t.get());
                }
                pool.shutdown();

            }
        } else {

            // we have the configuration properties and the diretory they are in
            if (connectionDesc == null) {
                Helper.abort("no connection descriptor found");
            }
            try (OracleConnection con = getConnection(connectionDesc)) {
                exportToDir(baseDir, con, con.getUserName(), false, props, encoding);
            }
        }
        if (usegit) {
            System.out.println("-------------------");
            GitHelper.AddVersion(baseDir.toFile(), "das war es");
        }
    }

    static void exportToDir(
            Path baseDir,
            OracleConnection con,
            String owner,
            boolean useDBAViews,
            java.util.Properties props,
            String encoding) throws SQLException, IOException {
        // now get the objects
        ObjectCond cond = new ObjectCond();
        cond.objs = Helper.getProp(props, "objects", null);
        cond.obj_where = Helper.getProp(props, "object-where", null);
        cond.obj_file = Helper.getProp(props, "object-file", null);

        ArrayList<DBObject> objects = getDBObjects(con, owner, useDBAViews, cond);
        ArrayList<Path> allobjects = new ArrayList<>();
        boolean combine_spec_body = Helper.getPropBool(props, "combine_spec_and_body", false);
        SourceCodeGetter scg = new SourceCodeGetter(con, owner, useDBAViews);
        scg.load(objects);
        for (DBObject dbo : objects) {
            System.out.println("doing " + dbo.type + " " + owner + "." + dbo.name);
            if (dbo.type.equals("PACKAGE")) {
                if (combine_spec_body) {
                    String s = scg.getCode("PACKAGE", dbo.name);
                    String b = scg.getCode("PACKAGE BODY", dbo.name);
                    if (b != null) {
                        allobjects.add(saveObject(baseDir, props, "PACKAGE", dbo.name, appendSlash(s) + appendSlash(b)));
                    } else {
                        allobjects.add(saveObject(baseDir, props, "PACKAGE", dbo.name, appendSlash(s)));
                    }
                } else {
                    String s = scg.getCode("PACKAGE", dbo.name);
                    allobjects.add(saveObject(baseDir, props, "PACKAGE SPEC", dbo.name, appendSlash(s)));
                    String b = scg.getCode("PACKAGE BODY", dbo.name);
                    if (b != null) {
                        allobjects.add(saveObject(baseDir, props, "PACKAGE BODY", dbo.name, appendSlash(b)));
                    }
                }
            } else if (dbo.type.equals("TYPE")) {
                if (combine_spec_body) {
                    String s = scg.getCode("TYPE", dbo.name);
                    String b = scg.getCode("TYPE BODY", dbo.name);
                    if (b != null) {
                        allobjects.add(saveObject(baseDir, props, "TYPE", dbo.name, appendSlash(s) + appendSlash(b)));
                    } else {
                        allobjects.add(saveObject(baseDir, props, "TYPE", dbo.name, appendSlash(s)));
                    }
                } else {
                    String s = scg.getCode("TYPE", dbo.name);
                    allobjects.add(saveObject(baseDir, props, "TYPE SPEC", dbo.name, appendSlash(s)));
                    String b = scg.getCode("TYPE BODY", dbo.name);
                    if (b != null) {
                        allobjects.add(saveObject(baseDir, props, "TYPE BODY", dbo.name, appendSlash(b)));
                    }
                }
            } else {
                String s = scg.getCode(dbo.type, dbo.name);
                allobjects.add(saveObject(baseDir, props, dbo.type, dbo.name, appendSlash(s)));
            }
        }
        boolean private_synonyms = Helper.getPropBool(props, "private-synonyms", true);
        if (private_synonyms) {
            Path p = writePrivateSynonyms(con, baseDir, encoding, useDBAViews, owner);
            allobjects.add(0, p);
        }
        boolean sequences = Helper.getPropBool(props, "sequences", true);
        if (sequences) {
            Path p = writeSequences(con, baseDir, encoding, useDBAViews, owner);
            allobjects.add(0, p);
        }

        Path allObjectsPath = baseDir.resolve("all-objects.sql");
        StringBuilder b = new StringBuilder();

        for (Path p : allobjects) {
            Path rel = baseDir.relativize(p);
            b.append("@@").append(rel.toString());
            b.append("\n");
        }

        Helper.writeTextFile(allObjectsPath, b.toString(), encoding);
    }

}
