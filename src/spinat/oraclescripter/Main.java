package spinat.oraclescripter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Map;

public class Main {

    private static void abort(String msg) {
        System.err.println("abort scripting:");
        System.err.println(msg);
        System.exit(1);
    }

    private static ArrayList<DBObject> getDBObjects(Connection c, java.util.Properties p) throws SQLException {
        String objs = Helper.getProp(p, "objects", null);
        String obj_where = Helper.getProp(p, "object-where", null);
        String obj_file = Helper.getProp(p, "object-file", null);

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
        try (PreparedStatement stm = c.prepareStatement("select distinct object_name,object_type\n"
                + "from user_objects \n"
                + "where object_type in ('PACKAGE','PROCEDURE',\n"
                + "'FUNCTION','VIEW','TRIGGER','TYPE')\n"
                + " and " + where_clause
                // views come last, this important
                + " order by object_type,object_name ");
                ResultSet rs = stm.executeQuery()) {
            ArrayList<DBObject> res = new ArrayList<>();
            while (rs.next()) {
                String name = rs.getString(1);
                String type = rs.getString(2);
                res.add(new DBObject(type, name));
            }
            return res;
        }
    }

    static Map<String, String> suffixMap = Helper.mkStringMap(
            new String[]{
                "package_body", "pkb",
                "package_spec", "pks",
                "package", "pkg",
                "type_body", "tbd",
                "type_spec", "tps",
                "type", "typ",
                "procedure", "prc",
                "function", "fun",
                "view", "vw",
                "trigger", "trg"});

    private static Path saveObject(Path baseDir, java.util.Properties props,
            String objectType, String objectName, String src)
            throws IOException {
        String type;
        if (objectType.equals("PACKAGE BODY")) {
            type = "package_body";
        } else if (objectType.equals("PACKAGE SPEC")) {
            type = "package_spec";
        } else if (objectType.equals("TYPE BODY")) {
            type = "type_body";
        } else if (objectType.equals("TYPE SPEC")) {
            type = "type_spec";
        } else {
            type = objectType.toLowerCase();
        }
        String dir = props.getProperty("dir." + type, "");
        String suffix = props.getProperty("suffix." + type, suffixMap.get(type));
        String filename = objectName.toLowerCase() + "." + suffix;
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
        String code2 = true
                ? Helper.stringUnixLineEnd(src)
                : Helper.stringWindowsLineEnd(src);
        System.out.println(" ->" + file);
        try (PrintStream ps = new PrintStream(file.toFile(), "UTF-8")) {
            ps.append(code2);
        }
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
        }
        Helper.deleteDirectoryContents(p);
    }

    static String appendSlash(String s) {
        if (s.endsWith("\n")) {
            return s + "/\n";
        } else {
            return s + "\n/\n";
        }
    }

    public static void main(String[] args) throws SQLException, IOException, ParseException {
        try {
            java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (Exception e) {
            throw new RuntimeException("can not find oracle driver");
        }

        if (args.length < 1) {
            abort("Expecting at least one Argument: the name of the property file");
        }

        String connectionDesc = null;
        if (args.length == 2) {
            connectionDesc = args[1];
        }
        java.util.Properties props = new java.util.Properties();

        {
            Path p = Paths.get(args[0]);
            if (Files.isReadable(p)) {
                try (FileInputStream fi = new FileInputStream(args[0])) {
                    props.load(fi);
                }
            } else {
                abort("can no read property file: " + p);
            }
        }

        Path baseDir = Paths.get(Helper.getProp(props, "directory")).toAbsolutePath();
        boolean usegit = Helper.getPropBool(props, "usegit", false);
        prepareBaseDir(baseDir, usegit);
        // we have the configuration properties and the diretory they are in
        if (connectionDesc == null) {
            connectionDesc = Helper.getProp(props, "connection");
        }
        Connection con = spinat.oraclelogin.OraConnectionDesc.fromString(connectionDesc).getConnection();
        // now get the objects
        ArrayList<DBObject> objects = getDBObjects(con, props);
        ArrayList<Path> allobjects = new ArrayList<>();
        boolean combine_spec_body = Helper.getPropBool(props, "combine_spec_and_body", false);
        SourceCodeGetter scg = new SourceCodeGetter();
        for (DBObject dbo : objects) {
            System.out.println("doing " + dbo.type + " " + dbo.name);
            if (dbo.type.equals("PACKAGE")) {
                if (combine_spec_body) {
                    String s = scg.getCode(con, "PACKAGE", dbo.name);
                    String b = scg.getCode(con, "PACKAGE BODY", dbo.name);
                    if (b != null) {
                        allobjects.add(saveObject(baseDir, props, "PACKAGE", dbo.name, appendSlash(s) + appendSlash(b)));
                    } else {
                        allobjects.add(saveObject(baseDir, props, "PACKAGE", dbo.name, appendSlash(s)));
                    }
                } else {
                    String s = scg.getCode(con, "PACKAGE", dbo.name);
                    allobjects.add(saveObject(baseDir, props, "PACKAGE SPEC", dbo.name, appendSlash(s)));
                    String b = scg.getCode(con, "PACKAGE BODY", dbo.name);
                    if (b != null) {
                        allobjects.add(saveObject(baseDir, props, "PACKAGE BODY", dbo.name, appendSlash(b)));
                    }
                }
            } else if (dbo.type.equals("TYPE")) {
                if (combine_spec_body) {
                    String s = scg.getCode(con, "TYPE", dbo.name);
                    String b = scg.getCode(con, "TYPE BODY", dbo.name);
                    if (b != null) {
                        allobjects.add(saveObject(baseDir, props, "TYPE", dbo.name, appendSlash(s) + appendSlash(b)));
                    } else {
                        allobjects.add(saveObject(baseDir, props, "TYPE", dbo.name, appendSlash(s)));
                    }
                } else {
                    String s = scg.getCode(con, "TYPE", dbo.name);
                    allobjects.add(saveObject(baseDir, props, "TYPE SPEC", dbo.name, appendSlash(s)));
                    String b = scg.getCode(con, "TYPE BODY", dbo.name);
                    if (b != null) {
                        allobjects.add(saveObject(baseDir, props, "TYPE BODY", dbo.name, appendSlash(b)));
                    }
                }
            } else {
                String s = scg.getCode(con, dbo.type, dbo.name);
                allobjects.add(saveObject(baseDir, props, dbo.type, dbo.name, appendSlash(s)));
            }
        }
        boolean private_synonyms = Helper.getPropBool(props, "private-synonyms", true);
        if (private_synonyms) {
            Path p = writePrivateSynonyms(con, baseDir);
            allobjects.add(0, p);
        }
        boolean sequences = Helper.getPropBool(props, "sequences", true);
        if (sequences) {
            Path p = writeSequences(con, baseDir);
            allobjects.add(0, p);
        }

        Path allObjectsPath = baseDir.resolve("all-objects.sql");
        try (PrintStream ps = new PrintStream(allObjectsPath.toFile(), "UTF-8")) {
            for (Path p : allobjects) {
                Path rel = baseDir.relativize(p);
                ps.append("@@").append(rel.toString());
                ps.println();
            }
        }
        if (usegit) {
            GitHelper.AddVersion(baseDir.toFile(), "das war es");
        }
    }

    static Path writePrivateSynonyms(Connection con, Path baseDir)
            throws FileNotFoundException, UnsupportedEncodingException, SQLException {
        Path synPath = baseDir.resolve("private-synonyms.sql");
        try (PrintStream ps = new PrintStream(synPath.toFile(), "UTF-8")) {
            try (Statement stm = con.createStatement();
                    ResultSet rs = stm.executeQuery("select synonym_name,table_owner,table_name,db_link from user_synonyms order by synonym_name")) {
                while (rs.next()) {
                    String s = "create or replace synonym " + rs.getString(1) + " for " + rs.getString(2) + "." + rs.getString(3);
                    String li = rs.getString(4);
                    if (li != null) {
                        s = s + "@" + li;
                    }
                    s = s + ";";
                    ps.println(s);
                }
            }
            return synPath;
        }
    }

    static Path writeSequences(Connection con, Path baseDir)
            throws FileNotFoundException, UnsupportedEncodingException, SQLException {
        Path synPath = baseDir.resolve("sequences.sql");
        try (PrintStream ps = new PrintStream(synPath.toFile(), "UTF-8")) {
            try (Statement stm = con.createStatement();
                    ResultSet rs = stm.executeQuery("select sequence_name,increment_by from user_sequences order by sequence_name")) {
                while (rs.next()) {
                    String s = "create sequence " + rs.getString(1);
                    String incby = rs.getString(2);
                    if (!incby.equals("1")) {
                        s = s + " increment by " + incby;
                    }
                    s = s + ";";
                    ps.println(s);
                }
            }
            return synPath;
        }
    }

}
