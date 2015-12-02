package spinat.oraclescripter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Map;

public class Main {

    private static ArrayList<DBObject> getDBObjects(Connection c, java.util.Properties p) throws SQLException {
        String objs = Helper.getProp(p, "objects", null);
        String obj_where = Helper.getProp(p, "object-where", null);
        String obj_file = Helper.getProp(p, "object-file", null);

        if ((objs == null ? 0 : 1) + (obj_where == null ? 0 : 1)
                + (obj_file == null ? 0 : 1) != 1) {
            throw new Error("either property \"objects\", \"object-where\""
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
                + " and " + where_clause);
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
                "package", "pks",
                "type_body", "tbd",
                "type", "tsp",
                "procedure", "prc",
                "function", "fun",
                "view", "vw",
                "trigger", "trg"});

    private static void saveObject(Path baseDir, java.util.Properties props,
            String objectType, String objectName, String src) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        String type;
        if (objectType.equals("PACKAGE BODY")) {
            type = "package_body";
        } else if (objectType.equals("TYPE BODY")) {
            type = "type_body";
        } else {
            type = objectType.toLowerCase();
        }
        String dir = props.getProperty(type + "_dir", "");
        final Path realBaseDir;
        if (!dir.equals("")) {
            realBaseDir = baseDir.resolve(dir);
        } else {
            realBaseDir = baseDir;
        }
        if (!Files.exists(realBaseDir)) {
            Files.createDirectories(realBaseDir);
        }
        String suffix = props.getProperty(type + "_suffix", suffixMap.get(type));
        Path file = realBaseDir.resolve(objectName.toLowerCase() + "." + suffix);
        String code2 = true
                ? Helper.stringUnixLineEnd(src)
                : Helper.stringWindowsLineEnd(src);
        System.out.println(" ->" + file);
        try (PrintStream ps = new PrintStream(file.toFile(), "UTF-8")) {
            ps.append(code2);
        }
    }
    
    static void prepareBaseDir(Path p) throws IOException {
        if (!Files.exists(p)) {
            Path pd = Files.createDirectories(p);
        } else {
            if (!Files.isDirectory(p)) {
                throw new Error("this is not a directory: " + p);
            }
        }
        Helper.deleteDirectoryContents(p);
    }

    public static void main(String[] args) throws SQLException, FileNotFoundException, UnsupportedEncodingException, IOException, ParseException {
        try {
            java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (Exception e) {
            throw new Error("can not find oracle driver");
        }

        if (args.length < 1) {
            throw new Error("Expecting at least one Argument: the name of the property file");
        }

        String connectionDesc = null;
        if (args.length == 2) {
            connectionDesc = args[1];
        }
        java.util.Properties props = new java.util.Properties();

        File f = new File(args[0]);
        try (FileInputStream fi = new FileInputStream(f)) {
            props.load(fi);
        }

        Path baseDir = Paths.get(Helper.getProp(props, "directory")).toAbsolutePath();
        prepareBaseDir(baseDir);
        // we have the configuration properties and the diretory they are in
        if (connectionDesc == null) {
            connectionDesc = Helper.getProp(props, "connection");
        }
        Connection con = spinat.oraclelogin.OraConnectionDesc.fromString(connectionDesc).getConnection();
        // now get the objects
        ArrayList<DBObject> objects = getDBObjects(con, props);
        boolean combine_spec_body = Helper.getPropBool(props, "combine_spec_and_body", false);
        SourceCodeGetter scg = new SourceCodeGetter();
        for (DBObject dbo : objects) {
            System.out.println("doing " + dbo.type + " " + dbo.name);
            if (dbo.type.equals("PACKAGE")) {
                if (combine_spec_body) {
                    String s = scg.getCode(con, "PACKAGE", dbo.name);
                    String b = scg.getCode(con, "PACKAGE BODY", dbo.name);
                    if (b != null) {
                        saveObject(baseDir, props, "PACKAGE", dbo.name, s + "\n/\n" + b + "\n/\n");
                    } else {
                        saveObject(baseDir, props, "PACKAGE", dbo.name, s + "\n/\n");
                    }
                } else {
                    String s = scg.getCode(con, "PACKAGE", dbo.name);
                    saveObject(baseDir, props, "PACKAGE", dbo.name, s + "\n/\n");
                    String b = scg.getCode(con, "PACKAGE BODY", dbo.name);
                    if (b != null) {
                        saveObject(baseDir, props, "PACKAGE BODY", dbo.name, b + "\n/\n");
                    }
                }
            } else if (dbo.type.equals("TYPE")) {
                if (combine_spec_body) {
                    String s = scg.getCode(con, "TYPE", dbo.name);
                    String b = scg.getCode(con, "TYPE BODY", dbo.name);
                    if (b != null) {
                        saveObject(baseDir, props, "TYPE", dbo.name, s + "\n/\n" + b + "\n/\n");
                    } else {
                        saveObject(baseDir, props, "TYPE", dbo.name, s + "\n/\n");
                    }
                } else {
                    String s = scg.getCode(con, "TYPE", dbo.name);
                    saveObject(baseDir, props, "TYPE", dbo.name, s + "\n/\n");
                    String b = scg.getCode(con, "TYPE BODY", dbo.name);
                    if (b != null) {
                        saveObject(baseDir, props, "TYPE BODY", dbo.name, b + "\n/\n");
                    }
                }
            } else {
                String s = scg.getCode(con, dbo.type, dbo.name);
                saveObject(baseDir, props, dbo.type, dbo.name, s + "\n/\n");
            }
        }
    }
}
