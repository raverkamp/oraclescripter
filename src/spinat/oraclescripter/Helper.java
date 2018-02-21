package spinat.oraclescripter;

import java.util.*;
import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Helper {

    // der text sollte mit Whitespace anfangen
    public static String removeLeadingWS(String s) {
        int i = 0;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return s.substring(i, s.length());
    }

    // wir suchen den user namen und objectnamen:
    public static String replaceObjectname(String s, String name, String user) {
        String toFind = "\"" + user + "\".\"" + name + "\"";
        //String nl = new Character(10).toString();
        int p1 = s.indexOf(toFind);
        return s.substring(0, p1) + "\"" + name + "\""
                + s.substring(p1 + toFind.length(), s.length());
    }

    public static String arrayToInList(ArrayList<String> a) {
        StringBuilder b = new StringBuilder();
        b.append("('doofer trick fuer leere Liste'");
        for (String a1 : a) {
            b.append(",'");
            b.append(stringReplace(a1, "'", "''"));
            b.append("'");
        }
        b.append(")");
        return b.toString();
    }

    public static ArrayList<String> objectsToArrayList(String s) {
        String[] objs = s.split(",", -1);
        ArrayList<String> res = new ArrayList<>();
        for (int i = 0; i < objs.length; i++) {
            String o = objs[i].trim();
            if (o.length() > 0) {
                if (o.charAt(0) == '"') {
                    if (o.charAt(o.length() - 1) == '"') {
                        res.add(o.substring(1, o.length() - 1));
                    } else {
                        throw new RuntimeException("wrong format of objectname: " + o);
                    }
                } else {
                    res.add(o.toUpperCase(Locale.US));
                }
            } else {/* we ignore empty Strings */

            }
        }
        return res;
    }

    public static String getProp(java.util.Properties p, String name) {
        String s = p.getProperty(name);
        if (s == null) {
            throw new RuntimeException("Property " + name + " not found");
        } else {
            return s;
        }
    }

    public static String getProp(java.util.Properties p, String name,
            String default_) {
        return p.getProperty(name, default_);
    }

    public static boolean getPropBool(java.util.Properties p, String name,
            boolean default_) {
        String s = p.getProperty(name);
        if (s == null) {
            return default_;
        }
        if (s.equalsIgnoreCase("true")) {
            return true;
        } else if (s.equalsIgnoreCase("false")) {
            return false;
        } else {
            throw new RuntimeException("Property " + name + " must be 'true' or 'false' but not '" + s + "'");
        }
    }

    public static ArrayList<String> objectsFromFile(String filename) {
        try {
            java.io.FileReader fr = new FileReader(filename);
            java.io.BufferedReader r = new BufferedReader(fr);
            ArrayList<String> res = new ArrayList<>();
            int lineno = 0;
            for (String l = r.readLine(); l != null; l = r.readLine()) {
                lineno++;
                String s = l.trim();
                // four cases 
                // a simple string => upcase
                // a " .." strinng => extract 
                // a # comment => nothing
                // a empty line => nothing
                if (s.length() == 0) {
                    continue;
                }
                if (s.charAt(0) == '#') {
                    continue;
                }
                if (s.charAt(0) == '\"') {
                    int p = s.length();
                    if (s.charAt(p - 1) != '"') {
                        throw new RuntimeException("strange object at line " + lineno);
                    }
                    res.add(s.substring(1, p - 1));
                } else {
                    // normal string
                    res.add(s.toUpperCase(Locale.US));
                }
            }
            r.close();
            fr.close();
            return res;
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String oracleIdentBeginChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String oracleIdentChars = oracleIdentBeginChars
            + "0123456789_$";

    public static boolean oracleIdentOK(String s) {
        if (s.length() == 0) {
            throw new RuntimeException("length of identifier must not be null");
        }
        char fc = s.charAt(0);
        if (oracleIdentBeginChars.indexOf(fc) < 0) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (oracleIdentChars.indexOf(s.charAt(i)) < 0) {
                return false;
            }
        }
        return true;
    }

    public static String maybeOracleQuote(String s) {
        if (oracleIdentOK(s)) {
            return s.toLowerCase();
        } else {
            return "\"" + s + "\"";
        }
    }

    public static int arrayIndexOf(Object[] a, Object o) {
        for (int i = 0; i < a.length; i++) {
            if (o == null && a[i] == null
                    || (o != null && o.equals(a[i]))) {
                return i;
            }
        }
        return -1;
    }

    public static String stringWindowsLineEnd(String s) {
        String s2 = stringReplace(s, Character.toString((char) 13), "");
        return stringReplace(s2, Character.toString((char) 10), Character.toString((char) 13) + Character.toString((char) 10));
    }

    public static String stringUnixLineEnd(String s) {
        return stringReplace(s, Character.toString((char) 13), "");
    }

    public static String stringReplace(String src, String what, String repl) {
        int p = src.indexOf(what);
        if (p < 0) {
            return src;
        } else {
            return src.substring(0, p) + repl
                    + stringReplace(src.substring(p + what.length()), what, repl);
        }
    }

    public static Map<String, String> mkStringMap(String[] init) {
        HashMap<String, String> res = new HashMap<>();
        if ((init.length / 2) * 2 != init.length) {
            throw new RuntimeException("string array must have even length");
        }
        for (int i = 0; i < init.length; i = i + 2) {
            res.put(init[i], init[i + 1]);
        }
        return res;
    }

    static void ensureDirExists(String filename) {
        try {
            String s = new File(filename).getCanonicalPath();
            int p = s.lastIndexOf("\\");
            String dirname = s.substring(0, p);
            if (!new File(dirname).exists()) {
                boolean b = new File(dirname).mkdirs();
                if (!b) {
                    throw new RuntimeException("could not create directory: " + dirname);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static ArrayList<Path> dirList(Path p) throws IOException {
        ArrayList<Path> l = new ArrayList<>();
        // first fetch the directory contents and then delete the contents
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(p)) {
            for (Path entry : stream) {
                l.add(entry);
            }
        }
        return l;
    }
    
    static boolean deleteFile(Path p) {
        try {
            Files.delete(p);
            return true;
        } catch (IOException ex) {
            System.err.printf("can not delete file " + p + " " + ex.toString() + "\n");
            return false;
        }
    }


    static boolean deleteThing(Path p) throws IOException {

        if (Files.isDirectory(p)) {
            boolean a = true;
            for (Path entry : dirList(p)) {
                boolean b = deleteThing(entry);
                a = b && a;
            }
            if (a) {
                return deleteFile(p);

            } else {
                return false;
            }
        } else {
            return deleteFile(p);
        }

    }

    static void deleteDirectoryContents(Path p) {
        try {
            if (!p.isAbsolute()) {
                throw new RuntimeException("not a absolute path");
            }
            if (!Files.isDirectory(p)) {
                throw new RuntimeException("this is not a directory: " + p);
            }
            // try to delete the dicrectory contents
            // we do three runs and sleep 3 sec between
            // WHY: I had problems on Windows, the first script attempt failed
            //   because a file could not be deleted, but the second succeeded
            for (int i = 0; i < 3; i++) {
                boolean a = true;
                for (Path entry : dirList(p)) {
                    String s = entry.getFileName().toString();
                    if (!(s.equalsIgnoreCase("git")
                            || s.equalsIgnoreCase("svn")
                            || s.equalsIgnoreCase("git")
                            || (s.length() > 1 && !Character.isAlphabetic(s.charAt(0))
                            && !Character.isDigit(s.charAt(0))))) {
                        // the order is important: we always want to execute deleteThing!
                        a = deleteThing(entry) && a;
                    }
                }
                if (a) {
                    return;
                } else {
                    Thread.sleep(3000);
                }
            }
            throw new RuntimeException("can not delete directory contents");

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public static java.util.Properties loadProperties(Path p) throws IOException {
        java.util.Properties props = new java.util.Properties();
        {
            if (Files.isReadable(p)) {
                try (FileInputStream fi = new FileInputStream(p.toFile())) {
                    props.load(fi);
                }
            } else {
                abort("cannot read property file: " + p);
            }
        }
        return props;
    }
    
    public static void abort(String msg) {
        System.err.println("abort scripting:");
        System.err.println(msg);
        System.exit(1);
    }
    
    public static void writeTextFile(Path file, String txt, String encoding) throws IOException {
        if (Files.exists(file)) {
            throw new RuntimeException("file " + file + " already exists");
        }
        String s = Helper.stringUnixLineEnd(txt);
        if (encoding.equals("german-ascii")) {
            encoding = "ascii";
            s = s.replace("\u00C4", "Ae").replace("\u00D6", "Oe").replace("\u00DC", "Ue")
                    .replace("\u00E4", "ae").replace("\u00F6", "oe").replace("\u00FC", "ue")
                    .replace("\u00DF", "ss");
        }

        try (PrintStream ps = new PrintStream(file.toFile(), encoding)) {
            ps.append(s);
        }
    }

}
