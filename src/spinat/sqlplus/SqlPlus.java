package spinat.sqlplus;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlPlus {

    static Pattern rg_start = Pattern.compile("\\s*(start\\s|(@@)|@)", Pattern.CASE_INSENSITIVE);
    static Pattern rg_create_code
            = Pattern.compile("\\s*create\\s+((procedure)|(package)|(trigger)|(type)|(function))",
                    Pattern.CASE_INSENSITIVE);
    static Pattern rg_create_or_replace_code
            = Pattern.compile("\\s*create\\s+or\\s+replace\\s+((procedure)|(package)|(trigger)|(type)|(function))",
                    Pattern.CASE_INSENSITIVE);
    static Pattern rg_ws = Pattern.compile("\\s+");

    static Pattern rg_create_view
            = Pattern.compile("\\s*create\\s+(force\\s+)?view", Pattern.CASE_INSENSITIVE);
    static Pattern rg_create_or_replace_view
            = Pattern.compile("\\s*create\\s+or\\s+replace\\s+(force\\s+)?view", Pattern.CASE_INSENSITIVE);

    static Pattern rg_create_synonym
            = Pattern.compile("\\s*create\\s+(public\\s+)?synonym", Pattern.CASE_INSENSITIVE);
    static Pattern rg_create_or_replace_synonym
            = Pattern.compile("\\s*create\\s+or\\s+replace\\s+(public\\s+)?synonym", Pattern.CASE_INSENSITIVE);
    static Pattern rg_create_sequence
            = Pattern.compile("\\s*create\\s+sequence", Pattern.CASE_INSENSITIVE);
    static Pattern rg_comment_on = Pattern.compile("\\s*comment\\s+on\\s+", Pattern.CASE_INSENSITIVE);
    static Pattern rg_create_table = Pattern.compile("\\s*create\\s+(global\\s+temporary\\s+)?table\\s+", Pattern.CASE_INSENSITIVE);
    static Pattern rg_create_index = Pattern.compile("\\s*create\\s+(unique\\s+)?index\\s+", Pattern.CASE_INSENSITIVE);
    static Pattern rg_alter_table = Pattern.compile("\\s*alter\\s+table\\s+", Pattern.CASE_INSENSITIVE);

    // CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED bla as ..
    static Pattern rg_java_etc
            = Pattern.compile("\\s*create(\\s+or\\s+replace)?(\\s+and\\s+compile)?\\s+java\\s+source", Pattern.CASE_INSENSITIVE);

    static Pattern rg_string = Pattern.compile("'(''|[^'])*'");
    static Pattern rg_qident = Pattern.compile("\\\"[^\\\"]*\\\"");
    static Pattern rg_ident = Pattern.compile("[a-zA-Z]([a-zA-Z0-9\\$\\#_])*");

    static Pattern rg_ignore = Pattern.compile("([^;/'\"-])|(/[^*])|(-[^-])");

    static Pattern rg_semi = Pattern.compile(";");

    static Pattern rg_star_comment = Pattern.compile("/\\*([^\\*]|(\\*[^/]))*\\*/");
    static Pattern rg_arg1 = Pattern.compile("\"([^\"]|\"\")*\"");
    static Pattern rg_arg2 = Pattern.compile("'([^']|'')*'");
    static Pattern rg_arg3 = Pattern.compile("[^ ]+");

    static Pattern rg_comment = Pattern.compile("\\s*--", Pattern.CASE_INSENSITIVE);

    static Pattern rg_comment_in_sql = Pattern.compile("--[^\\n]*\\n", Pattern.CASE_INSENSITIVE);

    private static class FileFrame {

        public final Path filePath;
        public final BufferedReader reader;
        public int lineNo;

        public FileFrame(Path filePath, BufferedReader reader,
                int lineNo) {
            this.filePath = filePath;
            this.reader = reader;
            this.lineNo = lineNo;
        }

    }

    // check if pattern p matches s starting at pos, return next position after pattern
    static int findPatternEnd(Pattern p, String s, int pos) {
        String s2 = s.substring(pos);
        Matcher m = p.matcher(s2);
        if (m.lookingAt()) {
            return m.end() + pos;
        } else {
            return -1;
        }
    }

    static class String2 {

        public final String s1;
        public final String s2;

        public String2(String s1, String s2) {
            this.s1 = s1;
            this.s2 = s2;
        }
    }

    final Path startFileName;
    final Path baseDir;

    final ArrayList<FileFrame> frames = new ArrayList<>();

    FileFrame openFile(Path path) throws Exception {
        if (Files.exists(path)) {
            BufferedReader reader = new BufferedReader(new FileReader(path.toFile()));
            return new FileFrame(path, reader, 1);
        } else {
            Path path2 = Paths.get(path.toAbsolutePath().toString() + ".sql");
            if (Files.exists(path2)) {
                BufferedReader reader = new BufferedReader(new FileReader(path2.toFile()));
                return new FileFrame(path2, reader, 1);
            }
        }
        throw new Exception("file not found: " + path);
    }

    public SqlPlus(Path filePath, Path baseDir) {
        this.startFileName = filePath;
        this.baseDir = baseDir;
    }

    String readLine() throws IOException {
        // fixme endoffile
        String s = this.frames.get(0).reader.readLine();
        this.frames.get(0).lineNo++;
        return s;
    }

    String readTillSlash(String firstLine) throws Exception {
        StringBuilder b = new StringBuilder(firstLine);
        while (true) {
            String line = this.readLine();
            if (line == null) {
                throw new Exception("unexpected end of file, expecting a '/'");
            }
            if (line.trim().equals("/")) {
                return b.toString();
            }
            b.append("\n").append(line);
        }
    }

    String eatSQL(String firstLine) throws Exception {
        Pattern[] to_skip = new Pattern[]{rg_string, rg_comment_in_sql, rg_qident, rg_ignore};
        // (find the ";"!, but respect comments)
        String res = firstLine;
        int pos = 0;

        while (true) {
            boolean hit = false;
            // rg_star_comment gives a stackoverflow if the the comm3ent is to large
            // we sue another method
            if (res.startsWith("/*", pos)) {
                pos = pos + 2;
                while (true) {
                    int o = res.indexOf("*/", pos);
                    if (o >= 0) {
                        pos = o + 2;
                        hit = true;
                        break;
                    }
                    String nextLine = this.readLine();
                    if (nextLine == null) {
                        throw new Exception("unexpected end in sql");
                    }
                    if (nextLine.trim().equals("/")) {
                        return res;
                    }
                    res = res + "\n" + nextLine;
                }
            } else {
                // the patterns exclude each others
                for (Pattern p : to_skip) {
                    int k = findPatternEnd(p, res, pos);
                    if (k >= 0) {
                        if (k == pos) {
                            throw new Error("bug: regular expression matching empyt string");
                        }
                        pos = k;
                        hit = true;
                        break;
                    }
                }
            }
            if (!hit) {
                if (findPatternEnd(rg_semi, res, pos) >= 0) {
                    return res;
                }
                String nextLine = this.readLine();
                if (nextLine == null) {
                    throw new Exception("unexpected end in sql");
                }
                if (nextLine.trim().equals("/")) {
                    return res;
                }
                res = res + "\n" + nextLine;
            }
        }

    }

    String2 eat() throws IOException, Exception {
        String line = this.readLine();
        if (line == null) {
            return null;
        }
        if (rg_start.matcher(line).lookingAt()) {
            return new String2("start", line);
        }
        if (rg_create_code.matcher(line).lookingAt()
                || rg_create_or_replace_code.matcher(line).lookingAt()) {
            String s = readTillSlash(line);
            return new String2("code", s);
        }
        if (line.trim().isEmpty()) {
            return new String2("empty", line);
        }
        if (line.trim().equals("/")) {
            return new String2("slash", line);
        }
        if (rg_comment.matcher(line).lookingAt()) {
            return new String2("comment", line);
        }

        if (rg_create_view.matcher(line).lookingAt()
                || rg_create_or_replace_view.matcher(line).lookingAt()) {
            String s = this.eatSQL(line);
            return new String2("code", s);
        }

        if (rg_create_table.matcher(line).lookingAt()) {
            String s = this.eatSQL(line);
            return new String2("create-table", s);
        }

        if (rg_create_index.matcher(line).lookingAt()) {
            String s = this.eatSQL(line);
            return new String2("create-index", s);
        }
        if (rg_alter_table.matcher(line).lookingAt()) {
            String s = this.eatSQL(line);
            return new String2("alter-table", s);
        }

        if (rg_create_or_replace_synonym.matcher(line).lookingAt()
                || rg_create_synonym.matcher(line).lookingAt()
                || rg_create_sequence.matcher(line).lookingAt()
                || rg_comment_on.matcher(line).lookingAt()) {

            String s = this.eatSQL(line);
            return new String2("other", s);
        }
        if (rg_java_etc.matcher(line).lookingAt()) {
            String s = this.readTillSlash(line);
            return new String2("code", s);
        }
        throw new Exception("can not identify line: " + line);
    }

    static class StartDecomp {

        public final String cmd;
        public final String fileName;
        public final ArrayList<String> args;

        public StartDecomp(String cmd, String fileName, ArrayList<String> args) {
            this.cmd = cmd;
            this.fileName = fileName;
            this.args = args;
        }
    }

    public ArrayList<String> decomposeLine(String s, int start) throws Exception {
        ArrayList<String> res = new ArrayList<>();
        s = s.substring(start);
        int pos = start;
        while (true) {
            if (s.length() == 0) {
                return res;
            }
            Matcher m = rg_ws.matcher(s);
            if (m.lookingAt()) {
                s = s.substring(m.end());
                continue;
            }
            m = rg_arg1.matcher(s);
            if (m.lookingAt()) {
                res.add(s.substring(0, m.end()).replace("\"\"", "\""));
                s = s.substring(m.end());
                continue;
            }
            m = rg_arg2.matcher(s);
            if (m.lookingAt()) {
                res.add(s.substring(0, m.end()).replace("''", "'"));
                s = s.substring(m.end());
                continue;
            }
            m = rg_arg3.matcher(s);
            if (m.lookingAt()) {
                res.add(s.substring(0, m.end()));
                s = s.substring(m.end());
                continue;
            }
            throw new Exception("was soll das: " + s);
        }
    }

    StartDecomp decomposeStart(String line) throws Exception {
        line = line.trim();
        Matcher m = rg_start.matcher(line);
        m.lookingAt();
        int e = m.end();
        // for start the regex has the traling whitespace, so trim!
        String cmd = line.substring(0, e).trim();
        int p = e;
        ArrayList<String> args = decomposeLine(line, p);
        String fileName = args.get(0);
        args.remove(0);
        return new StartDecomp(cmd, fileName, args);
    }

    void doStart(String text) throws Exception {
        StartDecomp dc = decomposeStart(text);
        final Path newFilePath;
        if (dc.cmd.compareToIgnoreCase("START") == 0 || dc.cmd.equals("@")) {
            newFilePath = this.baseDir.resolve(dc.fileName).toAbsolutePath();
        } else {

            newFilePath = this.frames.get(0).filePath.getParent().resolve(dc.fileName).toAbsolutePath();
        }
        FileFrame ff = this.openFile(newFilePath);
        this.frames.add(0, ff);

    }

    void doDefine(String text) {
    }

    void doSet(String text) {
    }

    void closeCurrent() throws IOException {
        this.frames.get(0).reader.close();
        this.frames.remove(0);
    }

    public ArrayList<Snippet> process() throws Exception {
        FileFrame ff = this.openFile(this.startFileName);
        this.frames.add(0, ff);
        ArrayList<Snippet> res = new ArrayList<>();
        int currentLineno;
        try {
            while (true) {
                currentLineno = this.frames.get(0).lineNo;
                String2 s2 = this.eat();
                if (s2 == null) {
                    this.closeCurrent();
                    if (this.frames.isEmpty()) {
                        return res;
                    }
                } else {
                    String what = s2.s1;
                    String text = s2.s2;
                    // text = this.expandDefs(text);

                    if (what.equals("start")) {
                        this.doStart(text);
                    } else if (what.equals("define")) {
                        this.doDefine(text);
                    } else if (what.equals("set")) {
                        this.doSet(text);
                    } else {
                        final Snippet.SnippetType stype;
                        switch (what) {
                            case "code":
                                stype = Snippet.SnippetType.CODE;
                                break;
                            case "comment":
                                stype = Snippet.SnippetType.COMMENT;
                                break;
                            case "other":
                                stype = Snippet.SnippetType.OTHER;
                                break;
                            case "slash":
                                stype = Snippet.SnippetType.SLASH;
                                break;
                            case "empty":
                                stype = Snippet.SnippetType.EMPTY;
                                break;
                            case "create-table":
                                stype = Snippet.SnippetType.CREATE_TABLE;
                                break;
                            case "create-index":
                                stype = Snippet.SnippetType.CREATE_INDEX;
                                break;
                            case "alter-table":
                                stype = Snippet.SnippetType.ALTER_TABLE;
                                break;
                            default:
                                throw new RuntimeException("unexpected type of snippet;: " + what);
                        }
                        res.add(new Snippet(stype, text, this.frames.get(0).filePath, currentLineno));
                    }
                }
            }
        } catch (Throwable e) {
            String msg = "failure when parsing at: " + this.frames.get(0).filePath + " at lineno=" + this.frames.get(0).lineNo + ":" + e.getMessage();
            throw new RuntimeException(msg, e);
        }
    }

    static Pattern rg_create_or_replace = Pattern.compile("\\s*create\\s+or\\s+replace\\s+(and\\s+compile\\s+)?", Pattern.CASE_INSENSITIVE);
    static Pattern rg_create = Pattern.compile("\\s*create\\s", Pattern.CASE_INSENSITIVE);

    static String skipCreateEtc(String s) throws Exception {
        {
            Matcher m = rg_create_or_replace.matcher(s);
            if (m.lookingAt()) {
                return s.substring(m.end());
            }
        }
        {
            Matcher m = rg_create.matcher(s);
            if (m.lookingAt()) {
                return s.substring(m.end());
            }
        }

        throw new Exception("no create or replace found");

    }

    static class StringInt {

        public final String s;
        public final int i;

        public StringInt(String s, int i) {
            this.s = s;
            this.i = i;
        }
    }

    static StringInt bestMatch(String s, Map<String, Pattern> m) {
        int len = -1;
        String best = null;
        for (String k : m.keySet()) {
            int x = findPatternEnd(m.get(k), s, 0);
            if (x > len) {
                len = x;
                best = k;
            }
        }
        if (best == null) {
            return null;
        }
        return new StringInt(best, len);
    }

    final static Map<String, Pattern> codes = new HashMap<>();

    static {
        codes.put("TYPE", Pattern.compile("type", Pattern.CASE_INSENSITIVE));
        codes.put("TYPE BODY", Pattern.compile("type\\s+body", Pattern.CASE_INSENSITIVE));
        codes.put("PACKAGE", Pattern.compile("package", Pattern.CASE_INSENSITIVE));
        codes.put("PACKAGE BODY", Pattern.compile("package\\s+body", Pattern.CASE_INSENSITIVE));
        codes.put("FUNCTION", Pattern.compile("function", Pattern.CASE_INSENSITIVE));
        codes.put("PROCEDURE", Pattern.compile("procedure", Pattern.CASE_INSENSITIVE));
        codes.put("TRIGGER", Pattern.compile("trigger", Pattern.CASE_INSENSITIVE));
        codes.put("VIEW", Pattern.compile("view", Pattern.CASE_INSENSITIVE));
        codes.put("FORCE VIEW", Pattern.compile("force\\s+view", Pattern.CASE_INSENSITIVE));
        codes.put("JAVA SOURCE", Pattern.compile("java\\s+source\\s+named", Pattern.CASE_INSENSITIVE));
    }

    private static String stripAsorIs(String s) {
        Pattern asOrIs = Pattern.compile("\\s*(as|is)\\s*", Pattern.CASE_INSENSITIVE);
        Matcher m = asOrIs.matcher(s);
        if (m.lookingAt()) {
            return s.substring(m.end());
        } else {
            return s;
        }
    }

    public static CodeInfo analyzeCreateTable(String text) throws Exception {
        int a = findPatternEnd(rg_create_table, text, 0);
        if (a < 0) {
            throw new Exception("this is not a create table " + text);
        }
        String text2 = text.substring(a).trim();
        int nameEnd = findPatternEnd(rg_ident, text2, 0);
        String name = text2.substring(0, nameEnd);
        CodeInfo ci = new CodeInfo("TABLE", name, null, text);
        return ci;
    }

    public static CodeInfo analyzeCode(String text) throws Exception {

        String prgText = skipCreateEtc(text);
        StringInt si = bestMatch(prgText, codes);
        if (si == null) {
            throw new Exception("can not analyze:" + prgText);
        }
        final String what;
        String rest;
        if (si.s.equals("FORCE VIEW")) {
            what = "VIEW";
            rest = prgText.substring(si.i).trim();
            prgText = prgText.substring("force".length()).trim();
        } else {
            what = si.s;
            rest = prgText.substring(si.i).trim();
        }

        // craete or replac package|procedure is cut off
        // now try to find the name, the name might include the owner and 
        // both identifiers might be quoted, i.e. owner.object or just object
        final String part1; // the first identifier
        int k = findPatternEnd(rg_ident, rest, 0);
        if (k >= 0) {
            part1 = rest.substring(0, k).toUpperCase();
            rest = rest.substring(k);
        } else {
            int k2 = findPatternEnd(rg_qident, rest, 0);
            if (k2 >= 0) {
                part1 = rest.substring(1, k2 - 1);
                rest = rest.substring(k2);
            } else {
                throw new Exception("can not parse ident");
            }
        }
        rest = rest.trim();
        final String part2; // the secon identifierr
        if (rest.startsWith(".")) {
            rest = rest.substring(1);
            int k2 = findPatternEnd(rg_ident, rest, 0);
            if (k2 >= 0) {
                part2 = rest.substring(0, k2).toUpperCase();
                rest = rest.substring(k2);
            } else {
                int k3 = findPatternEnd(rg_qident, rest, 0);
                if (k3 >= 0) {
                    part2 = rest.substring(1, k3 - 1);
                    rest = rest.substring(k3);
                } else {
                    throw new Exception("can not parse ident");
                }
            }
        } else {
            part2 = null;
        }
        if (what.equals("JAVA SOURCE")) {
            prgText = stripAsorIs(rest);
        }
        if (part2 == null) {
            return new CodeInfo(what, part1, null, prgText);
        } else {
            return new CodeInfo(what, part2, part1, prgText);
        }
    }
}
