package spinat.oraclescripter;

import spinat.sqlplus.SqlPlus;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import spinat.sqlplus.Snippet;

public class SqlPlusTest {

    public SqlPlusTest() {
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current relative path is: " + s);
    }

    @Test
    public void testInstall() throws Exception {
        SqlPlus sqlplus = new SqlPlus(Paths.get("test/testschema/all.sql"), Paths.get("test/testschema"));
        ArrayList<Snippet> sl = sqlplus.process();
        assertTrue(sl.size() > 0);
    }

    static Snippet.SnippetType atLine(ArrayList<Snippet> sl, int lineNo) {
        for (Snippet s : sl) {
            if (s.lineNo == lineNo) {
                return s.what;
            }
        }
        return null;
    }

    @Test
    public void testOthers() throws Exception {
        SqlPlus sqlplus = new SqlPlus(Paths.get("test/other.sql"), Paths.get("test"));
        ArrayList<Snippet> sl = sqlplus.process();
        assertTrue(sl.size() > 0);
        assertEquals(atLine(sl, 1), Snippet.SnippetType.COMMENT);
        assertEquals(atLine(sl, 2), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 2), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 4), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 6), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 9), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 12), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 15), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 16), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 17), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 19), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 20), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 21), Snippet.SnippetType.EMPTY);
        assertEquals(atLine(sl, 22), Snippet.SnippetType.OTHER);
        assertEquals(atLine(sl, 24), Snippet.SnippetType.CODE);
        assertEquals(atLine(sl, 29), Snippet.SnippetType.CODE);
    }
}
