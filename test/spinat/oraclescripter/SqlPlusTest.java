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

    static String atLine(ArrayList<Snippet> sl, int lineNo) {
        for (Snippet s : sl) {
            if (s.lineNo == lineNo) {
                return s.what;
            }
        }
        return "nix";
    }

    @Test
    public void testOthers() throws Exception {
        SqlPlus sqlplus = new SqlPlus(Paths.get("test/other.sql"), Paths.get("test"));
        ArrayList<Snippet> sl = sqlplus.process();
        assertTrue(sl.size() > 0);
        assertEquals(atLine(sl, 1), "comment");
        assertEquals(atLine(sl, 2), "other");
        assertEquals(atLine(sl, 2), "other");
        assertEquals(atLine(sl, 4), "other");
        assertEquals(atLine(sl, 6), "other");
        assertEquals(atLine(sl, 9), "other");
        assertEquals(atLine(sl, 12), "other");
        assertEquals(atLine(sl, 15), "other");
        assertEquals(atLine(sl, 16), "other");
        assertEquals(atLine(sl, 17), "other");
        assertEquals(atLine(sl, 19), "other");
        assertEquals(atLine(sl, 20), "other");
        assertEquals(atLine(sl, 21), "empty");
        assertEquals(atLine(sl, 22), "other");
        assertEquals(atLine(sl, 24), "code");
        assertEquals(atLine(sl, 29), "code");
    }
}
