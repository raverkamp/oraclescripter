package spinat.oraclescripter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import spinat.oraclescripter.SqlPlus.Snippet;

public class SqlPlusTest {

    public SqlPlusTest() {
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current relative path is: " + s);
    }

    @Test
    public void testOthers() throws Exception {
        SqlPlus sqlplus = new SqlPlus(Paths.get("test/testschema/all.sql"), Paths.get("test/testschema"));
        ArrayList<Snippet> sl = sqlplus.process();
        assertTrue(sl.size() > 0);
    }
}
