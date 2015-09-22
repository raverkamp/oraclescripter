package spinat.oraclescripter;

import java.sql.*;

public interface CodeGetter {

    public abstract String getCode(Connection c, String objecttype, String objectname, String schema);

}
