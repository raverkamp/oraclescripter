package spinat.oraclescripter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleTypes;

public class OraUtil {

    public static ArrayList<Record> resultSetToList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        HashMap<String, Integer> fieldMap = new HashMap<>();

        for (int i = 0; i < md.getColumnCount(); i++) {
            fieldMap.put(md.getColumnName(i + 1), i);

        }
        ArrayList<Record> res = new ArrayList<>();
        while (rs.next()) {
            Object[] vals = new Object[md.getColumnCount()];
            for (int i = 0; i < md.getColumnCount(); i++) {
                int ct = md.getColumnType(i + 1);
                if (ct == Types.VARCHAR || ct == Types.LONGVARCHAR) {
                    vals[i] = rs.getString(i + 1);
                } else if (ct == Types.BIGINT || ct == Types.DECIMAL || ct == Types.NUMERIC || ct == Types.INTEGER) {
                    vals[i] = rs.getBigDecimal(i + 1);
                } else if (ct == OracleTypes.CURSOR) {
                    try (ResultSet rs2 = ((OracleResultSet) rs).getCursor(i + 1)) {
                        vals[i] = resultSetToList(rs2);
                    }
                } else {
                    throw new RuntimeException("type not supported: " + ct
                            + " for column " + md.getColumnName(i + 1));
                }

            }
            res.add(new Record(fieldMap, vals));
        }
        return res;
    }

}
