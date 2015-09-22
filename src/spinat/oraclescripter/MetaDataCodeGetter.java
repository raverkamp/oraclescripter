package spinat.oraclescripter;

import java.sql.*;

public class MetaDataCodeGetter {

    public String getCode(Connection c, String objectType, String objectName, String schema) {
        try {
            if (!objectType.equals("TRIGGER")) {
                PreparedStatement s = c.prepareStatement("select dbms_metadata.get_ddl(?,?,?) from dual");
                s.setString(2, objectName);
                s.setString(1, objectType);
                s.setString(3, schema);

                ResultSet rs = s.executeQuery();
                rs.next();
                String code = rs.getString(1);
                code = Helper.replaceObjectname(Helper.removeLeadingWS(code),
                        objectName, schema);
                rs.close();
                s.close();
                return code;
            } else {
                String code = getTriggerCode(c, objectName);
                return Helper.replaceObjectname(code, objectName, schema);
            }
        } catch (SQLException e) {
            throw new Error(e);
        }
    }

    private static String triggerBlock
            = "declare \n"
            + "x integer; \n"
            + "l_handle integer;\n"
            + "begin\n"
            + "  l_handle:=dBMS_METADATA.OPEN ('TRIGGER');\n"
            + "  dbms_metadata.set_filter(l_handle,'NAME',?);\n"
            + "  x:=DBMS_METADATA.ADD_TRANSFORM (l_handle,'DDL');\n"
            + "  ?:=dbms_metadata.fetch_ddl(l_handle)(1).ddltext;\n"
            + "  dbms_metadata.close(l_handle);\n"
            + " end;\n";

    public String getTriggerCode(Connection c, String triggerName) throws SQLException {
        CallableStatement s = c.prepareCall(triggerBlock);
        s.setString(1, triggerName);
        s.registerOutParameter(2, Types.CLOB);
        s.execute();
        String res = s.getString(2);
        s.close();
        return res;
    }

}
