package spinat.oraclescripter;

import java.util.*;
import java.sql.*;

public class SourceCodeGetter implements CodeGetter {

    private static final String[] source_stuff = new String[]{
        "PACKAGE", "PACKAGE BODY", "TYPE", "TYPE BODY", "FUNCTION", "PROCEDURE"};

    @Override
    public String getCode(Connection c, String objectType, String objectName, String schema) {
        if (Helper.arrayIndexOf(source_stuff, objectType) >= 0) {
            return getUserSourceCode(c, objectType, objectName);
        }
        if ("VIEW".equals(objectType)) {
            return getViewSourceCode(c, objectName);
        }
        if ("TRIGGER".equals(objectType)) {
            return getTriggerSourceCode(c, objectName);
        }
        throw new Error("unknown kind of object " + objectType);
    }

    private String getUserSourceCode(Connection c, String objectType, String objectName) {
        try {
            String stmtext
                    = "declare a clob;\n"
                    + "begin\n"
                    + "  dbms_lob.createtemporary(a,true);\n"
                    + "  for r in (select * from user_source \n"
                    + " where name=? and type=? order by line) loop\n"
                    + "      dbms_lob.append(a,r.text);\n"
                    + "  end loop;\n"
                    + "  ?:=a;\n"
                    + "end;\n";
            CallableStatement s = c.prepareCall(stmtext);
            s.setString(1, objectName);
            s.setString(2, objectType);
            s.registerOutParameter(3, java.sql.Types.CLOB);
            boolean x = s.execute();
            Clob clob = s.getClob(3);
            String res = clob.getSubString(1, (int) clob.length());
            s.close();
            // eigentlich sollte das free durchgehen
            // aber es gibt einen AbstractMethodError ..
            // clob.free();
            return "CREATE OR REPLACE " + res;
        } catch (SQLException e) {
            throw new Error(e);
        }
    }

    private String getViewSourceCode(Connection con, String view) {
        try {
            String stmtext = "select text from user_views where view_name = ?";
            PreparedStatement s = con.prepareStatement(stmtext);
            s.setString(1, view);
            ResultSet rs = s.executeQuery();
            rs.next();
            String code = rs.getString(1);
            rs.close();
            s.close();
            PreparedStatement s2 = con.prepareStatement("select column_name from user_tab_columns "
                    + "where table_name = ? order by column_id");
            s2.setString(1, view);
            ResultSet rs2 = s2.executeQuery();
            String cols = "";
            while (rs2.next()) {
                String column = rs2.getString(1);
                cols = cols + ", " + Helper.maybeOracleQuote(column);
            }
            rs2.close();
            s2.close();
            return "CREATE OR REPLACE VIEW " + Helper.maybeOracleQuote(view)
                    + " (" + cols.substring(2, cols.length()) + ") as \n"
                    + code;
        } catch (SQLException e) {
            throw new Error(e);
        }
    }

    private static String default_ref_clause = "REFERENCING NEW AS NEW OLD AS OLD";

    // we search for "user." case insensitive or "\"user\"."  case sensitive
    // if they are found we remove them 
    // the risk of changing comments is ignored
    private String removeUser(String txt, String user) {
        String txtu = txt.toUpperCase(Locale.US);
        String useru = user.toUpperCase(Locale.US);
        int p = txtu.indexOf(useru + ".");
        String txt2 = null;
        if (p < 0) {
            txt2 = txt;
        } else {
            txt2 = txt.substring(0, p)
                    + txt.substring(p + user.length() + 1);
        }

        int p2 = txt2.indexOf("\"" + user + "\".");
        String txt3 = null;
        if (p2 < 0) {
            txt3 = txt2;
        } else {
            txt3 = txt2.substring(0, p2)
                    + txt2.substring(p2 + user.length() + 3);
        }
        if (txt3.equals(txt)) {
            return txt;
        } else {
            return removeUser(txt3, user);
        }
    }

    private String getTriggerSourceCode(Connection con, String trigger) {
        try {
            PreparedStatement s = con.prepareStatement("select "
                    + "TRIGGER_NAME,\n"
                    + "TRIGGER_TYPE,\n"
                    + "TRIGGERING_EVENT,\n"
                    + "TABLE_OWNER,\n"
                    + "BASE_OBJECT_TYPE,\n"
                    + "TABLE_NAME,\n"
                    + "COLUMN_NAME,\n"
                    + "REFERENCING_NAMES,\n"
                    + "WHEN_CLAUSE,\n"
                    + "STATUS,\n"
                    + "DESCRIPTION,\n"
                    + "ACTION_TYPE,\n"
                    + "TRIGGER_BODY,\n"
                    + "user\n"
                    + " from user_triggers where trigger_name = ?");
            s.setString(1, trigger);
            ResultSet rs = s.executeQuery();

            if (rs.next()) {
                String trigger_name = rs.getString(1);
                String trigger_type = rs.getString(2);
                String trigger_event = rs.getString(3);
                String table_owner = rs.getString(4);
                String table_name = rs.getString(5);
                String column_name = rs.getString(7);
                String referencing_names = rs.getString(8);
                String when_clause = rs.getString(9);
                String status = rs.getString(10);
                String description = rs.getString(11);
                String action_type = rs.getString(12);
                String trigger_body = rs.getString(13);
                String user = rs.getString(14);

              // column_name != null is not implemented,
                // I think this can be used with inline tables
                if (column_name != null) {
                    throw new Error("Trigger " + trigger_name + ": "
                            + " column_name in user_triggers is not null");
                }

                String desc2 = removeUser(description, user);

                rs.close();
                s.close();
                String res = "CREATE OR REPLACE TRIGGER " + desc2
                        + ((when_clause == null) ? "" : " WHEN (" + when_clause + ")\n")
                        + trigger_body;
                return res;
            } else {
                throw new Error("nicht gefunden");
            }
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
