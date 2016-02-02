package spinat.oraclescripter;

import java.util.*;
import java.sql.*;

public class SourceCodeGetter {

    private static final String[] source_stuff = new String[]{
        "PACKAGE", "PACKAGE BODY", "TYPE", "TYPE BODY", "FUNCTION", "PROCEDURE"};

    public String getCode(Connection c, String objectType, String objectName) {
        if (Helper.arrayIndexOf(source_stuff, objectType) >= 0) {
            return getUserSourceCode(c, objectName, objectType);
        }
        if ("VIEW".equals(objectType)) {
            return getViewSourceCode(c, objectName);
        }
        if ("TRIGGER".equals(objectType)) {
            return getTriggerSourceCode(c, objectName);
        }
        throw new RuntimeException("unknown kind of object " + objectType);
    }

    private String getUserSourceCode(Connection c, String objectName, String objectType) {
        try {
            String stmtext
                    = "select text from user_source \n"
                    + " where name=? and type=? order by line";

            try (PreparedStatement stm = c.prepareStatement(stmtext)) {
                stm.setString(1, objectName);
                stm.setString(2, objectType);
                StringBuilder b = new StringBuilder();
                try (ResultSet rs = stm.executeQuery()) {
                    while (rs.next()) {
                        b.append(rs.getString(1));
                    }
                    String res = b.toString();
                    if (res == null || res.trim().isEmpty()) {
                        return null;
                    }
                    return "CREATE OR REPLACE " + res;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getViewSourceCode(Connection con, String view) {
        try {
            String stmtext = "select text from user_views where view_name = ?";
            final String code;
            try (PreparedStatement s = con.prepareStatement(stmtext)) {
                s.setString(1, view);
                try (ResultSet rs = s.executeQuery()) {
                    if (rs.next()) {
                        code = rs.getString(1);
                    } else {
                        throw new RuntimeException("view source found");
                    }
                }
            }
            String cols;
            try (PreparedStatement s2 = con.prepareStatement("select column_name from user_tab_columns "
                    + "where table_name = ? order by column_id")) {
                s2.setString(1, view);
                try (ResultSet rs2 = s2.executeQuery()) {
                    cols = "";
                    while (rs2.next()) {
                        String column = rs2.getString(1);
                        cols = cols + ", " + Helper.maybeOracleQuote(column);
                    }
                }
            }
            return "CREATE OR REPLACE FORCE VIEW " + Helper.maybeOracleQuote(view)
                    + " (" + cols.substring(2, cols.length()) + ") as \n"
                    + code;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String default_ref_clause = "REFERENCING NEW AS NEW OLD AS OLD";

    // we search for "user." case insensitive or "\"user\"."  case sensitive
    // if they are found we remove them 
    // the risk of changing comments is ignored
    private String removeUser(String txt, String user) {
        String txtu = txt.toUpperCase(Locale.US);
        String useru = user.toUpperCase(Locale.US);
        int p = txtu.indexOf(useru + ".");
        String txt2;
        if (p < 0) {
            txt2 = txt;
        } else {
            txt2 = txt.substring(0, p)
                    + txt.substring(p + user.length() + 1);
        }

        int p2 = txt2.indexOf("\"" + user + "\".");
        String txt3;
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
                    throw new RuntimeException("Trigger " + trigger_name + ": "
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
                throw new RuntimeException("could not find trigger: " + trigger);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
