package spinat.oraclelogin;

import java.sql.SQLException;
import oracle.jdbc.OracleConnection;
import java.text.ParseException;

/**
 * describes an oracle connection
 *
 * @author roland
 */
public abstract class OraConnectionDesc {

    protected String user;
    protected String pwd;

    /**
     *
     * @return a string representation of the connection description, but
     * without the password
     */
    public abstract String display();

    /**
     * does the connection have a password set?
     *
     * @return true if the connection description has password set
     */
    public boolean hasPwd() {
        return pwd != null;
    }

    /**
     * get the password
     *
     * @return password
     */
    public String getPassword() {
        return pwd;
    }

    /**
     * get the user
     *
     * @return user
     */
    public String getUser() {
        return user;
    }

    /**
     * set the password of the connection description
     *
     * @param pwd the password
     */
    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    /**
     * get the jdbc connection string, no password is build into it some
     * applications use the connection string and user, password seperately
     *
     * @return the connection string
     */
    public abstract String getConnectionString();

    /**
     * get the jdbc connection string, user and pw are included
     *
     * @return the complete connection string
     */
    public abstract String getFullConnectionString();

    /**
     * get a oracle jdbc connection for the connection description
     *
     * @return an oracle jdbc connection
     * @throws SQLException
     */
    public abstract OracleConnection getConnection() throws SQLException;
    // ensure the driver is loaded
    static final oracle.jdbc.driver.OracleDriver d = new oracle.jdbc.driver.OracleDriver();

    /**
     * parse a connection description from a string
     *
     * @param conStr
     * @return connection description
     * @throws ParseException
     */
    public static OraConnectionDesc fromString(String conStr) throws ParseException {
        String errm = "expecting a conenction string in the form \"user[/pwd]@tnsname\""
                    + " or \"user[/pwd]@host:port:service\""
                    + " or \"user[/pwd]@//host:port/service\"";
        final int p = conStr.indexOf("@");
        if (p <= 0) {
            throw new ParseException(errm, 0);
        }
        final String userPart = conStr.substring(0, p);
        final String rest = conStr.substring(p + 1);
        final int p2 = userPart.indexOf("/");
        final String user;
        final String pwd;
        if (p2 <= 0) {
            user = userPart;
            pwd = null;
        } else {
            user = userPart.substring(0, p2);
            pwd = userPart.substring(p2 + 1);
        }

        if (rest.startsWith("//")) {
            // new format scott/tiger@//host:port/db
            final String[] a = rest.substring(2).split(":");
            if (a.length != 2) {
                throw new ParseException(errm, 0);
            }
            String host = a[0];
            String[] b = a[1].split("/");
            if (b.length != 2) {
                throw new ParseException(errm, 0);
            }
            final int port;
            try {
                port = Integer.parseInt(b[0]);
            } catch (java.lang.NumberFormatException ex) {
                throw new ParseException("port must be an integer >0, not: " + a[1], 0);
            }
            String db = b[1];
            return new ThinConnectionDesc(user, pwd, host, port, db);
        } else {
            final int pcolon1 = rest.indexOf(":");
            if (pcolon1 < 0) {
                return new OciConnectionDesc(user, pwd, rest);
            } else {
                final String[] a = rest.split(":");
                if (a.length != 3) {
                    throw new ParseException(errm, 0);
                }
                final int x;
                try {
                    x = Integer.parseInt(a[1]);
                } catch (java.lang.NumberFormatException ex) {
                    throw new ParseException("port must be an integer >0, not: " + a[1], 0);
                }
                return new ThinConnectionDesc(user, pwd, a[0], x, a[2]);
            }
        }
    }
}
