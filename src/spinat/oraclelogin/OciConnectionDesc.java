package spinat.oraclelogin;

import java.sql.DriverManager;
import java.sql.SQLException;
import oracle.jdbc.OracleConnection;

final class OciConnectionDesc extends OraConnectionDesc {

    String tnsname;

    public OciConnectionDesc(String user, String pwd, String tnsname) {
        this.user = user;
        this.pwd = pwd;
        this.tnsname = tnsname;
    }

    @Override
    public String display() {
        return user + "@" + tnsname;
    }

    @Override
    public OracleConnection getConnection() throws SQLException {
        return (OracleConnection) DriverManager.getConnection("jdbc:oracle:oci:@" + tnsname, user, pwd);
    }

    @Override
    public String getConnectionString() {
        return "jdbc:oracle:oci:@" + tnsname;
    }

    @Override
    public String getFullConnectionString() {
        return "jdbc:oracle:oci:" + this.user + "/" + this.pwd + "@" + this.tnsname;
    }

}
