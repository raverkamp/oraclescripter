package spinat.oraclelogin;

import oracle.jdbc.OracleConnection;

public interface ConnectionCheck {
    public String check(OraConnectionDesc desc, OracleConnection con);
}
