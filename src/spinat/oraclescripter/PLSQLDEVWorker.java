package spinat.oraclescripter;

import java.io.*;

public class PLSQLDEVWorker extends FileWorker {

    public PLSQLDEVWorker(String basepath, boolean unixLineEnd) {
        super(basepath, unixLineEnd);
    }

    private static StringMap a = new StringMap(new String[]{"PACKAGE-SPEC", "spc",
        "PACKAGE-BODY", "bdy",
        "PROCEDURE", "prc",
        "FUNCTION", "fun",
        "TRIGGER", "trg",
        "VIEW", "vw",
        "TYPE-SPEC", "tps",
        "TYPE-BODY", "tpb"});

    public String objectToFilename(DBObject o) {
        String x = a.getString(o.kind);
        if (x == null) {
            throw new Error("unknown kind: " + o.kind);
        } else {
            return _basepath + "/" + o.name + "." + x;
        }
    }
}
