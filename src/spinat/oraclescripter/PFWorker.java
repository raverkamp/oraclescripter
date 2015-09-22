package spinat.oraclescripter;

import java.io.*;

public class PFWorker extends FileWorker {

    public PFWorker(String basepath, boolean unixLineEnd) {
        super(basepath, unixLineEnd);
    }

    private static StringMap a = new StringMap(new String[]{"PACKAGE-SPEC", "packages",
        "PACKAGE-BODY", "packagebodies",
        "PROCEDURE", "procedures",
        "FUNCTION", "functions",
        "TRIGGER", "trigger",
        "VIEW", "views",
        "TYPE-SPEC", "types",
        "TYPE-BODY", "typebodies"});

    public String objectToFilename(DBObject o) {
        String x = a.getString(o.kind);
        if (x == null) {
            throw new Error("unknown kind: " + o.kind);
        } else {
            return _basepath + "/" + x + "/" + o.name + ".sql";
        }
    }
}
