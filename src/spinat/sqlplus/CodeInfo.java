package spinat.sqlplus;

/**
 *
 * @author roland
 */
public class CodeInfo {

    public final String what;
    public final String name;
    public final String schema;
    public final String text;

    public CodeInfo(String what, String name, String schema, String text) {
        this.what = what;
        this.name = name;
        this.schema = schema;
        this.text = text;
    }

}
