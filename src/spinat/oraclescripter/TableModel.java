package spinat.oraclescripter;

import java.util.ArrayList;

// the model of a database table
public class TableModel {

    public static class ColumnModel {

        public final String name;
        public final String datatype;
        public final boolean nullable;

        public ColumnModel(String name, String datatype, boolean nullable) {
            this.name = name;
            this.datatype = datatype;
            this.nullable = nullable;
        }
    }

    public final String name;
    public final ArrayList<ColumnModel> columns;
    public final boolean temporary;
    public final boolean onCommitPreserve;

    public TableModel(String name,
            boolean temporary,
            boolean onCommitPreserve,
            ArrayList<ColumnModel> columns) {
        this.name = name;
        this.columns = columns;
        this.temporary = temporary;
        this.onCommitPreserve = onCommitPreserve;
    }

    public String ConvertToCanonicalString() {
        StringBuilder b = new StringBuilder();
        b.append("create " + (this.temporary ? "global temporary " : "") + "table " + this.name + "(\n");
        for (int i = 0; i < this.columns.size(); i++) {
            ColumnModel c = this.columns.get(i);
            b.append(c.name + " " + c.datatype);
            if (!c.nullable) {
                b.append(" not null");
            }
            if (i < this.columns.size() - 1) {
                b.append(",\n");
            }
        }
        b.append(")");
        if (this.temporary) {
            if (this.onCommitPreserve) {
                b.append(" on commit preserve rows");
            } else {
                b.append(" on commit delete rows");
            }
        }
        b.append(";\n");
        return b.toString();
    }

}
