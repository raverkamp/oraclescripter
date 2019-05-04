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

    public static class ConstraintModel {

        public final String name;

        ConstraintModel(String name) {
            this.name = name;
        }
    }

    public static class CheckConstraintModel extends ConstraintModel {

        public final String condition;

        CheckConstraintModel(String name, String condition) {
            super(name);
            this.condition = condition;
        }
    }

    public final String name;
    public final ArrayList<ColumnModel> columns;
    public final boolean temporary;
    public final boolean onCommitPreserve;
    public final ArrayList<ConstraintModel> constraints;

    public TableModel(String name,
            boolean temporary,
            boolean onCommitPreserve,
            ArrayList<ColumnModel> columns,
            ArrayList<ConstraintModel> constraints
    ) {
        this.name = name;
        this.columns = columns;
        this.temporary = temporary;
        this.onCommitPreserve = onCommitPreserve;
        if (constraints != null) {
            this.constraints = constraints;
        } else {
            this.constraints = new ArrayList<>();
        }
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
        for (ConstraintModel cm : this.constraints) {
            b.append(",\n");
            b.append("constraint " + cm.name);
            if (cm instanceof CheckConstraintModel) {
                b.append(" check (" + ((CheckConstraintModel) cm).condition + ")");
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
