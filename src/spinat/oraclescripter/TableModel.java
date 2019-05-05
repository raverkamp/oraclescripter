package spinat.oraclescripter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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

        protected ConstraintModel(String name) {
            this.name = name;
        }
    }

    public final static class CheckConstraintModel extends ConstraintModel {

        public final String condition;

        public CheckConstraintModel(String name, String condition) {
            super(name);
            this.condition = condition;
        }
    }

    public final static class PrimaryKeyModel extends ConstraintModel {

        public final List<String> columns;

        public PrimaryKeyModel(String name, List<String> columns) {
            super(name);
            this.columns = columns;
        }
    }

    public final static class UniqueKeyModel extends ConstraintModel {

        public final List<String> columns;

        public UniqueKeyModel(String name, List<String> columns) {
            super(name);
            this.columns = columns;
        }
    }

    public final String name;
    public final List<ColumnModel> columns;
    public final boolean temporary;
    public final boolean onCommitPreserve;
    public final List<ConstraintModel> constraints;
    public final PrimaryKeyModel primaryKey;

    public TableModel(String name,
            boolean temporary,
            boolean onCommitPreserve,
            List<ColumnModel> columns,
            List<ConstraintModel> constraints,
            PrimaryKeyModel primaryKey) {
        this.name = name;
        this.columns = columns;
        this.temporary = temporary;
        this.onCommitPreserve = onCommitPreserve;
        if (constraints != null) {
            this.constraints = constraints;
        } else {
            this.constraints = new ArrayList<>();
        }
        this.primaryKey = primaryKey;
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
        if (this.primaryKey != null) {
            b.append(",\n");
            b.append("constraint " + primaryKey.name);
            b.append(" primary key (").append(String.join(", ", this.primaryKey.columns)).append(")");
        }
        ArrayList<ConstraintModel> x = new ArrayList<>();
        x.addAll(this.constraints);
        x.sort((ConstraintModel t, ConstraintModel t1) -> t.name.compareTo(t1.name));
        for (ConstraintModel cm : x) {
            b.append(",\n");
            b.append("constraint " + cm.name);
            if (cm instanceof CheckConstraintModel) {
                b.append(" check (" + ((CheckConstraintModel) cm).condition + ")");
            }
            if (cm instanceof UniqueKeyModel) {
                UniqueKeyModel km = (UniqueKeyModel) cm;
                b.append(" unique (").append(String.join(", ", km.columns)).append(")");
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
