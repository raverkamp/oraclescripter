package spinat.oraclescripter;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.List;

// the model of a database table
public class TableModel {

    public static class ColumnModel {

        public final String name;
        public final String datatype;
        public final boolean nullable;
        public final String comment;

        public ColumnModel(String name, String datatype, boolean nullable, String comment) {
            this.name = name;
            this.datatype = datatype;
            this.nullable = nullable;
            this.comment = comment;
        }
    }

    private static String columnsToCommaSeparated(List<String> c) {
        return String.join(", ", c.stream().map(x -> Helper.maybeOracleQuote(x)).collect(Collectors.toList()));
    }

    public static abstract class ConstraintModel {

        public final String name;

        protected ConstraintModel(String name) {
            this.name = name;
        }

        public abstract String convertToCanonicalString();
    }

    public final static class CheckConstraintModel extends ConstraintModel {

        public final String condition;

        public CheckConstraintModel(String name, String condition) {
            super(name);
            this.condition = condition;
        }

        @Override
        public String convertToCanonicalString() {
            return "constraint " + Helper.maybeOracleQuote(this.name) + " check (" + this.condition + ")";
        }
    }

    public final static class PrimaryKeyModel extends ConstraintModel {

        public final List<String> columns;

        public PrimaryKeyModel(String name, List<String> columns) {
            super(name);
            this.columns = columns;
        }

        @Override
        public String convertToCanonicalString() {
            return "constraint " + Helper.maybeOracleQuote(this.name)
                    + " primary key (" + columnsToCommaSeparated(this.columns) + ")";
        }
    }

    public final static class UniqueKeyModel extends ConstraintModel {

        public final List<String> columns;

        public UniqueKeyModel(String name, List<String> columns) {
            super(name);
            this.columns = columns;
        }

        @Override
        public String convertToCanonicalString() {

            return "constraint " + Helper.maybeOracleQuote(this.name)
                    + " unique (" + columnsToCommaSeparated(this.columns) + ")";
        }
    }

    public final static class ForeignKeyModel extends ConstraintModel {

        public final List<String> columns;
        public final List<String> rcolumns;
        public final String rowner;
        public final String rtable;

        public ForeignKeyModel(String name, String rowner, String rtable, List<String> columns, List<String> rcolumns) {
            super(name);
            this.rowner = rowner; // if this value is null it means same owner as that of tabel itself
            this.rtable = rtable;
            this.columns = columns;
            this.rcolumns = rcolumns;
        }

        @Override
        public String convertToCanonicalString() {
            StringBuilder b = new StringBuilder();
            b.append("constraint " + Helper.maybeOracleQuote(this.name));
            b.append(" foreign key (").append(columnsToCommaSeparated(this.columns)).append(")");
            b.append(" references ");
            if (this.rowner != null) {
                b.append(Helper.maybeOracleQuote(this.rowner)).append(".");
            }
            b.append(Helper.maybeOracleQuote(this.rtable));
            b.append("(").append(columnsToCommaSeparated(this.rcolumns)).append(")");
            return b.toString();
        }
    }

    public final static class IndexModel {

        public final String name;
        public final List<String> columns;
        public final boolean unique;

        public IndexModel(String name, List<String> columns, boolean unique) {
            this.name = name;
            this.columns = columns;
            this.unique = unique;
        }
    }

    public final static class ExternalTableData {

        public final String directory;
        public final String type;
        public final List<String> locations;

        public ExternalTableData(String directory, String type, List<String> locations) {
            this.directory = directory;
            this.type = type;
            this.locations = locations;
        }
    }

    public final String name;
    public final List<ColumnModel> columns;
    public final boolean temporary;
    public final boolean onCommitPreserve;
    public final List<ConstraintModel> constraints;
    public final PrimaryKeyModel primaryKey;
    public final String comment;
    public final List<IndexModel> indexes;
    public final ExternalTableData externalTableData;

    public TableModel(String name,
            boolean temporary,
            boolean onCommitPreserve,
            List<ColumnModel> columns,
            List<ConstraintModel> constraints,
            PrimaryKeyModel primaryKey,
            String comment,
            List<IndexModel> indexes,
            ExternalTableData externalTableData
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
        this.primaryKey = primaryKey;
        this.comment = comment;
        this.indexes = (indexes == null) ? new ArrayList<IndexModel>() : indexes;
        this.externalTableData = externalTableData;
    }

    private String convertIndexToCanonicalString(IndexModel m) {
        // we can not use columnsToCommaSeparated since for functiojn based indexes
        // the columns might be expresssion these would be quoted
        String colString = String.join(", ", m.columns);
        return "create " + (m.unique ? "unique " : "") + "index "
                + Helper.maybeOracleQuote(m.name)
                + " on "
                + Helper.maybeOracleQuote(this.name)
                + "(" + colString + ");\n";
    }

    public String convertToCanonicalString() {
        StringBuilder b = new StringBuilder();
        b.append("create " + (this.temporary ? "global temporary " : "") + "table " + Helper.maybeOracleQuote(this.name) + "(\n");
        for (int i = 0; i < this.columns.size(); i++) {
            ColumnModel c = this.columns.get(i);
            b.append(Helper.maybeOracleQuote(c.name) + " " + c.datatype);
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
        if (this.externalTableData != null) {
            b.append("\n organization external\n(type oracle_loader default directory "
                    + Helper.maybeOracleQuote(this.externalTableData.directory)
                    + "\nlocation(");
            b.append(String.join(", ", this.externalTableData.locations.stream()
                    .map(y -> "'" + y + "'").collect(Collectors.toList())));
            b.append("))");

        }
        b.append(";\n");
        if (this.comment != null && !this.comment.isEmpty()) {
            b.append("comment on table " + this.name
                    + " is '" + this.comment.replace("'", "''") + "';\n");
        }
        for (ColumnModel cm : this.columns) {
            if (cm.comment != null && !cm.comment.isEmpty()) {
                b.append("comment on column " + Helper.maybeOracleQuote(this.name)
                        + "." + Helper.maybeOracleQuote(cm.name)
                        + " is '" + cm.comment.replace("'", "''") + "';\n");
            }
        }
        
         if (this.primaryKey != null) {
            b.append("alter table " + Helper.maybeOracleQuote(this.name) + " add "); 
            b.append(primaryKey.convertToCanonicalString());
            b.append(";\n");
        }
        ArrayList<ConstraintModel> x = new ArrayList<>();
        x.addAll(this.constraints);
        x.sort((ConstraintModel t, ConstraintModel t1) -> t.name.compareTo(t1.name));
        for (ConstraintModel cm : x) {
            b.append("alter table " + Helper.maybeOracleQuote(this.name) + " add "); 
            b.append(cm.convertToCanonicalString());
            b.append(";\n");
        }
        
        ArrayList<IndexModel> y = new ArrayList<>();
        y.addAll(this.indexes);
        y.sort((IndexModel t, IndexModel t1) -> t.name.compareTo(t1.name));
        
        for (IndexModel m : y) {
            b.append(convertIndexToCanonicalString(m));
        }
        return b.toString();
    }

}
