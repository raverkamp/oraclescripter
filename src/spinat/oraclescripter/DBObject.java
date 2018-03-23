package spinat.oraclescripter;

public final class DBObject {

    public final String type;
    public final String name;

    public DBObject(String type, String name) {
        if (name == null || type == null) {
            throw new NullPointerException("name or type is null");
        }
        this.type = type;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof DBObject) {
            DBObject o2 = (DBObject) o;
            return this.name.equals(o2.name) && this.type.equals(o2.type);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // xor components
        return this.name.hashCode() ^ this.type.hashCode();
    }

    @Override
    public String toString() {
        return "<DBObject name=" + this.name + ", type=" + this.type + ">";
    }

}
