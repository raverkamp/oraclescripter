package spinat.oraclescripter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// simple class to dela with ResultSets
public class Record {

    final Object[] vals;
    final Map<String, Integer> fieldMap;

    public Record(Map<String, Integer> fieldMap, List<Object> fields) {
        if (fields.size() != fieldMap.size()) {
            throw new IllegalArgumentException("fieldMap size and fields lenggth must macth");
        }
        this.fieldMap = fieldMap;
        vals = new Object[fields.size()];
    }

    public Record(Map<String, Integer> fieldMap, Object[] fields) {
        if (fields.length != fieldMap.size()) {
            throw new IllegalArgumentException("fieldMap size and fields lenggth must macth");
        }
        this.fieldMap = fieldMap;
        this.vals = new Object[fieldMap.size()];
        System.arraycopy(fields, 0, vals, 0, vals.length);
    }

    public int size() {
        return vals.length;
    }

    private int getPos(String name) {
        Integer pos = fieldMap.get(name);
        if (pos == null) {
            throw new IndexOutOfBoundsException("field does not exist: " + name);
        }
        if (pos < 0 || pos > vals.length) {
            throw new IndexOutOfBoundsException("bad index of field: " + name + " -> " + pos);
        }
        return pos;
    }

    public void set(String name, Object o) {
        int pos = getPos(name);
        vals[pos] = o;
    }

    public Object get(String name) {
        return vals[getPos(name)];
    }

    public String getString(String name) {
        return (String) get(name);
    }

    public Integer getInteger(String name) {
        Object o = get(name);
        if (o == null) {
            return null;
        } else {
            return ((BigDecimal) o).intValueExact();
        }
    }

    static boolean match(String[] fields, Record r1, Record r2) {
        for (String field : fields) {
            Object o = r1.get(field);
            if (o == null) {
                if (r2 != null) {
                    return false;
                }
            } else if (!o.equals(r2.get(field))) {
                return false;
            }
        }
        return true;
    }

    public static ArrayList<ArrayList<Record>> group(ArrayList<Record> l, String[] fields) {
        ArrayList<ArrayList<Record>> res = new ArrayList<>();
        ArrayList<Record> currentGroup = null;
        for (Record r : l) {
            if (currentGroup == null) {
                currentGroup = new ArrayList<>();
                currentGroup.add(r);
                continue;
            }
            if (match(fields, currentGroup.get(0), r)) {
                currentGroup.add(r);
            } else {
                res.add(currentGroup);
                currentGroup = new ArrayList<>();
                currentGroup.add(r);
            }

        }
        if (currentGroup != null) {
            res.add(currentGroup);
        }
        return res;
    }
}
