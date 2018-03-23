package spinat.oraclescripter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SourceRepo {

    private final Map<DBObject, String> map = new HashMap<>();

    public SourceRepo() {
    }

    public boolean exists(DBObject o) {
        return map.containsKey(o);
    }

    public void add(DBObject o, String s) {
        this.map.put(o, s.trim());
    }

    public String get(DBObject o) {
        return this.map.get(o);
    }

    Set<DBObject> getEntries() {
        return this.map.keySet();
    }

}
