package spinat.oraclescripter;

public class StringMap extends java.util.HashMap {

    public StringMap(String[] init) {
        super();
        if ((init.length / 2) * 2 != init.length) {
            throw new Error("string must have even length");
        }
        for (int i = 0; i < init.length; i = i + 2) {
            this.put(init[i], init[i + 1]);
        }
    }

    public String getString(String s) {
        return (String) get(s);
    }
}
