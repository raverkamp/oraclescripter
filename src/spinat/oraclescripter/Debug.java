package spinat.oraclescripter;

public class Debug {

    private static java.io.PrintStream _stream = System.out;

    public static void p(String s) {
        _stream.println(s);
    }
}
