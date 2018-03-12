package spinat.oraclescripter;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length >= 1 && "|-h|-?|--help|".contains("|" + args[0].toLowerCase() + "|")) {
            InputStream input = (new Main()).getClass().getResourceAsStream("/readme.txt");
            BufferedReader r = new BufferedReader(new InputStreamReader(input));
            while (true) {
                String s = r.readLine();
                if (s == null) {
                    break;
                }
                System.out.println(s);
            }
            return;
        }
        if (args.length >= 1 && args[0].compareToIgnoreCase("compare") == 0) {
            Comparer.mainx(args);
            return;
        }
        Scripter.mainx(args);
    }
}
