package spinat.oraclescripter;


public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length >=1 && args[0].compareToIgnoreCase("compare")==0) {
            Comparer.mainx(args);
            return;
        }
        Scripter.mainx(args);
    }
}