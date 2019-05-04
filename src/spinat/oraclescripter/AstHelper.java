package spinat.oraclescripter;

import java.util.List;
import spinat.plsqlparser.Scanner;
import spinat.plsqlparser.Seq;
import spinat.plsqlparser.Token;
import spinat.plsqlparser.TokenType;

public class AstHelper {

    public static String toCanonicalString(List<Token> l) {
        StringBuilder b = new StringBuilder();
        for (Token t : l) {
            if (t.ttype == TokenType.Ident) {
                b.append(t.str.toUpperCase());
            } else {
                b.append(t.str);
            }
            b.append(" ");

        }
        return b.toString().trim();
    }

    public static String toCanonicalString(Seq s) {
        return toCanonicalString(s.toList());
    }

    public static String toCanonicalString(String s) {
        return toCanonicalString(Scanner.scanToSeq(s));
    }

}
