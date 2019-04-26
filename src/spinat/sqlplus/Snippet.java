package spinat.sqlplus;

import java.nio.file.Path;

/**
 *
 * @author roland
 */
public class Snippet {

    public final String what;
    public final String text;
    public final Path filePath;
    public final int lineNo;

    public Snippet(String what, String text, Path filePath, int lineNo) {
        this.what = what;
        this.text = text;
        this.filePath = filePath;
        this.lineNo = lineNo;
    }

    @Override
    public String toString() {
        return "<Snippet " + what + ", file=" + filePath + ", lineno=" + lineNo + " text=" + text.substring(0, Math.min(text.length(), 60)) + ">";
    }

}
