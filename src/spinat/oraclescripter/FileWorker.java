package spinat.oraclescripter;

import java.io.*;

public abstract class FileWorker implements IObjectWorker {

    String _basepath;
    boolean _unixLineEnd;

    public FileWorker(String basepath, boolean unixLineEnd) {
        //try {
        _basepath = basepath;
        _unixLineEnd = unixLineEnd;
        File f = new File(_basepath);
        System.out.println(f);
        if (!(f.exists() && f.isDirectory())) {
            throw new Error("directory does not exist");
        }
    }

    protected static void ensureDirExists(String filename) {
        try {
            String s = new File(filename).getCanonicalPath();
            int p = s.lastIndexOf("\\");
            String dirname = s.substring(0, p);
            System.out.println(dirname);
            if (!new File(dirname).exists()) {
                boolean b = new File(dirname).mkdirs();
                if (!b) {
                    throw new Error("could not create directory");
                }
            }
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    public abstract String objectToFilename(DBObject o);

    public void work(DBObject o, String code) {
        try {
            String fname = objectToFilename(o);
            ensureDirExists(fname);
            FileWriter w = new FileWriter(fname);
            String code2 = _unixLineEnd
                    ? Helper.stringUnixLineEnd(code)
                    : Helper.stringWindowsLineEnd(code);
            w.write(code2);
            w.close();
            System.out.println(o.kind + " - " + o.name + " =>" + fname);
        } catch (IOException e) {
            throw new Error(e);
        }
    }
}
