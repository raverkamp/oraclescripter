package spinat.oraclescripter;

import java.io.*;

public class Main {

    public static void main(String[] args) {
        try {
            java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (Exception e) {
            throw new Error("can not find oracle driver");
        }

        if (args.length != 1) {
            throw new Error("Expecting one Argument: the name of the property file");
        }
        java.util.Properties p = new java.util.Properties();
        try {
            File f = new File(args[0]);
            FileInputStream fi = new FileInputStream(f);

            p.load(fi);
            fi.close();
        } catch (IOException e) {
            throw new Error("Could not load property file: " + args[0]);
        }
        doWork(p);
    }

    public static void doWork(java.util.Properties prop) {
        String scriptername = Helper.getProp(prop, "scripter-class");
        Class clasz;
        try {
            clasz = Class.forName(scriptername);
        } catch (ClassNotFoundException e) {
            throw new Error(e);
        }
        BaseScripter scripter;
        try {
            scripter = (BaseScripter) clasz.newInstance();
        } catch (InstantiationException e) {
            throw new Error(e);
        } catch (IllegalAccessException e) {
            throw new Error(e);
        }
        scripter.doit(prop);
    }
}
