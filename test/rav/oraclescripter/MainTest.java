/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package rav.oraclescripter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rav
 */
public class MainTest {

   static void ensureDir(String name) {
       if (!name.toLowerCase().startsWith("c:/temp/")) {
           throw new Error("tot");
       }
       File f = new File(name);
       if (f.exists()) {
       f.delete();
       }
       f.mkdirs();
   }

    /**
     * Test of main method, of class Main.
     */
    @Test
    public void testMain() throws FileNotFoundException, IOException {
        System.out.println("main");
        Properties p = new Properties();
        p.load(new FileInputStream("A:/JAVA/OracleScripter/test/testscott.properties"));
        String dir = p.getProperty("directory");
        ensureDir(dir);
        spinat.oraclescripter.Main.main(new String[]{"A:/JAVA/OracleScripter/test/testscott.properties"});
        // TODO review the generated test code and remove the default call to fail.
    }

    

}