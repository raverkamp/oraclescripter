/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package spinat.oraclescripter;

import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTableModel {

    public TestTableModel() {
    }

    @Test
    public void TestConvertToCanonicalString() throws Exception {
        ArrayList<TableModel.ColumnModel> l = new ArrayList<>();
        l.add(new TableModel.ColumnModel("C1", "number", true));
        l.add(new TableModel.ColumnModel("C2", "bla", false));
        l.add(new TableModel.ColumnModel("C3", "xyz", true));
        TableModel m = new TableModel("table1", false, false, l);
        String s = m.ConvertToCanonicalString();
        assertEquals(s, "create table table1(\n"
                + "C1 number,\n"
                + "C2 bla not null,\n"
                + "C3 xyz);\n");

    }
}