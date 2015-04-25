// Make sure that your classpath points to .../zql/classes as well as to the
// location of jython.jar.

import org.python.util.PythonInterpreter;
import org.python.core.*;

import java.sql.SQLException;
import java.util.Vector;
import java.io.*;

import java.util.*;
import org.gibello.zql.*;
import org.gibello.zql.data.*;

public class SqlParser {
    private static PythonInterpreter interpreter = new PythonInterpreter(null, new PySystemState());
    //interp = new PythonInterpreter(null, new PySystemState());

    
    public static void main(String args[]) {
        /* If the user inputs an incorrect SQL statement, st = p.readStatement()
         * will throw an exception.
         * TODO: find a way to gracefully handle the exception instead of
         * crashing.
         */
        try {
            System.out.println("Preparing buzhug environment.");
            PySystemState sys = Py.getSystemState();
//            sys.path.append(new PyString("/Users/saziz/jython2.7rc3/jython.jar"));
//            sys.path.append(new PyString("/Users/saziz/Desktop/cs123/buzhug/buzhug"));
//            System.out.println(sys);

            sys.path.append(new PyString("/usr/java/Jython/jython.jar"));
            sys.path.append(new PyString("/home/david/CS123/buzhug/buzhug"));

            interpreter.exec("import sys");
            interpreter.exec("print sys.version_info");

            interpreter.exec("import buzhug");
            interpreter.exec("from buzhug import Base");
            ZqlParser p = null;
            ZStatement st;

            if(args.length < 1) {
                System.out.println("Reading SQL from stdin (quit; or exit; to quit)");
                p = new ZqlParser(System.in);
            } else {
                p = new ZqlParser(new DataInputStream(new FileInputStream(args[0])));
            }

            while((st = p.readStatement()) != null) {
                /*
                 * TODO: implement some way to parse Create and Drop commands,
                 * as Zql doesn't see to be able to handle DDL
                 */
                /*
                 * TODO: find a way to handle the fact that a buzhug table must
                 * be opened within the python environment before DML can be
                 * called on it.
                 */
                if (st instanceof ZInsert) {
                    callInsert((ZInsert)st);
                }
                else if (st instanceof ZUpdate) {
                    callUpdate((ZUpdate) st);
                    //System.out.println("Update not yet supported!");
                    // TODO: call buzhug update with the parsed inputs
                }
                else if (st instanceof ZDelete) {
                    System.out.println("Delete not yet supported!");
                    // TODO: call buzhug delete with the parsed inputs
                }
                else if (st instanceof ZQuery) {
                    callSelect((ZQuery)st);
//                    System.out.println("Select not yet supported!");
                    // TODO: call buzhug select with the parsed inputs
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void callInsert(ZInsert st) {
        Vector columns = st.getColumns();
        Vector values = st.getValues();
        String buzhugInsert = st.getTable();
        buzhugInsert = buzhugInsert + ".insert(";
        
        // If the columns have not been specified, then proceed as though the
        // insert is a "INSERT INTO [table] values (value1, value2, ...)"
        if (columns == null) {
            // Add every insert value to the insert string
            for (int i = 0; i < values.size(); i++) {
                buzhugInsert = buzhugInsert + values.get(i) + ",";
            }
        }
        else {
            // Check that there are the same number of columns and values; if
            // there is not, print an error message and do not call buzhug.
            if (columns.size() == values.size()) {
                // Add every insert value to the insert string
                for (int i = 0; i < values.size(); i++) {
                    buzhugInsert = buzhugInsert + columns.get(i) + "=" +
                                   values.get(i) + ",";
                }
            }
            else {
                System.out.println("Error: number of columns not equal to " +
                    "number of values.");
            }
        }

        // Get rid of the comma after the final select value
        buzhugInsert = buzhugInsert.substring(0,buzhugInsert.length()-1);
        // Add the final parenthesis
        buzhugInsert = buzhugInsert + ")";
        
        System.out.println("Formated statement:\n" + buzhugInsert);
        
        // call buzhug.
        interpreter.exec(st.getTable() + " = Base('/home/david/CS123/buzhug/javasrc/tables').open()");
        interpreter.exec(buzhugInsert);
    }
    
    private static void callUpdate(ZUpdate st) {
        String table = st.getTable();
        // i think this is how tables are opened? Not sure since I can't execute...
        // loading the table
        interpreter.exec("temp = Base('/home/david/CS123/buzhug/javasrc/tables/'"+table+".db).open()");
        // finding the records to update
        String cmd1 = "recs = temp.select_for_update(None, " + st.getWhere() + ")";
        interpreter.exec(cmd1);
        // updating
        String cmd2 = table + ".update(recs, ";
        HashSet vals = st.getSet();
        for(String col:vals.keys())
        {
            cmd2+=col+"="+vals.get(col).toString()+",";
        }
        cmd2 = cmd2.substring(0, cmd2.length()-1) + ")";
        interpreter.exec(cmd2);
    }
    
    private static void callDelete(ZDelete st) {

    }
    
    private static void callSelect(ZQuery st) {
        // result_set holds the selected
        String cmd = "result_set = ";
        // make sure its a simple select
        if(st.getFrom().contains("join"))
        {
            System.out.println("Only simple joins work");
            return;
        }
        //  its a *(i have to check what's in getSelect() when its a star)
        // can't run at the moment, so im assuming null
        if(st.getSelect() == null)
        {
            cmd += "db.select(None)";
        }
        // if it's not a star, simply go through and find the columns
        // we're looking for
        else
        {
            Vector columns = st.getSelect();
            String values = "[";
            for (int i = 0; i < columns.size(); i++) {
                values = values + columns.get(i) + ",";
            }
            values = values.substring(0, values.length()-1);
            values += ']';
            cmd += "db.select("+values + ")";
        }
        // if there is a where, we add it
        if(st.getWhere() != null)
        {
            cmd = cmd.substring(0, cmd.length() - 1);
            cmd += "," + st.getWhere().toString()+")";
        }
        // run on python
        interpreter.exec("db = Base('/home/david/CS123/buzhug/javasrc/tables/"+st.getFrom().toString()+"').open()");
        interpreter.exec(cmd);
        interpreter.exec("for a in result_set:");
        interpreter.exec("\tprint a");
    }
    
    // Returns the buzhug-formated select statement generated from st.
    // Unlike callSelect, this method will not call buzhug with the generated
    // statement, it will simply return it for use by other methods.
    private static String formatSelect(ZQuery st) {
        return "";
    }
}
