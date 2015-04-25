// Make sure that your classpath points to .../zql/classes as well as to the
// location of jython.jar.

import org.python.util.PythonInterpreter;
import org.python.core.*;

import java.sql.SQLException;
import java.util.Vector;
import java.io.*;

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
         * ^ We can do this by reading inputs as strings and then turning the
         * string into an inputStream and initializing a ZqlParser on it as
         * necessary. (This will also allow us to handle "Create" and "Drop"
         * SQL cases, which are simple enough to parse on our own) (also be sure
         * to handle "exit" or "quit")
         */
        System.out.println("Preparing buzhug environment.");
        PySystemState sys = Py.getSystemState();
        sys.path.append(new PyString("/usr/java/Jython/jython.jar"));
        sys.path.append(new PyString("/home/david/CS123/buzhug/buzhug"));
        interpreter.exec("import buzhug");
        interpreter.exec("from buzhug import Base");
        
        // Default to reading from stdin
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String thisLine = "";
        String fullCommand = "";

        if(args.length < 1) {
            System.out.println("Reading SQL from stdin (quit; or exit; to quit)");
        } else {
            try {
                System.out.println("Reading SQL from file " + args[0]);
                in = new BufferedReader(new FileReader(args[0]));
            }
            catch (FileNotFoundException e) {
                System.out.println("File not found!");
                System.exit(1);
            }
        }

        while(thisLine != null) {
            try {
                System.out.print(">> ");
                thisLine = in.readLine();
            }
            catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
                continue;
            }
            int semicolonInd = thisLine.indexOf(';');
            // If there is no semicolon then there is more command to follow
            if (semicolonInd == -1) {
                fullCommand = fullCommand + thisLine.trim() + " ";
            }
            else { //We now have the full command. Process it.
                fullCommand = fullCommand 
                                    + thisLine.substring(0, semicolonInd + 1);
                
                // Get the first word so we can check if it is exit, quit,
                // Create, or Drop. If it is none of these then we will
                // pass the string into a ZqlParser.
                int firstSpace = fullCommand.indexOf(' ');
                // If firstSpace is -1, then there are no spaces. Check that
                // the command is either exit; or quit; and if it isn't,
                // set "firstWord" to be the entire command.
                String firstWord = "";
                if (firstSpace == -1) {
                    if (fullCommand.equalsIgnoreCase("exit;") ||
                        fullCommand.equalsIgnoreCase("quit;")) {
                        System.out.println("Exiting!");
                        System.exit(0);
                    }
                    else {
                        firstWord = fullCommand;
                    }
                }
                
                // If firstWord is not set, then set it appropriately.
                if (firstWord.length() == 0 && firstSpace != -1) {
                    firstWord = fullCommand.substring(0, firstSpace);
                }
                // If the first word is 'Create' or 'Drop', then handle
                // the command. Otherwise, call ZqlParser on it and
                // handle the parsing that way.
                if (firstWord.equalsIgnoreCase("Create")) {
                    callCreate(fullCommand);
                }
                else if (firstWord.equalsIgnoreCase("Drop")) {
                    callDrop(fullCommand);
                }
                else {
                    // Create a ZqlParser from fullCommand
                    try {
                        InputStream stream = new ByteArrayInputStream(
                              fullCommand.getBytes("UTF-8"));
                        ZqlParser p = new ZqlParser(stream);
                        ZStatement st = p.readStatement();
                        
                        if (st instanceof ZInsert) {
                            callInsert((ZInsert)st);
                        }
                        else if (st instanceof ZUpdate) {
                            callUpdate((ZUpdate) st);
                        }
                        else if (st instanceof ZDelete) {
                            callDelete((ZDelete) st);
                        }
                        else if (st instanceof ZQuery) {
                            callSelect((ZQuery) st);
                        }
                        else {
                            System.out.println("Command unsupported by buzhug:"
                                                + "\n" + fullCommand);
                        }
                    }
                    catch(Exception e) {
                        System.out.println("\nError parsing SQL command:\n" 
                                            + fullCommand);
                        System.out.println("Error Message: " + e.getMessage());
                        // Debugging? Might not need to leave this in the
                        // final project
                        //System.out.println("Stack Trace:");
                        //e.printStackTrace();
                        System.out.println();
                    }
                }
                // We're done processing the current command. Reset the
                // fullCommand string and begin reading the next command.
                fullCommand = "";
            }
            /*
             * TODO: find a way to handle the fact that a buzhug table must
             * be opened within the python environment before DML can be
             * called on it.
             */
        }
    }
    
    private static void callCreate(String st) {
        System.out.println("Create table not yet supported!");
    }
    
    private static void callDrop(String st) {
        System.out.println("Drop table not yet supported!");
    
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
        
        // Debugging purposes
        System.out.println("Formated statement:\n" + buzhugInsert);
        
        // call buzhug.
        interpreter.exec(st.getTable() + " = Base('/home/david/CS123/buzhug/javasrc/tables').open()");
        interpreter.exec(buzhugInsert);
    }
    
    private static void callUpdate(ZUpdate st) {
        // 1. Get a buzhug select statement from the where clause
        // 2. get the associated records within the buzhug environment
        // 3. use those records with buzhug's update method
        System.out.println("Update not yet supported!");
    }
    
    private static void callDelete(ZDelete st) {
        // 1. Get a buzhug select statement from the where clause
        // 2. get the associated records within the buzhug environment
        // 3. use those records with buzhug's delete method
        System.out.println("Delete not yet supported!");
    }
    
    private static void callSelect(ZQuery st) {
        System.out.println("Select not yet supported!");
    }
    
    // Returns the buzhug-formated select statement generated from st.
    // Unlike callSelect, this method will not call buzhug with the generated
    // statement, it will simply return it for use by other methods.
    private static String formatSelect(ZQuery st) {
        System.out.println("formatSelect not yet supported!");
        String buzhugSelect = "";
        return buzhugSelect;
    }
}
