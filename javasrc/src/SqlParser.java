
import org.python.util.PythonInterpreter;
import org.python.core.*;

import java.sql.SQLException;
import java.util.Vector;
import java.util.ArrayList;
import java.io.*;

import java.util.*;
import org.gibello.zql.*;
import org.gibello.zql.data.*;

public class SqlParser {
    private static String basePath = "tables/";

    private static PythonInterpreter interpreter = new PythonInterpreter(null,
                                                           new PySystemState());

    
    public static void main(String args[]) {
        System.out.println("Preparing buzhug environment...");
        PySystemState sys = Py.getSystemState();
        sys.path.append(new PyString("lib/Jython/jython.jar"));
        sys.path.append(new PyString("lib/buzhug"));
        interpreter.exec("import buzhug");
        interpreter.exec("from buzhug import Base");
        interpreter.exec("import sys");
        interpreter.exec("sys.path.insert(0, '/Users/saziz/Desktop/cs123/buzhug/javasrc/src')");
        interpreter.exec("import Joins");

        // Default to reading from stdin
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String thisLine = "";
        String fullCommand = "";

        if(args.length < 1) {
            System.out.println("Reading SQL from stdin (quit; or exit; to quit)");
            System.out.println("End all complete commands with ';'");
        } 
        else {
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
                // Drop isn't actually supported by buzhug?!
                else if (firstWord.equalsIgnoreCase("Drop")) {
                    callDrop(fullCommand);
                }
                else {
                    // Create a ZqlParser from fullCommand
                    try {
                        InputStream stream = new ByteArrayInputStream(
                                                 fullCommand.getBytes("UTF-8"));
                        ZqlParser p = new ZqlParser(stream);
                        System.out.println("MADE IT TO TRY");

                        ZStatement st = p.readStatement();
                        System.out.println("MADE IT TO TRY");
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
        }
    }
    
    /* Zql does not parse SQL DDL, so we are forced to manually parse a string
     * for table creation. */
    private static void callCreate(String st) {
        // Get the table name
        String tableName = "";
        while (tableName.length() == 0) {
            int nextSpace = st.indexOf(' ');
            if (nextSpace == -1) {
                System.out.println("Unable to create table: table name not"
                                    + " found!");
                return;
            }
            String tmp = st.substring(0, nextSpace);
            st = st.substring(nextSpace + 1);
            st.trim();
            if(!tmp.equalsIgnoreCase("Create") && 
               !tmp.equalsIgnoreCase("Table")) {
                tableName = tmp;
            }
        }
        // Get the opening paren so we know where to start
        int openParen = st.indexOf('(');
        if (openParen == -1) {
            System.out.println("Unable to create table: bad syntax!");
            return;
        }
        st = st.substring(openParen + 1);
        st = st.trim();
        
        // Begin getting the column names and column types
        ArrayList<String> columnNames = new ArrayList<String>();
        ArrayList<String> columnTypes = new ArrayList<String>();
        ArrayList<String> columnDefaults = new ArrayList<String>();
        while (st.length() != 0) {
            // Get the column name
            int spaceLoc = st.indexOf(' ');
            if (spaceLoc == -1) {
                System.out.println("Unable to create table: parsing error!");
                return;
            }
            columnNames.add(st.substring(0, spaceLoc));
            st = st.substring(spaceLoc);
            st = st.trim();
            
            // Get the column type
            String colTypePotential = "";
            int commaLoc = st.indexOf(',');
            if (commaLoc == -1) {
                //We must be at the end of the table. Get the last paren instead.
                commaLoc = st.lastIndexOf(')');
                if (commaLoc == -1) {
                    //If this doesn't exist, then there's really a problem.
                    System.out.println("Unable to create table: parsing error!");
                    return;
                }
                else {
                    colTypePotential = st.substring(0, commaLoc);
                    st = "";
                }
            }
            else {
                colTypePotential = st.substring(0, commaLoc);
                st = st.substring(commaLoc+1);
                st = st.trim();
            }
            colTypePotential = colTypePotential.trim();
            
            // Get the actual column type from colTypePotential
            spaceLoc = colTypePotential.indexOf(' ');
            String colType = "";
            if (spaceLoc == -1) {
                //There's no extra information; set colType
                colType = colTypePotential;
            }
            else {
                colType = colTypePotential.substring(0,spaceLoc);
            }
            
            // if there is a default value, get it. Otherwise, ignore all other
            // keywords as buzhug doesn't support them
            String defaultVal = "";
            while(colTypePotential.length() > 9) {
                //"Default" is 7-characters long
                String testString = colTypePotential.substring(0,7);
                if(testString.equalsIgnoreCase("Default")) {
                    colTypePotential = colTypePotential.substring(7);
                    colTypePotential = colTypePotential.trim();
                    // Get the col type; it is the next value in the string
                    spaceLoc = colTypePotential.indexOf(' ');
                    if (spaceLoc == -1) {
                        defaultVal = colTypePotential;
                    }
                    else {
                        defaultVal = colTypePotential.substring(0, spaceLoc);
                    }
                    break;
                }
                else {
                    colTypePotential = colTypePotential.substring(1);
                }
            }
            columnDefaults.add(defaultVal);
            
            // buzhug only supports str, unicode, int, float, bool, date, or
            // datetime column types. Technically it also supports "links",
            // but these are basically equivalent to foreign keys, which we can
            // deal with once we implement joins (if we implement joins...)
            // It may not be standard sql, but if a column type is not one of 
            // the above then we will ignore it and print an error.
            if(!colType.equalsIgnoreCase("str") && 
               !colType.equalsIgnoreCase("unicode") &&
               !colType.equalsIgnoreCase("int") &&
               !colType.equalsIgnoreCase("float") &&
               !colType.equalsIgnoreCase("bool") &&
               !colType.equalsIgnoreCase("date") &&
               !colType.equalsIgnoreCase("datetime")) {
                System.out.println("Buzhug only supports the following column"
                                    + " types:");
                System.out.println("\tstr");
                System.out.println("\tunicode");
                System.out.println("\tint");
                System.out.println("\tfloat");
                System.out.println("\tbool");
                System.out.println("\tdate");
                System.out.println("\tdatetime");
                System.out.println("Can't create table.");
                System.out.println("  colType: " + colType);
                return;
            }
            else {
                columnTypes.add(colType);
            }
        }
        
        // Ensure that the three arrayLists are the same size, or there is a
        // problem
        if (columnTypes.size() != columnDefaults.size() ||
            columnTypes.size() != columnNames.size() ||
            columnNames.size() != columnDefaults.size()) {
            System.out.println("Unable to create table: number of columns types"
                                + ", names, or defaults is not equal!");
            return;
        }
        
        // Create the buzhug-formatted string
        String buzhugCreate1 = tableName + " = Base('" + basePath + tableName + "')";;
        String buzhugCreate2 = tableName + ".create(";
        for (int i = 0; i < columnNames.size(); i++) {
            String colEntry = "('" + columnNames.get(i) +"', "+columnTypes.get(i);
            if (columnDefaults.get(i).length() != 0) {
                colEntry = colEntry + ", " + columnDefaults.get(i) + ")";
            }
            else {
                colEntry = colEntry + ")";
            }
            buzhugCreate2 = buzhugCreate2 + colEntry + ", ";
        }
        buzhugCreate2 = buzhugCreate2.substring(0,buzhugCreate2.length()-2) + ")";
        
        // Debugging purposes
        System.out.println("Formated statement:\n" + buzhugCreate2);
        
        // call buzhug.
        try {
            interpreter.exec(buzhugCreate1);
            interpreter.exec(buzhugCreate2);
        }
        catch (Exception e) {
            System.out.println("Python Interpreter exception: " + e.getMessage());
            System.out.println("Stacktrace:");
            e.printStackTrace();
        }
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
                return;
            }
        }

        // Get rid of the comma after the final select value
        buzhugInsert = buzhugInsert.substring(0,buzhugInsert.length()-1);
        // Add the final parenthesis
        buzhugInsert = buzhugInsert + ")";
        
        // Debugging purposes
        System.out.println("Buzhug-Formated statement:\n" + buzhugInsert);
        
        // call buzhug.
        try {
            interpreter.exec(st.getTable() + " = Base('" + basePath 
                             + st.getTable() + "').open()");
            interpreter.exec(buzhugInsert);
        }
        catch (Exception e) {
            System.out.println("Python Interpreter exception: " + e.getMessage());
            System.out.println("Stacktrace:");
            e.printStackTrace();
        }
    }
    
    private static void callUpdate(ZUpdate st) {
        String table = st.getTable();
        // finding the records to update
        String where = formatWhere(st.getWhere());
        String cmd1 = "recs = " + table + ".select_for_update(None, " + where + ")";
        // updating
        String cmd2 = table + ".update(recs, ";
        Hashtable<String,ZConstant> vals = st.getSet();
        for(String col:vals.keySet())
        {
            cmd2+=col+"="+vals.get(col).toString()+",";
        }
        cmd2 = cmd2.substring(0, cmd2.length()-1) + ")";
        
        // print out the buzhug-formated statements, for debugging purposes
        System.out.println("Buzhug-formated statements:");
        System.out.println(cmd1);
        System.out.println(cmd2);
        
        // call buzhug
        try {
            //load the table
            interpreter.exec(table +
                             " = Base('" + basePath + table + "').open()");
            interpreter.exec(cmd1);
            interpreter.exec(cmd2);
        }
        catch (Exception e) {
            System.out.println("Python Interpreter exception: " + e.getMessage());
            System.out.println("Stacktrace:");
            e.printStackTrace();
        }
    }
    
    private static void callDelete(ZDelete st) {
        String table = st.getTable();
        // finding the records to delete
        String where = formatWhere(st.getWhere());
        String cmd1 = "recs = " + table + ".select(None, " + where + ")";
        // deleting
        String cmd2 = table + ".delete(recs)";
        String cmd3 = table + ".cleanup()";
        // print out the formated statements, mostly for debugging purposes
        System.out.println("Buzhug-formated statements:");
        System.out.println(cmd1);
        System.out.println(cmd2);
        System.out.println(cmd3);
        // call buzhug
        try {
            //load the table
            interpreter.exec(table + " = Base('" + basePath + table + "').open()");
            interpreter.exec(cmd1);
            interpreter.exec(cmd2);
            interpreter.exec(cmd3);
        }
        catch (Exception e) {
            System.out.println("Python Interpreter exception: " + e.getMessage());
            System.out.println("Stacktrace:");
            e.printStackTrace();
        }
    }
    
    private static void callSelect(ZQuery st) {
        // result_set holds the selected
        String cmd = "result_set = db.select(";
        // make sure its a simple select
        boolean isJoined = false;
        if(st.getFrom().contains("join"))
        {
            isJoined = true;
            System.out.println("Only simple selects work");

            return;
        }
    
        // Get the select values and form a string with them of the form
        // ['col1', 'col2', ..., 'colN']
        // If the select is a wildcard, however, then values is set to "None"
        Vector columns = st.getSelect();
        String values = "";
        if (!columns.get(0).toString().equals("*")) {
            values = "[";
            for (int i = 0; i < columns.size(); i++) {
                values = values + "'" + columns.get(i) + "',";
            }
            // Remove the final comma
            values = values.substring(0, values.length()-1);
            values += "]";
        }
        else {
            values = "None";
        }
        
        // if there is a where, we add it
        if(st.getWhere() != null)
        {
            String where = formatWhere(st.getWhere());
            cmd += values + ", " + where + ")";
        }
        else {
            cmd += values + ")";
        }
        System.out.println(cmd);
        String order = null;
        if(st.getOrderBy() != null)
        {
            order = "result_set = result_set.sort_by(\"";
            for(ZOrderBy OB: (Vector<ZOrderBy>)st.getOrderBy())
            {
                String[] orderBy = OB.toString().split(" ");
                if(OB.getAscOrder())
                    order+="+";
                else
                    order+="-";
                order += orderBy[0];
            }
            order += "\")";
            System.out.println(order);
        }

        // run on python
        try {
            //Get the table name
            if(isJoined)
            {
                // UPDATE THIS
                // IF FIND A WAY TO PARSE JOINS

            }
            else {
                String tableName = st.getFrom().get(0).toString();
                interpreter.exec("db = Base('" + basePath + tableName + "').open()");
                interpreter.exec(cmd);
                if(order!=null)
                    interpreter.exec(order);
                // Get the results so we can print them in java
                PyObject test = interpreter.eval("result_set");
                selectPrinter(test.toString());
            }
        }
        catch (Exception e) {
            System.out.println("Python Interpreter exception: " + e.getMessage());
            System.out.println("Stacktrace:");
            e.printStackTrace();
        }
    }
    
    // Returns the buzhug-formated select statement generated from st.
    // Unlike callSelect, this method will not call buzhug with the generated
    // statement, it will simply return it for use by other methods.
    private static String formatSelect(ZQuery st) {
        System.out.println("formatSelect not yet supported!");
        String buzhugSelect = "";
        return buzhugSelect;
    }
    
    // Turn a where clause into a format supported by Buzhug's select function
    private static String formatWhere(ZExp wh) {
        String result = helperWhere(wh, 0);
        int semiColon = result.indexOf(';');
        String where = "\"" + result.substring(0,semiColon) + "\"," 
                       + result.substring(semiColon+1);
        return where;
    }
    
    /* Helper function for formatWhere
     *
     * wh is the where clause. c is an integer value that is intended to ensure
     * than none of the "temporary" variables in the buzhug select share the
     * same name
     */
    private static String helperWhere(ZExp wh, int c) {
        String where = "";
        if (wh instanceof ZQuery) {
            System.out.println("Error: ZExp wh should be a where clause, not" +
                               " a full-blown query!");
            where = "ERROR";
        }
        else if (wh instanceof ZExpression) {
            ZExpression w = (ZExpression) wh;
            String operator = w.getOperator().toLowerCase();
            if (operator.equals("="))
                operator = "==";
            String whereFront = "";
            String whereBack = "";
            
            for(int i = 0; i < w.nbOperands(); i++) {
                String tmp = helperWhere(w.getOperand(i), c + i + 1000);
                if (tmp.equals("ERROR"))
                    return "ERROR";
                
                int semiColon = tmp.indexOf(';');
                whereFront = whereFront + tmp.substring(0, semiColon) 
                             + " " + operator + " ";
                if (semiColon+1 < tmp.length())
                    whereBack = whereBack + tmp.substring(semiColon+1) + ",";
            }
            where = whereFront.substring(0, whereFront.length() - operator.length() - 2)
                    + ";" + 
                    whereBack.substring(0, whereBack.length() - 1);
        }
        else if (wh instanceof ZConstant) {
            ZConstant w = (ZConstant) wh;
            if (w.getType() == ZConstant.COLUMNNAME) {
                where = w.getValue() + ";";
            }
            else {
                where = "tmp" + String.valueOf(c) + ";" +
                        "tmp" + String.valueOf(c) + "=" + w.getValue();
            }
        }
        else {
            System.out.println("Error: ZExp wh not ZQuery, ZExpression," +
                               " or ZConstant");
            where = "ERROR";
        }
        return where;
    }
    
    // Takes a string that is the result of a select call to buzhug and formats
    // it to look like a table.
    private static void selectPrinter(String sel) {
        ArrayList<String> row = new ArrayList<String>();
        // Cut off the [] from sel
        sel = sel.substring(1, sel.length()-1);
        
        int lastBrack = sel.indexOf('>');
        while (lastBrack != -1) {
            String thisRow = sel.substring(1, lastBrack);
            // Check to see that __id__ and __version__ are actually present,
            // since they only seem to be added for wildcard queries
            if(thisRow.indexOf("__id__") != -1) {
                // Remove the __id__ and __version__ fields that buzhug adds
                int spaceLoc = thisRow.indexOf(' ');
                thisRow = thisRow.substring(spaceLoc+1);
                spaceLoc = thisRow.indexOf(' ');
                thisRow = thisRow.substring(spaceLoc+1);
            }
            
            row.add(thisRow);
            // +3 to account for the ', <' substring after the '>'
            if (lastBrack+3 > sel.length())
                sel = "";
            else
                sel = sel.substring(lastBrack+3);
                
            lastBrack = sel.indexOf('>');
        }
        
        // Get the column names
        ArrayList<String> colNames = new ArrayList<String>();
        String tmp = row.get(0);
        int colon = tmp.indexOf(':');
        while (colon != -1) {
            colNames.add(tmp.substring(0,colon));
            if (tmp.indexOf(' ') == -1)
                tmp = "";
            else
                tmp = tmp.substring(tmp.indexOf(' ') + 1);
            colon = tmp.indexOf(':');
        }
        
        // Start printing shit
        // Temporary solution: simply print out the rows, now that we've removed
        // the __id__ and __version__ nonsense
        for (int i = 0; i < row.size(); i++) {
            System.out.println(row.get(i));
        }
    }
}
