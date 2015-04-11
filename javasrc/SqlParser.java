import java.sql.SQLException;
import java.util.Vector;
import java.io.*;

import org.gibello.zql.*;
import org.gibello.zql.data.*;

public class SqlParser {
    public static void main(String args[]) {
        /* If the user inputs an incorrect SQL statements, st = p.readStatement()
         * will throw an exception.
         * TODO: find a way to gracefully handle the exception instead of
         * crashing.
         */
        try {
            ZqlParser p = null;

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
                    System.out.println("Insert not yet supported!");
                    // TODO: call buzhug insert with the parsed inputs
                }
                else if (st instanceof ZUpdate) {
                    System.out.println("Update not yet supported!");
                    // TODO: call buzhug update with the parsed inputs
                }
                else if (st instanceof ZDelete) {
                    System.out.println("Delete not yet supported!");
                    // TODO: call buzhug delete with the parsed inputs
                }
                else if (st instanceof ZQuery) {
                    System.out.println("Select not yet supported!");
                    // TODO: call buzhug select with the parsed inputs
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    private void callInsert(ZStatement st) {
    }
    
    private void callUpdate(ZStatement st) {
    }
    
    private void callDelete(ZStatement st) {
    }
    
    private void callSelect(ZStatement st) {
    }
}
