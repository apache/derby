/*

   Derby - Class org.apache.derbyBuild.ErrorMessageGenerator

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyBuild;

import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.util.Hashtable;
import java.lang.Math;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;


/**
 *
 * This tool is used to generate the DITA file that lists all the SQL states
 * and their error messages.
 */
public class ErrorMessageGenerator
{
    /** Driver name */
	private	static	final	String	DERBY_EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

    /** Output file name */
    private	static	final	String	DITA_FILE_NAME = "rrefexcept71493.dita";

    /** Location of output file in documentation client. The root of the
        documentation client is the directory just above src.
     */
	private	static	final	String	OUTPUT_FILE_STUB = "src/ref/" + DITA_FILE_NAME;


    /** Usage string */
    private static  final   String  USAGE_STRING =
        "Usage:\n" +
        "\n" +
        "  java org.apache.derbyBuild.ErrorMessageGenerator DOC_ROOT\n" +
        "\n" +
        "    where DOC_ROOT = Root of documentation client (the directory just\n" +
        "                     above src). E.g., /home/myname/derby/docs/trunk\n"
        ;


    /** Success exit */
    private static  final   int SUCCESS = 0;
    /** Failure exit */
    private static  final   int FAILURE = 1;


    /** Root of the documentation client */
    private String  docClientRoot;

    /** The name of the DITA file */
    private String  ditaFileName = DITA_FILE_NAME;
    
    /** The connection URL */
    private String  url = "jdbc:derby:wombat;create=true";
    
    /** Used to write to the DITA file */
    private PrintWriter ditaWriter;
    
    /** Table of SQL State codes and their meaning */
    private static Hashtable codes = new Hashtable();

    /** short-hand for a double-quote */
    char dq = '"'; 
    
    /** Indicates whether we are on the first table */
    boolean firstTable = true;
    
    /** Stores the code for the current row */
    String currentCode;
    
    /** Stores the class comment for the current row */
    String currentComment;
    
    static
    {
        codes.put("0A", "Feature not supported");
        codes.put("01", "Warning");
        codes.put("04", "Database authentication");
        codes.put("07", "Dynamic SQL Error");
        codes.put("08", "Connection Exception");
        codes.put("21", "Cardinality Violation");
        codes.put("22", "Data Exception");
        codes.put("23", "Constraint Violation ");
        codes.put("24", "Invalid Cursor State");
        codes.put("25", "Invalid Transaction State");
        codes.put("28", "Invalid Authorization Specification");
        codes.put("2D", "Invalid Transaction Termination");
        codes.put("38", "External Function Exception");
        codes.put("39", "External Routine Invocation Exception");
        codes.put("3B", "Invalid SAVEPOINT");
        codes.put("40", "Transaction Rollback");
        codes.put("42", "Syntax Error or Access Rule Violation");
        codes.put("57", "DRDA Network Protocol: Execution Failure");
        codes.put("58", "DRDA Network Protocol: Protocol Error");
        codes.put("X0", "Execution exceptions");
        codes.put("XBCA", "CacheService");
        codes.put("XBCM", "ClassManager");
        codes.put("XBCX", "Cryptography");
        codes.put("XBM", "Monitor");
        codes.put("XCL", "Execution exceptions");
        codes.put("XCW", "Upgrade unsupported");
        codes.put("XCX", "Internal Utility Errors");
        codes.put("XCY", "Derby Property Exceptions");
        codes.put("XCZ", "org.apache.derby.database.UserUtility");
        codes.put("XD00", "Dependency Manager");
        codes.put("XIE", "Import/Export Exceptions");
        codes.put("XJ", "Connectivity Errors");
        codes.put("XN", "Network Client Exceptions");
        codes.put("XSAI", "Store - access.protocol.interface");
        codes.put("XSAM", "Store - AccessManager");
        codes.put("XSAS", "Store - Sort");
        codes.put("XSAX", "Store - access.protocol.XA statement");
        codes.put("XSCB", "Store - BTree");
        codes.put("XSCG0", "Conglomerate");
        codes.put("XSCH", "Heap");
        codes.put("XSDA", "RawStore - Data.Generic statement");
        codes.put("XSDB", "RawStore - Data.Generic transaction");
        codes.put("XSDF", "RawStore - Data.Filesystem statement");
        codes.put("XSDG", "RawStore - Data.Filesystem database");
        codes.put("XSLA", "RawStore - Log.Generic database exceptions");
        codes.put("XSLB", "RawStore - Log.Generic statement exceptions");
        codes.put("XSRS", "RawStore - protocol.Interface statement");
        codes.put("XSTA2", "XACT_TRANSACTION_ACTIVE");
        codes.put("XSTB", "RawStore - Transactions.Basic system");
        codes.put("XXXXX", "No SQLSTATE");
    }
    
    /**
     * <p>
     * Generate the dita file of SQLStates for inclusion in
     * Derby's Reference Guide.
     * </p>
     *
     * <ul>
     * <li>args[ 0 ] = Root of docs client. E.g. "/home/myname/derby/docs/trunk"</li>
     * </ul>
     */
    public static void main(String[] args)
    {
        ErrorMessageGenerator generator = new ErrorMessageGenerator();
        
        if ( !generator.parseArgs( args ) )
        {
            generator.printUsage();

            System.exit( FAILURE );
        }

        generator.setDitaFileName( generator.docClientRoot + '/' + OUTPUT_FILE_STUB );
        
        try
        {
            generator.execute();        
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
            System.exit( FAILURE );
        }
        System.exit( SUCCESS );
    }
    
    /**
     * <p>
     * Parse the arguments. Returns false if the arguments are malformed.
     * </p>
     *
     * <ul>
     * <li>args[ 0 ] = Root of docs client. E.g. "/home/myname/derby/docs"</li>
     * </ul>
     */
    private boolean    parseArgs( String args[] )
    {
        int     idx = 0;
        
        if ( args == null ) { return false; }
        if ( args.length != 1 ) { return false; }

        docClientRoot = args[ idx++ ];

        return true;
    }

    /**
     * <p>
     * Print instructions on how to run this program.
     * </p>
     */
    private void    printUsage()
    {
        System.out.println( USAGE_STRING );
    }

    /**
     * Set the name of the DITA file
     */
    public void setDitaFileName(String ditaFileName)
    {
        this.ditaFileName = ditaFileName;
    }
    
    /**
     * Set the database URL
     */
    public void setDatabaseUrl(String url)
    {
        this.url = url;
    }
    
    /**
     * Execute the program
     */
    public void execute() throws Exception
    {
        try
        {
            // Open the DITA file
            ditaWriter = openDitaFile();

            // Generate the header of the DITA file
            generateDitaHeader();

            // Generate the error messages for the DITA file
            generateMessages();

            // Generate the footer of the DITA file
            generateDitaFooter();

            ditaWriter.close();
        }
        catch ( Exception e )
        {
            throw e;
        }
        finally
        {
            if ( ditaWriter != null )
            {
                ditaWriter.close();
            }
        }
    }    
    
    /**
     * Open the DITA file for writing
     *
     * @return a PrintWriter for the DITA file
     */
    protected PrintWriter openDitaFile() throws Exception
    {
        return new PrintWriter(new FileOutputStream(ditaFileName));
    }
    
    /** 
     * Generate the header for the DITA file
     */
    protected void generateDitaHeader() throws Exception
    {
        PrintWriter dw = this.ditaWriter;
                
        ditaWriter.println("<?xml version=" + dq + "1.0" + dq +
            " encoding=" + dq + "utf-8" + dq + "?>");
        ditaWriter.println("<!DOCTYPE reference PUBLIC " + dq + 
            "-//OASIS//DTD DITA Reference//EN" + dq);
        ditaWriter.println(dq + "../dtd/reference.dtd" + dq + ">");
        ditaWriter.println("<reference id=" + dq + "rrefexcept71493" + dq +
            " xml:lang=" + dq + "en-us" + dq + ">");
        ditaWriter.println("<!-- ");
        ditaWriter.println("Licensed to the Apache Software Foundation (ASF) under one or more");
        ditaWriter.println("contributor license agreements.  See the NOTICE file distributed with");
        ditaWriter.println("this work for additional information regarding copyright ownership.");
        ditaWriter.println("The ASF licenses this file to You under the Apache License, Version 2.0");
        ditaWriter.println("(the \"License\"); you may not use this file except in compliance with");
        ditaWriter.println("the License.  You may obtain a copy of the License at      ");
        ditaWriter.println("");
        ditaWriter.println("http://www.apache.org/licenses/LICENSE-2.0  ");
        ditaWriter.println("");
        ditaWriter.println("Unless required by applicable law or agreed to in writing, software  ");
        ditaWriter.println("distributed under the License is distributed on an \"AS IS\" BASIS,  ");
        ditaWriter.println("WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  ");
        ditaWriter.println("See the License for the specific language governing permissions and  ");
        ditaWriter.println("limitations under the License.");
        ditaWriter.println("-->");
        ditaWriter.println("<!-- ");
        ditaWriter.println("NOTE: this file is generated by running org.apache.derbyBuild.ErrorMessageGenerator  ");
        ditaWriter.println("This utility reads all the error messages from the database and ");
        ditaWriter.println("generates this file.  Please do not feel obligated to update it manually  ");
        ditaWriter.println("-->");
        ditaWriter.println("<title>SQL error messages and exceptions</title>");
        ditaWriter.println("<refbody>");
        ditaWriter.println("<section><p>The following tables list " +
            "<i>SQLStates</i> for exceptions. Exceptions ");
        ditaWriter.println("that begin with an <i>X</i> are specific to " +
            "<ph conref=" + dq + "refconrefs.dita#prod/productshortname" + 
            dq + "></ph>.");
        ditaWriter.println("In the messages below each {n} tag, where n is a number, represents a\n" +
                           "value that the Derby engine fills in at runtime. Examples of values\n" +
                           "include database names, database object names, property names, user\n" +
                           "names, and parameters passed to a function or procedure:\n" );
        ditaWriter.println("</p></section>");
        ditaWriter.println("<section>");
    }
    
    /** 
     * Generate the footer for the DITA file
     */
    protected void generateDitaFooter() throws Exception
    {
        ditaWriter.println("</section>");
        ditaWriter.println("</refbody>");
        ditaWriter.println("</reference>");
    }
                

    /**
     * Generate the actual error messages
     */
    protected void generateMessages() throws Exception
    {
        // Get the list of messages from the database
        ResultSet rs = getMessages();
        
        String prevSqlState = null;
        while ( rs.next() )
        {
            String sqlState = rs.getString(1);
            String message  = replaceSpecialChars(rs.getString(2));
            String severity = rs.getString(3);
            
            // See if it's a new SQL State category, and if so,
            // start a new table in the DITA file
            testForNewCategory(sqlState, prevSqlState);
            
            generateTableEntry(sqlState, message, severity);
            prevSqlState = sqlState;
        }

        // Tidy up the last table.
        generateTableFooter();
    }
    
    /**
     * Replace a substring with some equivalent. For example, we would
     * like to replace "<" with "&lt;" in the error messages.
     * Add any substrings you would like to replace in the code below.
     * Be aware that the first paramter to the replaceAll() method is
     * interpreted as a regular expression.
     *
     * @param input 
     *      A String that may contain substrings that we want to replace
     * @return 
     *      Output String where substrings selected for replacement have been
     *      replaced.
     * @see java.util.regex.Pattern
     */
    private static String replaceSpecialChars(java.lang.String input) {
        String output = input.replaceAll("<", "&lt;");
        output = output.replaceAll(">", "&gt;");
        
        return output;
    }
    
    /**
     * Test to see if we have a new SQL State category, and if so,
     * end the old table and start a new table in the DITA file.
     *
     * @param sqlState
     *      The SQL State for the current row
     *
     * @param oldSqlState
     *      The SQL State for the previous row
     */
    protected void testForNewCategory(String sqlState, String prevSqlState)
        throws Exception
    {
        String prevCode = currentCode;
        currentCode = getCode(sqlState);
        
        if ( currentCode == null )
        {
            ditaWriter.println("Unable to determine code for SQL State " + 
                sqlState);
            System.out.println("Unable to determine code for SQL State " +
                sqlState);
            return;
        }
        
        if ( currentCode.equals(prevCode))
        {
            return;
        }

        // If we got here, it's a new prefix, let's end the old table
        // and generate a header for a new table
        generateTableHeader();
    }
    
    /**
     * Get the class for the current SQL State.  
     * SIDE EFFECT: sets this.currentComment
     */
    protected String getCode(String sqlState) throws Exception
    {
        String comment = null;
        String code = null;
        
        if ( sqlState == null )
        {
            return null;
        }
        
        int codeLen = sqlState.length();
        
        while ( codeLen >= 2 )
        {
            code = sqlState.substring(0,codeLen);
            comment = (String)codes.get(code);
            if ( comment != null )
            {
                this.currentComment = comment;
                return code;
            }
            codeLen--;
        }
        
        if ( comment == null )
        {
            return null;
        }
        
        return code;
    }
            
    /**
     * Generate the table header for a given prefix
     */
    protected void generateTableHeader() 
        throws Exception
    {
        // Generate the end of the previous table
        if ( ! firstTable )
        {
            generateTableFooter();
        }
        else
        {
            firstTable = false;
        }
               
        // Generate the header for this table
        ditaWriter.println("<table><title>Class " + currentCode + ": " + 
            currentComment + "</title>");
        ditaWriter.println("<tgroup cols=" + dq + "2" + dq + 
            "><colspec colname=" + dq + "col1" + dq + " colwidth=" + dq +
            "1*" + dq + "/><colspec colname=" + dq + "col2" + dq);
        ditaWriter.println("colwidth=" + dq + "7.5*" + dq +
            "/>");
        ditaWriter.println("<thead>");
        ditaWriter.println("<row valign=" + dq + "bottom" + dq + ">");
        ditaWriter.println("<entry colname=" + dq + "col1" + dq +
            ">SQLSTATE</entry>");
        ditaWriter.println("<entry colname=" + dq + "col2" + dq +
            ">Message Text</entry>");
        ditaWriter.println("</row>");
        ditaWriter.println("</thead>");
        ditaWriter.println("<tbody>");
    }
    
    /**
     * Generate the table footer for a given prefix
     */
    protected void generateTableFooter() 
        throws Exception
    {
        ditaWriter.println("</tbody>");
        ditaWriter.println("</tgroup>");
        ditaWriter.println("</table>");
    }
    
    /**
     * Generate a table entry for the current row
     */
    protected void generateTableEntry(String sqlState, String message,
        String severity) throws Exception
    {
        ditaWriter.println("<row>");
        ditaWriter.println("<entry colname =" + dq + "col1" + dq + ">" +
            sqlState + "</entry>");
        ditaWriter.println("<entry colname =" + dq + "col2" + dq + ">" +
            message + "</entry>");
        ditaWriter.println("</row>");
    }
    
    /**
     * Get the messages from the database
     */
    protected ResultSet getMessages() throws Exception
    {
        Class.forName( DERBY_EMBEDDED_DRIVER );
        
        Connection conn = DriverManager.getConnection( url );
        
        if ( conn == null )
        {
            throw new Exception("Unable to connect to " + url);
        }
        
        Statement stmt = conn.createStatement();
        
        ResultSet rs = stmt.executeQuery("SELECT SQL_STATE, MESSAGE, SEVERITY FROM " +
            "new org.apache.derby.diag.ErrorMessages() AS vti "
            + "ORDER BY SQL_STATE");
        
        return rs;
    }
}
