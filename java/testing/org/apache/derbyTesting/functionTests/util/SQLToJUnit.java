/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.derbyTesting.functionTests.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * Convert a SQL script to a JUnit test.
 * 
 * Usage: java org.apache.derbyTesting.functionTests.util.SQLToJUnit <embedded_sql_out_file>
 */
public class SQLToJUnit
{
    // Default left margin is 8 spaces.
    private static final int DEFAULT_LEFT_MARGIN = 8;
    private static String leftMargin;

    private static final String IJ_COMMENT = "--";
    private static final String IJ_WARNING = "ij warning";
    private static final String JAVA_COMMENT = "//";
    private static final String RS_META_OBJECT_NAME = "rsmd";
    private static final String RS_OBJECT_NAME = "rs";
    private static final String CSTMT_OBJECT_NAME = "cSt";
    private static final String PSTMT_OBJECT_NAME = "pSt";
    private static final String SQL_WARN_OBJECT_NAME = "sqlWarn";

    private static String IJ_PROMPT = "ij>";
    private static String CONN_OBJECT_NAME = "conn";
    private static String STMT_OBJECT_NAME = "st";
    private static String USER_NAME = "";
    private static String getWarningLogic;

    // DDL statements for which we will NOT generate row count assertions.
    private static final String [] DDL_NO_RC_COMMANDS =
    {
        "create", "drop", "alter", "insert", "rename", "grant ", "set schema",
        "revoke", "lock table"
    };

    // DDL statements for which we WILL generate row count assertions.
    private static final String [] DDL_RC_COMMANDS =
    {
        "update", "delete", "declare"
    };

    // SQL statements that return a result set.
    private static final String [] QUERY_COMMANDS =
    {
        "select", "values"
    };
    
    // IJ-only (non-SQL) commands
    private static final String [] IJ_COMMANDS =
    {
    	"show", "describe"
    };

    /* Positive numbers (and zero) indicate that we need to
     * generate some kind of JDBC call and (potentially)
     * assert something (result set, row count, etc.); negative
     * numbers represent a "result" from executing some
     * statement--ex. error, warning, row count, etc.
     */
    private static int EXEC_QUERY = 0;
    private static int EXEC_DDL_NO_ROW_COUNT = 1;
    private static int EXEC_DDL_WITH_ROW_COUNT = 2;
    private static int PREPARE = 3;
    private static int P_EXECUTE = 4;
    private static int CALL_STMT = 5;
    private static int COMMENT = 6;
    private static int AUTOCOMMIT = 7;
    private static int COMMIT = 8;
    private static int ROLLBACK = 9;
    private static int SET_SCHEMA = 10;
    private static int REMOVE = 11;
    private static int GRANT = 12;
    private static int GET_CURSOR = 13;
    private static int CURSOR_NEXT = 14;
    private static int CURSOR_CLOSE = 15;
    private static int CONNECT = 16;
    private static int SET_CONNECTION = 17;
    private static int REVOKE = 18;
    private static int IJ_COMMAND = 19;
    
    private static int ROW_COUNT = -1;
    private static int BLANK_LINE = -3;
    private static int SQL_ERROR = -4;
    private static int SQL_WARNING = -5;
    private static int IWARNING = -7;

    /* Note that the result set produced by execution of a
     * QUERY_COMMAND falls into the category of "UNKNOWN
     * LINE" because there is no fixed "prefix" that matches
     * the start of all result sets.
     */
    private static int UNKNOWN_LINE = -999;

    // Maximum length of a line to print to the JUnit output
    // (with some exceptions).
    private static int LINE_LENGTH = 60;

    private int prevLineType;
    private String jTestName;

    private BufferedReader ijScript;
    private BufferedWriter junit;
    
    // tmpBuf is used as a pushback buffer to store lines in case we
    // go too far reading in result set lines.
    private StringBuffer tmpBuf;
    
    // used to skip commands in runtime statistics calls across calls to getNextIjCommand
    private boolean gotRuntimeStatistics = false;

    private int numIgnored = 0;
    private int numUnconverted = 0;
    
    /* This is set if connect .. user .. as statements are found in
     * the test to decide if we should alter our search patterns to
     * account for ij's multiple connection behavior. The hashtable
     * is used to store the user and connection names.
     */
    private boolean multipleUserConnections = false;
    private Properties userConnections = new Properties();
    private int usersConnected;
    private int anonymousCount = 0;

    public static void main(String [] args)
    {
        try {

            (new SQLToJUnit()).convert(args);

        } catch (Exception e) {

            System.out.println("OOPS, top-level error:");
            e.printStackTrace();

        }
    }

    /**
     * Convert .sql test output to JUnit JDBC code.
     * 
     * This keeps two lines/blocks of output at a time in currLineType
     * and nextLineType so that the result of statement executions can be
     * handled along with the executing statement, for example to generate
     * the necessary asserts for errors or resultsets.
     */
    public void convert(String [] args) throws Exception
    {
        if (args.length < 1)
        {
            System.out.println("\n  Usage:  java SQLToJUnit <embedded_sql_out_file>\n");
            return;
        }

        if (!loadIJScript(args[0]))
            return;

        writePrologue();
        tmpBuf = new StringBuffer();

        try {

            leftMargin = "";
            for (int i = 0; i < DEFAULT_LEFT_MARGIN; i++)
                leftMargin += " ";

            writeJUnitEOL();

            int nextLineType;
            boolean writeCurrLine;
            boolean done = false;
            StringBuffer currLine = new StringBuffer();
            StringBuffer nextLine = new StringBuffer();
            StringBuffer str = new StringBuffer();
            while (!done && getNextIjCommand(str))
            {
                nextLineType = getLineType(str);
                
                // if the next line is a set connection, make the switch to
                // the object names before the next line after is read.
                //
                // TODO: not sure this is the exactly right place for this,
                //       some comments are recorded with their ij prompt intact.
                if (nextLineType == SET_CONNECTION)
                {
                	CONN_OBJECT_NAME = str.substring(str.indexOf("set connection ") + 15, str.length());
                	STMT_OBJECT_NAME = "st_" + CONN_OBJECT_NAME;
            	    IJ_PROMPT = "ij(" + CONN_OBJECT_NAME.toUpperCase()+ ")>";
                	USER_NAME = (String)userConnections.getProperty(CONN_OBJECT_NAME);
                	junit.write("// set connection " + CONN_OBJECT_NAME);
                	writeJUnitEOL();
                }

                // Ignore IJ-specific warnings for now.
                while (ignorableLine(nextLineType))
                {
                    if (nextLineType != BLANK_LINE)
                    {
                        junit.write("[**:: IGNORED ::**] ");
                        junit.write(str.toString().trim());
                        writeJUnitEOL();
                        writeJUnitEOL();
                        numIgnored++;
                    }
                    str.delete(0, str.length());
                    if (!getNextIjCommand(str))
                    {
                        done = true;
                        break;
                    }
                    nextLineType = getLineType(str);
                }

                writeCurrLine = true;
                
                // Condense multiple comment lines into uniform
                // blocks of Java comments.
                if (nextLineType == COMMENT)
                {
                    if (prevLineType == COMMENT)
                    {
                        nextLine.append(strip(str, IJ_COMMENT));
                        writeCurrLine = false;
                    }
                    else
                    {
                        nextLine.append(str.toString().trim());
                        writeCurrLine = (currLine.length() > 0);
                    }
                }

                if (writeCurrLine && ((nextLineType != SQL_ERROR)
                    || (prevLineType != SQL_ERROR)))
                {
                    if (nextLineType != COMMENT)
                        nextLine.append(str);
                    
                    writeJavaLine(currLine, nextLine);
                    if (nextLine.length() == 0)
                        writeJUnitEOL();
                }

                // Clear out the lines that have been processed and shuffle
                // the buffers to prepare for the next incoming line.
                // NOTE: prevLineType should maybe be named currLineType?
                prevLineType = nextLineType;
                str.delete(0, str.length());
                currLine.append(nextLine.toString());
                nextLine.delete(0, nextLine.length());
            }
    
            if (currLine.length() > 0)
                writeJavaLine(currLine, nextLine);

            // TODO: need cleanup for multiple connections
            writeJUnitEOL();
            junit.write("getConnection().rollback();");
            writeJUnitEOL();
            junit.write("st.close();");
            junit.write("\n    }\n}");

            System.out.println("\n  ==> Ignored " + numIgnored + " lines and left " +
                numUnconverted + " lines unconverted.\n  ==> Output is in '" +
                jTestName + ".junit'.\n\n");
            
            if (multipleUserConnections) {
            	System.out.print("Found multiple users: ");
            	for (Enumeration e = userConnections.elements(); e.hasMoreElements(); )
            	{
            		System.out.print("\"");
            		System.out.print(e.nextElement());
            		System.out.print("\"");
            		System.out.print((e.hasMoreElements() ? ", " : ""));
            	}
            }

            System.out.println("\n\nDone.\n");
            junit.flush();
    
        } catch (Exception e) {

            // If something went wrong flush the junit output so that
            // user can see whereabouts the problem occurred.
            if (junit != null)
                junit.flush();

            throw e;
        }
    }

    private boolean loadIJScript(String fName)
        throws Exception
    {
        try {

            ijScript = new BufferedReader(
                new InputStreamReader(
                    new FileInputStream(fName)));

            if (fName.endsWith(".out"))
                jTestName = fName.substring(0, fName.indexOf("."));
            else
                jTestName = fName;

            jTestName = jTestName.replaceAll("[.]", "_");
            junit = new BufferedWriter(new FileWriter(jTestName + ".junit"));

        } catch (IOException ioe) {

            System.out.println("-=- Could not find IJ master: '" +
                fName + "'");

            return false;

        }

        return true;
    }

    private void writeJavaLine(StringBuffer lineToWrite,
        StringBuffer followupLine) throws Exception
    {
        if ((lineToWrite == null) || (lineToWrite.length() == 0))
            return;

        boolean writeEOL = true;
        strip(lineToWrite, IJ_PROMPT);
        if (prevLineType == COMMENT)
        {
            strip(lineToWrite, IJ_COMMENT);
            junit.write(JAVA_COMMENT);
            writeMaxLenLine(lineToWrite, JAVA_COMMENT + " ", null);
            writeJUnitEOL();
        }
        else if (prevLineType == IJ_COMMAND)
        {
            junit.write("[**:: UNCONVERTED ::**] ");
            junit.write(lineToWrite.toString());
            writeJUnitEOL();
            numUnconverted++;
        }
        else if (prevLineType >= 0)
            writeJDBCCode(lineToWrite, followupLine);    
        else if (prevLineType == SQL_WARNING)
        {
            writeAssertWarning(lineToWrite);
            writeJUnitEOL();
        }
        else
        {
            String str = lineToWrite.toString();
            if (str.trim().length() > 0)
            {
                junit.write("[**:: UNCONVERTED ::**] ");
                junit.write(str);
                writeJUnitEOL();
                numUnconverted++;
            }
            else
                writeEOL = false;
        }

        if (writeEOL)
            writeJUnitEOL();

        lineToWrite.delete(0, lineToWrite.length());
        return;
    }

    private void writeJDBCCode(StringBuffer stmt,
        StringBuffer result) throws Exception
    {
        int resultType = getLineType(result);
        boolean success = (resultType != SQL_ERROR);

        String indent = "";
        if (!success)
            indent = "    ";

        if (prevLineType == EXEC_QUERY)
        {
            if (success)
            {
                junit.write("rs = ");
                junit.write(STMT_OBJECT_NAME);
                junit.write(".executeQuery(");
                writeJUnitEOL();
                writeQuotedLine(stmt, indent);
                writeJUnitEOL();
                writeAssertResultSet(result);
            }
            else
                writeFailStatement(stmt, null, result, false);
        }
        else if ((prevLineType == EXEC_DDL_WITH_ROW_COUNT) 
            || (prevLineType == EXEC_DDL_NO_ROW_COUNT))
        {
            if (success)
            {
                if ((resultType == ROW_COUNT) &&
                    (prevLineType == EXEC_DDL_WITH_ROW_COUNT))
                {
                    writeAssertDDLCount(stmt, result, STMT_OBJECT_NAME);
                }
                else
                {
                    junit.write(STMT_OBJECT_NAME);
                    junit.write(".executeUpdate(");
                    writeJUnitEOL();
                    writeQuotedLine(stmt, indent);
                    result.delete(0, result.length());
                }
            }
            else
                writeFailStatement(stmt, null, result, false);
        }
        else if (prevLineType == PREPARE)
        {
            stmt.delete(0, stmt.indexOf("'") + 1);
            stmt.delete(stmt.lastIndexOf("'"), stmt.length());
            if (success)
            {
                junit.write(indent);
                junit.write(PSTMT_OBJECT_NAME);
                junit.write(" = ");
                junit.write("prepareStatement(");
                writeJUnitEOL();
                writeQuotedLine(stmt, indent);
                writeJUnitEOL();
            }
            else
                writeFailStatement(stmt, null, result, true);
        }
        else if (prevLineType == P_EXECUTE)
        {
            // Bind the parameters.
            if (stmt.indexOf("'") != -1)
            {
                stmt.delete(0, stmt.indexOf("'") + 1);
                stmt.delete(stmt.lastIndexOf("'"), stmt.length());
                junit.write(RS_OBJECT_NAME);
                junit.write(" = ");
                junit.write(STMT_OBJECT_NAME);
                junit.write(".executeQuery(");
                writeJUnitEOL();
                collapseQuotes(stmt, '\'');
                writeQuotedLine(stmt, indent);
                writeJUnitEOL();
                writeJUnitEOL();
                junit.write(RS_OBJECT_NAME);
                junit.write(".next();");
                writeJUnitEOL();
                junit.write(RS_META_OBJECT_NAME);
                junit.write(" = ");
                junit.write(RS_OBJECT_NAME);
                junit.write(".getMetaData();");
                writeJUnitEOL();
                junit.write("for (int i = 1; i <= ");
                junit.write(RS_META_OBJECT_NAME);
                junit.write(".getColumnCount(); i++)");
                writeJUnitEOL();
                junit.write("    ");
                junit.write(PSTMT_OBJECT_NAME);
                junit.write(".setObject(i, ");
                junit.write(RS_OBJECT_NAME);
                junit.write(".getObject(i));");
                writeJUnitEOL();
                writeJUnitEOL();
            }

            // Now execute.
            if (success)
            {
                if (resultType == ROW_COUNT)
                    writeAssertDDLCount(null, result, PSTMT_OBJECT_NAME);
                else
                {
                    junit.write("rs = ");
                    junit.write(PSTMT_OBJECT_NAME);
                    junit.write(".executeQuery();");
                    writeAssertResultSet(result);
                }
            }
            else
            {
                writeFailStatement(null, PSTMT_OBJECT_NAME,
                    result, false);
            }
        }
        else if (prevLineType == CALL_STMT)
        {
            junit.write(CSTMT_OBJECT_NAME);
            junit.write(" = prepareCall(");
            writeJUnitEOL();
            writeQuotedLine(stmt, "");
            
            if (success) {
            	writeJUnitEOL();
                writeAssertDDLCount(null, result, CSTMT_OBJECT_NAME);
            }
            else
            {
                writeJUnitEOL();
                writeFailStatement(null, CSTMT_OBJECT_NAME,
                    result, false);
            }
        }
        else if (prevLineType == AUTOCOMMIT)
        {
            junit.write(CONN_OBJECT_NAME + ".setAutoCommit(");
            junit.write(stmt.indexOf(" off") == -1 ? "true" : "false");
            junit.write(");");
            writeJUnitEOL();
        }
        else if (prevLineType == COMMIT)
            junit.write(CONN_OBJECT_NAME + ".commit();");
        else if (prevLineType == ROLLBACK)
        {
            junit.write(CONN_OBJECT_NAME + ".rollback();");
            writeJUnitEOL();
        }
        else if (prevLineType == GET_CURSOR)
            writeGetCursor(stmt, indent);
        else if (prevLineType == CURSOR_NEXT)
        {
            junit.write(stmt.substring(stmt.lastIndexOf(" ") + 1));
            junit.write(".next();");
            writeJUnitEOL();
        }
        else if (prevLineType == CURSOR_CLOSE)
        {
            String cName = stmt.substring(stmt.lastIndexOf(" ") + 1).trim();
            junit.write(cName);
            junit.write(".close();");
            writeJUnitEOL();
            junit.write("ps_");
            junit.write(cName);
            junit.write(".close();");
            writeJUnitEOL();
        } else if (prevLineType == CONNECT)
        {
        	multipleUserConnections = true;
        	int userpos = 0;
        	boolean anonymous = false;
        	
        	if (stmt.indexOf("user=") > 0) {
        		userpos = stmt.indexOf("user=") + 4;
        		if (stmt.indexOf(" as ") < 0) {
        			anonymous = true;
        		}
        	} else
        	{
        		userpos = stmt.indexOf("'", stmt.indexOf("user") + 1);
        	}
        	
            if (anonymous) {
            	CONN_OBJECT_NAME = "CONNECTION" + anonymousCount;
            	anonymousCount++;
            } else {
            	int connpos = stmt.indexOf(" as ");
            	CONN_OBJECT_NAME = stmt.substring(connpos + 4, stmt.length());
            }
        	STMT_OBJECT_NAME = "st_" + CONN_OBJECT_NAME;
        	USER_NAME = stmt.substring(userpos + 1, stmt.indexOf("'", userpos + 1));
        	if (userConnections.get(USER_NAME) == null) {
        		junit.write("Connection ");
        		junit.write(CONN_OBJECT_NAME);
        		junit.write(" = openUserConnection(\"");
        		junit.write(USER_NAME);
        		junit.write("\");");
        		writeJUnitEOL();
        		junit.write("Statement ");
        		junit.write(STMT_OBJECT_NAME);
        		junit.write(" = ");
        		junit.write(CONN_OBJECT_NAME);
        		junit.write(".createStatement();");
        		writeJUnitEOL();
        	}
        	userConnections.setProperty(CONN_OBJECT_NAME, USER_NAME);
        	usersConnected++;
        	if (usersConnected > 1)
        	    IJ_PROMPT = "ij(" + CONN_OBJECT_NAME.toUpperCase()+ ")>";
        } 
    }

    private void writeFailStatement(StringBuffer stmt,
        String stmtName, StringBuffer result, boolean compileTime)
        throws Exception
    {
        String sqlState = extractSQLState(result);
        if (compileTime)
            junit.write("assertCompileError(\"");
        else
            junit.write("assertStatementError(\"");
        junit.write(sqlState);
        junit.write("\", ");
        if (stmt != null)
        {
            if (!compileTime)
            {
                junit.write(STMT_OBJECT_NAME);
                junit.write(",");
            }
            writeJUnitEOL();
            writeQuotedLine(stmt, "");
        }
        else
        {
            junit.write(stmtName);
            junit.write(");");
        }
    }

    private void writeAssertDDLCount(StringBuffer stmt,
        StringBuffer ddlCount, String stmtName)
        throws Exception
    {
        junit.write("assertUpdateCount(");
        junit.write(stmtName);
        junit.write(", ");
        junit.write(extractRowCount(ddlCount));
        if (stmt != null)
        {
            junit.write(",");
            writeJUnitEOL();
            writeQuotedLine(stmt, "");
        }
        else
            junit.write(");");
    }

    private String extractRowCount(StringBuffer sBuf)
        throws Exception
    {
        int lineType = getLineType(sBuf);
        if (lineType != ROW_COUNT)
        {
            System.out.println("OOPS, tried to extract row count from " + sBuf);
            return "";
        }

        String rowCount = sBuf.substring(0, sBuf.indexOf(" ")).trim();
        sBuf.delete(0, sBuf.length());
        return rowCount;
    }

    /**
     * Write JDBC code for an ij statement of the form:
     *
     *   get cursor c1 as 'select * from t1' 
     */
    private void writeGetCursor(StringBuffer stmt, String indent)
        throws Exception
    {
        int pos_1 = stmt.indexOf("cursor") + 7;
        if (pos_1 < 0) 
        	pos_1 = stmt.indexOf("CURSOR") + 7;
        
        int pos_2 = stmt.indexOf(" as ");
        if (pos_2 < 0) 
        	pos_2 = stmt.indexOf(" AS ");
        String cName = stmt.substring(pos_1, pos_2).trim();
        junit.write("PreparedStatement ps_");
        junit.write(cName);
        junit.write(" = prepareStatement(");
        writeJUnitEOL();

        stmt.delete(0, stmt.indexOf("'") + 1);
        stmt.delete(stmt.lastIndexOf("'"), stmt.length());
        collapseQuotes(stmt, '\'');
        writeQuotedLine(stmt, indent);
        writeJUnitEOL();
        writeJUnitEOL();

        junit.write("ResultSet ");
        junit.write(cName);
        junit.write(" = ps_");
        junit.write(cName);
        junit.write(".executeQuery();");
        writeJUnitEOL();
    }

    private void writeMaxLenLine(StringBuffer aLine,
        String prefix, String suffix) throws Exception
    {
        if (aLine.length() <= LINE_LENGTH)
        {
            junit.write(aLine.toString());
            return;
        }

        int prefixLen = (prefix == null) ? 0 : prefix.length();
        int suffixLen = (suffix == null) ? 0 : suffix.length();

        String s;
        int pos = -1;
        boolean firstLine = true;
        while (aLine.length() > 0)
        {
            if (!firstLine)
            {
                writeJUnitEOL();
                if (prefixLen > 0)
                    junit.write(prefix);
            }

            pos = aLine.indexOf("\n");
            if (pos != -1)
            {
                writeMaxLenLine(new StringBuffer(aLine.substring(0, pos)),
                    prefix, suffix);
                pos++;
            }
            else if (aLine.length() <= LINE_LENGTH)
            {
                junit.write(aLine.toString());
                pos = aLine.length();
            }
            else
            {
                s = aLine.toString().substring(
                    0, LINE_LENGTH - prefixLen - suffixLen);

                pos = s.lastIndexOf(" ") + 1;
                if (pos == 0)
                    pos = s.length();
                junit.write(s.substring(0, pos));
            }

            aLine.delete(0, pos);
            if ((suffixLen > 0) && (aLine.length() > 0))
                junit.write(suffix);

            firstLine = false;
        }

        return;
    }

    private void escapeQuotes(StringBuffer aLine)
    {
        if ((aLine == null) || (aLine.length() == 0))
            return;

        int len = aLine.length();
        for (int i = len - 1; i >= 0; i--)
        {
            if (aLine.charAt(i) == '"')
                aLine.replace(i, i+1, "\\\"");
        }

        return;
    }

    private void collapseQuotes(StringBuffer aLine, char quote)
    {
        if ((aLine == null) || (aLine.length() == 0))
            return;

        int len = aLine.length();
        for (int i = len - 1; i >= 1; i--)
        {
            if ((aLine.charAt(i) == quote) &&
                (aLine.charAt(i-1) == quote))
            {
                aLine.deleteCharAt(i);
                i--;
            }
        }

        return;
    }

    private String strip(String str, String toStrip)
    {
        if ((toStrip == null) || (toStrip.length() == 0))
            return str;

        if (!str.trim().startsWith(toStrip))
            return str;

        return str.substring(
            str.indexOf(toStrip) + toStrip.length()).trim();
    }

    private StringBuffer strip(StringBuffer sBuf, String toStrip)
    {
        if ((toStrip == null) || (toStrip.length() == 0))
            return sBuf;

        if (sBuf.length() == 0)
            return sBuf;

        // Move past the beginning whitespace, if any.
        int pos = 0;
        while (Character.isWhitespace(sBuf.charAt(pos)))
            pos++;

        int len = toStrip.length();
        boolean okayToStrip = true;
        for (int i = 0; (i < len) && okayToStrip; i++)
        {
            if (sBuf.charAt(pos+i) != toStrip.charAt(i))
                okayToStrip = false;
        }

        if (okayToStrip)
            sBuf.delete(0, pos + len);

        return sBuf;
    }

    private int getLineType(StringBuffer sBuf)
    {
        return getLineType(sBuf.toString());
    }

    private int getLineType(String str)
    {
        str = strip(str, IJ_PROMPT).toLowerCase().trim();
        if (str.length() == 0)
            return BLANK_LINE;
        else if (str.startsWith(IJ_WARNING))
            return IWARNING;
        else if (str.startsWith(IJ_COMMENT))
            return COMMENT;
        else if (str.startsWith("error"))
            return SQL_ERROR;
        else if (str.startsWith("warning"))
            return SQL_WARNING;
        else if (isIjCommand(str))
        	return IJ_COMMAND;
        else if (isQueryStatement(str))
            return EXEC_QUERY;
        else if (isDDLWithRowCount(str))
            return EXEC_DDL_WITH_ROW_COUNT;
        else if (isDDLNoRowCount(str))
            return EXEC_DDL_NO_ROW_COUNT;
        else if (str.startsWith("prepare"))
            return PREPARE;
        else if (str.startsWith("execute"))
            return P_EXECUTE;
        else if (str.startsWith("call "))
            return CALL_STMT;
        else if (Character.isDigit(str.charAt(0)))
        {
            int pos = str.indexOf(" ");
            if ((pos > 0) && str.substring(pos).trim().startsWith("row"))
                return ROW_COUNT;
            return UNKNOWN_LINE;
        }
        else if (str.startsWith("autocommit"))
            return AUTOCOMMIT;
        else if (str.startsWith("commit"))
            return COMMIT;
        else if (str.startsWith("rollback"))
            return ROLLBACK;
        else if (str.startsWith("set schema"))
            return SET_SCHEMA;
        else if (str.startsWith("grant "))
            return GRANT;
        else if (str.startsWith("revoke"))
            return REVOKE;
        else if (str.startsWith("remove"))
            return REMOVE;
        else if (str.startsWith("get cursor"))
            return GET_CURSOR;
        else if (str.startsWith("next "))
            return CURSOR_NEXT;
        else if (str.startsWith("close"))
            return CURSOR_CLOSE;
        else if (str.startsWith("connect"))
        	return CONNECT;
        else if (str.startsWith("set connection"))
        	return SET_CONNECTION;
        else
            return UNKNOWN_LINE;
    }

    /* Note: the next four methods could probably be condensed into one
     * method: e.g. void isMember(String[], String)
     */
    
    private boolean isDDLWithRowCount(String str)
    {
        for (int i = 0; i < DDL_RC_COMMANDS.length; i++)
        {
            if (str.startsWith(DDL_RC_COMMANDS[i]))
                return true;
        }

        return false;
    }

    private boolean isDDLNoRowCount(String str)
    {
        for (int i = 0; i < DDL_NO_RC_COMMANDS.length; i++)
        {
            if (str.startsWith(DDL_NO_RC_COMMANDS[i]))
                return true;
        }

        return false;
    }

    private boolean isQueryStatement(String str)
    {
        for (int i = 0; i < QUERY_COMMANDS.length; i++)
        {
            if (str.startsWith(QUERY_COMMANDS[i]))
                return true;
        }

        return false;
    }
    
    private boolean isIjCommand(String str)
    {
        for (int i = 0; i < IJ_COMMANDS.length; i++)
        {
            if (str.startsWith(IJ_COMMANDS[i]))
                return true;
        }

        return false;
    }

    private void writeAssertResultSet(StringBuffer rsAsText)
        throws Exception
    {
        while ((rsAsText.length() > 0) &&
            Character.isWhitespace(rsAsText.charAt(0)))
        {
            rsAsText.deleteCharAt(0);
        }

        BufferedReader rsReader =
            new BufferedReader(
                new StringReader(rsAsText.toString()));

        int rowCount = 0;
        int colCount = 0;
        boolean wroteDecl = false;
        StringBuffer sBuf = new StringBuffer();
        StringTokenizer tkzr = null;
        for (String row = rsReader.readLine(); row != null;
            row = rsReader.readLine(), rowCount++)
        {
            // Second row is just "underlining" of the column names,
            // so skip it.
            if (rowCount == 1)
                continue;
            
            // ignore last line ROW_COUNTs
            // continue to write out assert statement.
            if (getLineType(row) == ROW_COUNT) {
            	rowCount--;
            	continue;
            }

            // First row is column names.
            if (rowCount == 0)
            {
                writeJUnitEOL();
                junit.write("expColNames = new String [] {");
            }
            else if (!wroteDecl)
            {
                writeJUnitEOL();
                junit.write("expRS = new String [][]");
                writeJUnitEOL();
                junit.write("{");
                wroteDecl = true;
            }

            if (rowCount > 2)
                junit.write(",");

            colCount = 0;
            tkzr = new StringTokenizer(row, "|");
            if (rowCount > 0)
            {
                writeJUnitEOL();
                junit.write("    {");
            }

            while (tkzr.hasMoreTokens())
            {
                sBuf.append(tkzr.nextToken().trim());
                escapeQuotes(sBuf);
                if (colCount > 0)
                    junit.write(", ");

                if (sBuf.toString().equals("NULL"))
                    junit.write("null");
                else
                {
                    junit.write("\"");
                    writeMaxLenLine(sBuf, "        + \"", "\"");
                    junit.write("\"");
                }

                colCount++;
                sBuf.delete(0, sBuf.length());
            }

            junit.write("}");
            if (rowCount == 0)
            {
                junit.write(";");
                writeJUnitEOL();
                junit.write("JDBC.assertColumnNames(rs, expColNames);");
                writeJUnitEOL();
            }
        }

        // Row count of 2 means we had an empty result set.
        if (rowCount == 2)
            junit.write("JDBC.assertDrainResults(rs, 0);");
        else
        {
            writeJUnitEOL();
            junit.write("};");
            writeJUnitEOL();
            writeJUnitEOL();
            junit.write("JDBC.assertFullResultSet(rs, expRS, true);");
        }

        rsAsText.delete(0, rsAsText.length());
    }

    /**
     * Extract out the SQLSTATE.  Messages are of the
     * form "ERROR <SQLSTATE>: ..." or "WARNING <SQLSTATE>: ...".
     * or "ERROR ... SQLSTATE <SQLSTATE>"
     */
    private String extractSQLState(StringBuffer errString)
        throws Exception
    {
    	String sqlState;
    	if (errString.indexOf("SQLSTATE") > 0)
    	{
    		sqlState = errString.delete(0, errString.indexOf(" ", errString.indexOf("SQLSTATE")) + 1).toString().substring(0,5);
    		errString.delete(0, 6);
    	} else {
            sqlState =
                errString.substring(
                    errString.indexOf(" ") + 1, errString.indexOf(":"));
            errString.delete(0, errString.length());	
    	}
        return sqlState;
    }

    private void writeAssertSQLState(String sqlState,
        String objName) throws Exception
    {
        junit.write("assertSQLState(\"");
        junit.write(sqlState);
        junit.write("\", ");
        junit.write(objName);
        junit.write(");");

        return;
    }

    private void writeAssertWarning(StringBuffer warnString)
        throws Exception
    {
        String indent = "    ";
        if (getWarningLogic == null)
        {
            getWarningLogic = 
                "if (usingEmbedded())\n" + leftMargin + "{\n" +
                leftMargin + indent + "if ((" + SQL_WARN_OBJECT_NAME +
                " == null) && (" + STMT_OBJECT_NAME + " != null))\n"
                + leftMargin + indent + indent + SQL_WARN_OBJECT_NAME
                + " = " + STMT_OBJECT_NAME + ".getWarnings();\n" +
                leftMargin + indent + "if (" + SQL_WARN_OBJECT_NAME +
                " == null)\n" + leftMargin + indent + indent +
                SQL_WARN_OBJECT_NAME + " = " + 
                (multipleUserConnections ? CONN_OBJECT_NAME : "getConnection()")
                + ".getWarnings();\n" + leftMargin;
        }
        
        junit.write(getWarningLogic);
        junit.write(indent);
        junit.write("assertNotNull(\"Expected warning but found none\", ");
        junit.write(SQL_WARN_OBJECT_NAME);
        junit.write(");");
        writeJUnitEOL();
        junit.write(indent);
        writeAssertSQLState(
            extractSQLState(warnString), SQL_WARN_OBJECT_NAME);
        writeJUnitEOL();
        junit.write(indent);
        junit.write("sqlWarn = null;");
        writeJUnitEOL();
        junit.write("}");
    }

    private void writeQuotedLine(StringBuffer line, String indent)
        throws Exception
    {
        junit.write(indent);
        junit.write("    \"");
        escapeQuotes(line);
        writeMaxLenLine(line, indent + "    + \"", "\"");
        junit.write("\");");
    }

    private void writeJUnitEOL()
        throws IOException
    {
        junit.write("\n");
        junit.write(leftMargin);
    }

    /**
     * Return one ij command, or if it's a result set, return the entire result set
     * in the StringBuffer.
     */
    private boolean getNextIjCommand(StringBuffer aLine)
        throws IOException
    {

        int c = 0;
        boolean done = false;

        StringBuffer targetBuf = aLine;
        String nextline = null;
        boolean insideRS = false;
        boolean gotFirstComment = false;
        boolean gotCommand = false;
        boolean readFromTmpBuf = false;

        if (tmpBuf.length() > 0)
        {
        	// get pushed back command 
            nextline = tmpBuf.toString();
            tmpBuf.delete(0, tmpBuf.length());
            readFromTmpBuf = true;
        }

        while(!done)
        {
        	if (!readFromTmpBuf) {
            	nextline = ijScript.readLine();	
        	} else {
        		readFromTmpBuf = false;
        	}
        	int linetype;
        	
            // Skip entirely blank lines:
            while (nextline != null)
            {
                nextline = nextline.trim();
                if (nextline.length() == 0)
                    nextline = ijScript.readLine();	
                else
                    break;
            }
        	if (nextline == null) {
        		c = -1;
        		break;
        	} else {
        		linetype = getLineType(nextline);
        		// multiple lines of SQL comments will be condensed in convert().
        		// If we are inside a result set, allow the first line of a command
        		// to be pushed back, otherwise Once we are accumulating lines for a command,
        		// don't stop until we find a semicolon.
        		if (!insideRS) 
        			gotCommand = gotCommand || (linetype > -1 && linetype != COMMENT);
        	}
        		
            if (!insideRS && (nextline.charAt(nextline.length() - 1) == ';'))
        	{
                // most likely a single-line command, chomp the semicolon and return
  			    targetBuf.append(nextline.substring(0, nextline.length() - 1));
  			    // if we're getting runtime statistics, skip the next command inside a 
  			    // result set, it will be the command that we're getting RS for.
  			    if (nextline.indexOf("GET_RUNTIMESTATISTICS()") > 0)
  			    	gotRuntimeStatistics = true;
  			    break;
        	} else {
        		// unknown lines are assumed to be a part of a command if we got one previously.
        		// otherwise, must be part of a result set.
        		if (!gotCommand) {
        			// must be part of a result set
        			if (linetype == UNKNOWN_LINE || (linetype == COMMENT && !gotFirstComment && insideRS)) {
            			insideRS = true;
            			if (linetype == COMMENT) gotFirstComment = true;
      			        targetBuf.append(nextline);
      			        targetBuf.append("\n");
      			        continue;
        			}
        	    }
        		if (insideRS) {
        			if (linetype > -1) {
            			// got a command, comment or query. Result
            			// set is over and we went one line too far, hold it for
            			// the next call.

                        // put the first command found into the resulset if this is a
        				// RuntimeStatistics resultset.
            			if (gotRuntimeStatistics) {
            				gotRuntimeStatistics = false;
              			    targetBuf.append(nextline);
              			    targetBuf.append("\n");
            				continue;
            			}
            			
            			tmpBuf.append(nextline);
            			break;
            		} else {
            			// got a row count or error, return it with the result set.
          			    targetBuf.append(nextline);
          			    break;
            		}
        		}  else {
      			    targetBuf.append(nextline);
      			    if (gotCommand){
            			// multi-line command or comment, keep going till
            			// we get a semicolon. Add a space to avoid glomming
      			    	// multiple trimmed strings together.
      			    	targetBuf.append(" ");
      			    	continue;
      			    } else{
      			    	// single line warning or error, finished
      			    	break;
      			    }
        		}
        	}
        }

        return (c != -1);
    }

    private boolean haveNonCommand(String aLine)
    {
        int lType = getLineType(aLine);
        return ((lType < 0) || (lType == COMMENT));
    }

    private boolean ignorableLine(int lineType)
    {
        return (lineType == IWARNING)
            || (lineType == BLANK_LINE);
    }

    /* TODO: It would be nice to automatically write the prologue to
     *       contain the proper sqlAuthorizationDecorator if we have
     *       multiple connections in the test, but this would require
     *       rethinking how the output is written, since the prologue
     *       could not be constructed until the rest of the output has
     *       been processed.
     */
    private void writePrologue() throws IOException
    {
        final String prologueText =
            "\npublic final class IJToJUnitTest extends BaseJDBCTestCase {\n\n" + 
            "    /**\n" +
            "     * Public constructor required for running test as standalone JUnit.\n" +
            "     */\n" +
            "    public IJToJUnitTest(String name)\n" +
            "    {\n" +
            "        super(name);\n" +
            "    }\n\n" +
            "    public static Test suite()\n" +
            "    {\n" +
            "        BaseTestSuite suite = " +
            "new BaseTestSuite(\"IJToJUnitTest Test\");\n" +
            "        suite.addTest(TestConfiguration.defaultSuite(IJToJUnitTest.class));\n" +
            "        return suite;\n" +
            "    }\n\n" +
            "    public void test_IJToJUnitTest() throws Exception\n" +
            "    {\n" +
            "        ResultSet rs = null;\n" +
            "        ResultSetMetaData rsmd;\n" +
            "        SQLWarning sqlWarn = null;\n\n" +
            "        PreparedStatement pSt;\n" +
            "        CallableStatement cSt;\n" +
            "        Statement st = createStatement();\n\n" +
            "        String [][] expRS;\n" +
            "        String [] expColNames;\n\n";

        junit.write(prologueText.replaceAll("IJToJUnitTest", jTestName));
    }
}
