/*

   Derby - Class org.apache.derbyTesting.functionTests.util.TestUtil

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */


package org.apache.derbyTesting.functionTests.util;

import java.sql.*;
import java.io.*;
import org.apache.derby.iapi.reference.JDBC30Translation;


/**
	Utility methods for tests, in order to bring some consistency to test output.
*/
public class TestUtil {

	public static void dumpSQLExceptions(SQLException sqle) {
		TestUtil.dumpSQLExceptions(sqle, false);
	}

	public static void dumpSQLExceptions(SQLException sqle, boolean expected) {
		String prefix = "";
		if (!expected) {
			System.out.println("FAIL -- unexpected exception ****************");
		}
		else
		{
			prefix = "EXPECTED ";
		}

		do
		{
			System.out.println(prefix + "SQLSTATE("+sqle.getSQLState()+"): " + sqle.getMessage());
			sqle = sqle.getNextException();
		} while (sqle != null);
	}


	  public static String sqlNameFromJdbc(int jdbcType) {
		switch (jdbcType) {
			case Types.BIT 		:  return "Types.BIT";
			case JDBC30Translation.SQL_TYPES_BOOLEAN  : return "Types.BOOLEAN";
			case Types.TINYINT 	:  return "Types.TINYINT";
			case Types.SMALLINT	:  return "SMALLINT";
			case Types.INTEGER 	:  return "INTEGER";
			case Types.BIGINT 	:  return "BIGINT";

			case Types.FLOAT 	:  return "Types.FLOAT";
			case Types.REAL 	:  return "REAL";
			case Types.DOUBLE 	:  return "DOUBLE";

			case Types.NUMERIC 	:  return "Types.NUMERIC";
			case Types.DECIMAL	:  return "DECIMAL";

			case Types.CHAR		:  return "CHAR";
			case Types.VARCHAR 	:  return "VARCHAR";
			case Types.LONGVARCHAR 	:  return "LONG VARCHAR";
            case Types.CLOB     :  return "CLOB";

			case Types.DATE 		:  return "DATE";
			case Types.TIME 		:  return "TIME";
			case Types.TIMESTAMP 	:  return "TIMESTAMP";

			case Types.BINARY			:  return "CHAR () FOR BIT DATA";
			case Types.VARBINARY	 	:  return "VARCHAR () FOR BIT DATA";
			case Types.LONGVARBINARY 	:  return "LONG VARCHAR FOR BIT DATA";
            case Types.BLOB             :  return "BLOB";

			case Types.OTHER		:  return "Types.OTHER";
			case Types.NULL		:  return "Types.NULL";
			default : return String.valueOf(jdbcType);
		}
	}
	  public static String jdbcNameFromJdbc(int jdbcType) {
		switch (jdbcType) {
			case Types.BIT 		:  return "Types.BIT";
			case JDBC30Translation.SQL_TYPES_BOOLEAN  : return "Types.BOOLEAN";
			case Types.TINYINT 	:  return "Types.TINYINT";
			case Types.SMALLINT	:  return "Types.SMALLINT";
			case Types.INTEGER 	:  return "Types.INTEGER";
			case Types.BIGINT 	:  return "Types.BIGINT";

			case Types.FLOAT 	:  return "Types.FLOAT";
			case Types.REAL 	:  return "Types.REAL";
			case Types.DOUBLE 	:  return "Types.DOUBLE";

			case Types.NUMERIC 	:  return "Types.NUMERIC";
			case Types.DECIMAL	:  return "Types.DECIMAL";

			case Types.CHAR		:  return "Types.CHAR";
			case Types.VARCHAR 	:  return "Types.VARCHAR";
			case Types.LONGVARCHAR 	:  return "Types.LONGVARCHAR";
            case Types.CLOB     :  return "Types.CLOB";

			case Types.DATE 		:  return "Types.DATE";
			case Types.TIME 		:  return "Types.TIME";
			case Types.TIMESTAMP 	:  return "Types.TIMESTAMP";

			case Types.BINARY			:  return "Types.BINARY";
			case Types.VARBINARY	 	:  return "Types.VARBINARY";
			case Types.LONGVARBINARY 	:  return "Types.LONGVARBINARY";
            case Types.BLOB             :  return "Types.BLOB";

			case Types.OTHER		:  return "Types.OTHER";
			case Types.NULL		:  return "Types.NULL";
			default : return String.valueOf(jdbcType);
		}
	}

	/*** Some routines for printing test information to html  **/

	public static String TABLE_START_TAG =  "<TABLE border=1 cellspacing=1 cellpadding=1  bgcolor=white  style='width:100%'>";
	public static String TABLE_END_TAG = "</TABLE>";
	public static String TD_INVERSE =
		"<td  valign=bottom align=center style=background:#DADADA;  padding:.75pt .75pt .75pt .75pt'> <p class=MsoNormal style='margin-top:6.0pt;margin-right:0in;margin-bottom:  6.0pt;margin-left:0in'><b><span style='font-size:8.5pt;font-family:Arial;  color:black'>";

	public static String TD_CENTER = "<TD valign=center align=center> <p class=MsoNormal style='margin-top:6.0pt;margin-right:0in;margin-bottom:6.0pt;margin-left:0in'><b><span style='font-size:8.5pt;font-family:Arial;  color:black'>";

	public static String TD_LEFT = "<TD valign=center align=left> <p class=MsoNormal style='margin-top:6.0pt;margin-right:0in;margin-bottom:6.0pt;margin-left:0in'><b><span style='font-size:8.5pt;font-family:Arial;  color:black'>";

	
	public static String TD_END = "</SPAN></TD>";

	public static String END_HTML_PAGE="</BODY> </HTML>";
	

	public static void startHTMLPage(String title, String author)
	{
		System.out.println("<HTML> \n <HEAD>");
		System.out.println(" <meta http-equiv=\"Content-Type\"content=\"text/html; charset=iso-8859-1\">");
		System.out.println("<meta name=\"Author\" content=\"" + author +  "\">");
		System.out.println("<title>" + title + "</title>");
		System.out.println("</HEAD> <BODY>");
		System.out.println("<H1>" + title + "</H1>");
	}

	public static void endHTMLPage()
	{
		System.out.println(END_HTML_PAGE);
	}

/*	public static void main(String[] argv)
	{
		testBoolArrayToHTMLTable();
	}
*/

	/** 
	 * Converts 2 dimensional boolean array into an HTML table.
	 * used by casting.java to print out casting doc
	 *
	 * @param rowLabels   - Row labels
	 * @param colLabels   - Column labels
	 **/
	public static void printBoolArrayHTMLTable(String rowDescription,
											   String columnDescription,
											   String [] rowLabels, 
											   String [] colLabels,
											   boolean[][] array,
											   String tableInfo)
	{

		System.out.println("<H2>" + tableInfo + "</H2>");

		System.out.println(TABLE_START_TAG);
		System.out.println("<TR>");
		// Print corner with labels
		System.out.println(TD_INVERSE +columnDescription + "---><BR><BR><BR><BR><BR>");
		System.out.println("<---" +rowDescription);
		System.out.println(TD_END);

		
		// Print column headers
		for (int i = 0; i < colLabels.length; i++)
		{
			System.out.println(TD_INVERSE);
			for (int c = 0; c < colLabels[i].length() && c < 20; c++)
			{
				System.out.println(colLabels[i].charAt(c) + "<BR>");
			}
			System.out.println(TD_END);
		}

		System.out.println("</TR>");

		// Print the Row Labels and Data
		for (int i = 0; i < rowLabels.length; i ++)
		{
			System.out.println("<TR>");
			System.out.println(TD_LEFT);
			System.out.println("<C> " +  rowLabels[i] + "</C>");
			System.out.println(TD_END);

			for (int j = 0; j < colLabels.length; j ++)
			{
				System.out.println(TD_CENTER);
				System.out.println((array[i][j]) ? "Y" : "-");
				System.out.println(TD_END);
			}
			System.out.println("</TR>");
		}


		System.out.println(TABLE_END_TAG);
		System.out.println("<P><P>");

	}

	/**
	 * Just converts a string to a hex literal to assist in converting test
	 * cases that used to insert strings into bit data tables
	 * Converts using UTF-16BE just like the old casts used to.
	 *
	 * @ param s  String to convert  (e.g
	 * @ resturns hex literal that can be inserted into a bit column.
	 */
	public static String stringToHexLiteral(String s)
	{
		byte[] bytes;
		String hexLiteral = null;
		try {
			bytes = s.getBytes("UTF-16BE");
			hexLiteral = convertToHexString(bytes);
		}
		catch (UnsupportedEncodingException ue)
		{
			System.out.println("This shouldn't happen as UTF-16BE should be supported");
			ue.printStackTrace();
		}

		return hexLiteral;
	}

	private static String convertToHexString(byte [] buf)
	{
		StringBuffer str = new StringBuffer();
		str.append("X'");
		String val;
		int byteVal;
		for (int i = 0; i < buf.length; i++)
		{
			byteVal = buf[i] & 0xff;
			val = Integer.toHexString(byteVal);
			if (val.length() < 2)
				str.append("0");
			str.append(val);
		}
		return str.toString() +"'";
	}



	/**
		Get the JDBC version, inferring it from the driver.
		We cannot use the JDBC DatabaseMetaData method
		as it is not present in JDBC 2.0.
	*/

	public static int getJDBCMajorVersion(Connection conn)
	{
		try {
			conn.getClass().getMethod("setSavepoint", null);
			return 3;
		} catch (NoSuchMethodException e) {
			return 2;
		} catch (NoClassDefFoundError e2) {
			return 2;
		}

	}


}



