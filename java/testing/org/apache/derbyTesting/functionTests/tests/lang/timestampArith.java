/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.timestampArith

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

package org.apache.derbyTesting.functionTests.tests.lang;

import org.apache.derby.tools.ij;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import java.util.Calendar;

/**
 * Test the JDBC TIMESTAMPADD and TIMESTAMPDIFF escape functions.
 *
 * Things to test:
 *   + Test each interval type with timestamp, date, and time inputs.
 *   + Test diff with all 9 combinations of datetime input types (timestamp - timestamp, timestamp - date, etc).
 *   + Test PreparedStatements with parameters, '?', in each argument, and Statements. (Statements are prepared
 *     internally so we do not also have to test PrepardStatements without parameters).
 *   + Test with null inputs.
 *   + Test with input string that is convertible to timestamp.
 *   + Test with invalid interval type.
 *   + Test with invalid arguments in the date time arguments.
 *   + Test TIMESTAMPADD with an invalid type in the count argument.
 *   + Test overflow cases.
 */
public class timestampArith
{
    private static final int FRAC_SECOND_INTERVAL = 0;
    private static final int SECOND_INTERVAL = 1;
    private static final int MINUTE_INTERVAL = 2;
    private static final int HOUR_INTERVAL = 3;
    private static final int DAY_INTERVAL = 4;
    private static final int WEEK_INTERVAL = 5;
    private static final int MONTH_INTERVAL = 6;
    private static final int QUARTER_INTERVAL = 7;
    private static final int YEAR_INTERVAL = 8;
    private static final String[] intervalJdbcNames =
    {"SQL_TSI_FRAC_SECOND", "SQL_TSI_SECOND", "SQL_TSI_MINUTE", "SQL_TSI_HOUR",
     "SQL_TSI_DAY", "SQL_TSI_WEEK", "SQL_TSI_MONTH", "SQL_TSI_QUARTER", "SQL_TSI_YEAR"};

    private static final int ONE_BILLION = 1000000000;

    int errorCount = 0;
    private Connection conn;
    private PreparedStatement[] tsAddPS = new PreparedStatement[intervalJdbcNames.length];
    private PreparedStatement[] tsDiffPS = new PreparedStatement[intervalJdbcNames.length];
    private Statement stmt;
    private static final String TODAY;
    private static final String TOMORROW;
    private static final String YEAR_FROM_TOMORROW;
    private static final String YEAR_FROM_TODAY;
    private static final String YESTERDAY;
    private static final String WEEK_FROM_TODAY;
    static {
        Calendar cal = Calendar.getInstance();
        // Make sure that we are not so close to midnight that TODAY might be yesterday before
        // we are finished using it.
        while( cal.get( Calendar.HOUR) == 23 && cal.get( Calendar.MINUTE) >= 58)
        {
            try
            {
                Thread.sleep( (60 - cal.get( Calendar.SECOND))*1000);
            }
            catch( InterruptedException ie) {};
            cal = Calendar.getInstance();
        }
        TODAY = isoFormatDate( cal);
        cal.add( Calendar.DATE, -1);
        YESTERDAY = isoFormatDate( cal);
        cal.add( Calendar.DATE, 2);
        TOMORROW = isoFormatDate( cal);
        cal.add( Calendar.YEAR, 1);
        YEAR_FROM_TOMORROW = isoFormatDate( cal);
        cal.add( Calendar.DATE, -1);
        YEAR_FROM_TODAY = isoFormatDate( cal);
        cal.add( Calendar.YEAR, -1); // today
        cal.add( Calendar.DATE, 7);
        WEEK_FROM_TODAY = isoFormatDate( cal);
    }
    
    private static String isoFormatDate( Calendar cal)
    {
        StringBuffer sb = new StringBuffer();
        String s = String.valueOf( cal.get( Calendar.YEAR));
        for( int i = s.length(); i < 4; i++)
            sb.append( '0');
        sb.append( s);
        sb.append( '-');

        s = String.valueOf( cal.get( Calendar.MONTH) + 1);
        for( int i = s.length(); i < 2; i++)
            sb.append( '0');
        sb.append( s);
        sb.append( '-');

        s = String.valueOf( cal.get( Calendar.DAY_OF_MONTH));
        for( int i = s.length(); i < 2; i++)
            sb.append( '0');
        sb.append( s);

        return sb.toString();
    }
    
    private final OneTest[] tests =
    {
        // timestamp - timestamp
        new OneDiffTest( FRAC_SECOND_INTERVAL, ts( "2005-05-10 08:25:00"), ts("2005-05-10 08:25:00.000001"), 1000,
                         null, null),
        new OneDiffTest( SECOND_INTERVAL, ts( "2005-05-10 08:25:01"), ts("2005-05-10 08:25:00"), -1, null, null),
        new OneDiffTest( SECOND_INTERVAL, ts( "2005-05-10 08:25:00.1"), ts("2005-05-10 08:25:00"), 0, null, null),
        new OneDiffTest( SECOND_INTERVAL, ts( "2005-05-10 08:25:00"), ts("2005-05-10 08:26:00"), 60, null, null),
        new OneDiffTest( MINUTE_INTERVAL, ts( "2005-05-11 08:25:00"), ts("2005-05-10 08:25:00"), -24*60, null, null),
        new OneDiffTest( HOUR_INTERVAL, ts("2005-05-10 08:25:00"), ts( "2005-05-11 08:25:00"), 24, null, null),
        new OneDiffTest( DAY_INTERVAL, ts("2005-05-10 08:25:00"), ts( "2005-05-11 08:25:00"), 1, null, null),
        new OneDiffTest( DAY_INTERVAL, ts("2005-05-10 08:25:01"), ts( "2005-05-11 08:25:00"), 0, null, null),
        new OneDiffTest( WEEK_INTERVAL, ts("2005-02-23 08:25:00"), ts( "2005-03-01 08:25:00"), 0, null, null),
        new OneDiffTest( MONTH_INTERVAL, ts("2005-02-23 08:25:00"), ts( "2005-03-23 08:25:00"), 1, null, null),
        new OneDiffTest( MONTH_INTERVAL, ts("2005-02-23 08:25:01"), ts( "2005-03-23 08:25:00"), 0, null, null),
        new OneDiffTest( QUARTER_INTERVAL, ts("2005-02-23 08:25:00"), ts( "2005-05-23 08:25:00"), 1, null, null),
        new OneDiffTest( QUARTER_INTERVAL, ts("2005-02-23 08:25:01"), ts( "2005-05-23 08:25:00"), 0, null, null),
        new OneDiffTest( YEAR_INTERVAL, ts("2005-02-23 08:25:00"), ts( "2005-05-23 08:25:00"), 0, null, null),
        new OneDiffTest( YEAR_INTERVAL, ts("2005-02-23 08:25:00"), ts( "2006-02-23 08:25:00"), 1, null, null),

        // timestamp - time, time - timestamp
        new OneDiffTest( FRAC_SECOND_INTERVAL, ts( TODAY + " 10:00:00.123456"), tm( "10:00:00"), -123456000, null, null),
        new OneDiffTest( FRAC_SECOND_INTERVAL, tm( "10:00:00"), ts( TODAY + " 10:00:00.123456"), 123456000, null, null),
        new OneDiffTest( SECOND_INTERVAL, ts( TODAY + " 10:00:00.1"), tm( "10:00:01"), 0, null, null),
        new OneDiffTest( SECOND_INTERVAL, tm( "10:00:01"), ts( TODAY + " 10:00:00"), -1, null, null),
        new OneDiffTest( MINUTE_INTERVAL, ts( TODAY + " 10:02:00"), tm( "10:00:00"), -2, null, null),
        new OneDiffTest( MINUTE_INTERVAL, tm( "11:00:00"), ts( TODAY + " 10:02:00"), -58, null, null),
        new OneDiffTest( HOUR_INTERVAL, ts( TODAY + " 10:02:00"), tm( "10:00:00"), 0, null, null),
        new OneDiffTest( HOUR_INTERVAL, tm( "10:00:00"), ts( TODAY + " 23:02:00"), 13, null, null),
        new OneDiffTest( DAY_INTERVAL, ts( TODAY + " 00:00:00"), tm( "23:59:59"), 0, null, null),
        new OneDiffTest( DAY_INTERVAL, tm( "23:59:59"), ts( TODAY + " 00:00:00"), 0, null, null),
        new OneDiffTest( WEEK_INTERVAL, ts( TODAY + " 00:00:00"), tm( "23:59:59"), 0, null, null),
        new OneDiffTest( WEEK_INTERVAL, tm( "23:59:59"), ts( TODAY + " 00:00:00"), 0, null, null),
        new OneDiffTest( MONTH_INTERVAL, ts( TODAY + " 00:00:00"), tm( "23:59:59"), 0, null, null),
        new OneDiffTest( MONTH_INTERVAL, tm( "23:59:59"), ts( TODAY + " 00:00:00"), 0, null, null),
        new OneDiffTest( QUARTER_INTERVAL, ts( TODAY + " 00:00:00"), tm( "23:59:59"), 0, null, null),
        new OneDiffTest( QUARTER_INTERVAL, tm( "23:59:59"), ts( TODAY + " 00:00:00"), 0, null, null),
        new OneDiffTest( YEAR_INTERVAL, ts( TODAY + " 00:00:00"), tm( "23:59:59"), 0, null, null),
        new OneDiffTest( YEAR_INTERVAL, tm( "23:59:59"), ts( TODAY + " 00:00:00"), 0, null, null),

        // timestamp - date, date - timestamp
        new OneDiffTest( FRAC_SECOND_INTERVAL, ts( "2004-05-10 00:00:00.123456"), dt("2004-05-10"), -123456000,
                         null, null),
        new OneDiffTest( FRAC_SECOND_INTERVAL, dt("2004-05-10"), ts( "2004-05-10 00:00:00.123456"), 123456000,
                         null, null),
        new OneDiffTest( SECOND_INTERVAL, ts( "2004-05-10 08:25:01"), dt("2004-05-10"), -(1+60*(25+60*8)), null, null),
        new OneDiffTest( SECOND_INTERVAL, dt( "2004-05-10"), ts("2004-05-09 23:59:00"), -60, null, null),
        new OneDiffTest( MINUTE_INTERVAL, ts( "2004-05-11 08:25:00"), dt("2004-05-10"), -(24*60+8*60+25), null, null),
        new OneDiffTest( MINUTE_INTERVAL, dt("2004-05-10"), ts( "2004-05-11 08:25:00"), 24*60+8*60+25, null, null),
        new OneDiffTest( HOUR_INTERVAL, ts("2004-02-28 08:25:00"), dt( "2004-03-01"), 39, null, null),
        new OneDiffTest( HOUR_INTERVAL, dt( "2005-03-01"), ts("2005-02-28 08:25:00"), -15, null, null),
        new OneDiffTest( DAY_INTERVAL, ts("2004-05-10 08:25:00"), dt( "2004-05-11"), 0, null, null),
        new OneDiffTest( DAY_INTERVAL, dt("2004-05-10"), ts( "2004-05-11 08:25:00"), 1, null, null),
        new OneDiffTest( WEEK_INTERVAL, ts("2004-02-23 00:00:00"), dt( "2004-03-01"), 1, null, null),
        new OneDiffTest( WEEK_INTERVAL, dt( "2004-03-01"), ts("2004-02-23 00:00:00"), -1, null, null),
        new OneDiffTest( MONTH_INTERVAL, ts("2004-02-23 08:25:00"), dt( "2004-03-24"), 1, null, null),
        new OneDiffTest( MONTH_INTERVAL, dt( "2005-03-24"), ts("2004-02-23 08:25:00"), -13, null, null),
        new OneDiffTest( QUARTER_INTERVAL, ts("2004-02-23 08:25:00"), dt( "2004-05-24"), 1, null, null),
        new OneDiffTest( QUARTER_INTERVAL, dt( "2004-05-23"), ts("2004-02-23 08:25:01"), 0, null, null),
        new OneDiffTest( YEAR_INTERVAL, ts("2004-02-23 08:25:00"), dt( "2004-05-23"), 0, null, null),
        new OneDiffTest( YEAR_INTERVAL, dt( "2004-05-23"), ts("2003-02-23 08:25:00"), -1, null, null),

        // date - time, time - date
        new OneDiffTest( FRAC_SECOND_INTERVAL, dt( TODAY), tm("00:00:01"), ONE_BILLION, null, null),
        new OneDiffTest( FRAC_SECOND_INTERVAL, tm("00:00:02"), dt( TODAY), -2*ONE_BILLION, null, null),
        new OneDiffTest( SECOND_INTERVAL, dt( TODAY), tm("00:00:01"), 1, null, null),
        new OneDiffTest( SECOND_INTERVAL, tm("00:00:02"), dt( TODAY), -2, null, null),
        new OneDiffTest( MINUTE_INTERVAL, dt( TODAY), tm("12:34:56"), 12*60 + 34, null, null),
        new OneDiffTest( MINUTE_INTERVAL, tm("12:34:56"), dt( TODAY), -(12*60 + 34), null, null),
        new OneDiffTest( HOUR_INTERVAL, dt( TODAY), tm("12:34:56"), 12, null, null),
        new OneDiffTest( HOUR_INTERVAL, tm("12:34:56"), dt( TODAY), -12, null, null),
        new OneDiffTest( DAY_INTERVAL, dt( TOMORROW), tm( "00:00:00"), -1, null, null),
        new OneDiffTest( DAY_INTERVAL, tm( "00:00:00"), dt( TOMORROW), 1, null, null),
        new OneDiffTest( WEEK_INTERVAL, dt( TOMORROW), tm( "00:00:00"), 0, null, null),
        new OneDiffTest( WEEK_INTERVAL, tm( "00:00:00"), dt( TOMORROW), 0, null, null),
        new OneDiffTest( MONTH_INTERVAL, dt( YEAR_FROM_TOMORROW), tm( "12:00:00"), -12, null, null),
        new OneDiffTest( MONTH_INTERVAL, tm( "12:00:00"), dt( YEAR_FROM_TOMORROW), 12, null, null),
        new OneDiffTest( QUARTER_INTERVAL, dt( YEAR_FROM_TOMORROW), tm( "12:00:00"), -4, null, null),
        new OneDiffTest( QUARTER_INTERVAL, tm( "12:00:00"), dt( YEAR_FROM_TOMORROW), 4, null, null),
        new OneDiffTest( YEAR_INTERVAL, dt( YEAR_FROM_TOMORROW), tm( "12:00:00"), -1, null, null),
        new OneDiffTest( YEAR_INTERVAL, tm( "12:00:00"), dt( YEAR_FROM_TOMORROW), 1, null, null),

        // Test add with all combinatons of interval types and datetime types
        new OneAddTest( FRAC_SECOND_INTERVAL, 1000, ts("2005-05-11 15:55:00"), ts("2005-05-11 15:55:00.000001"),
                        null, null),
        new OneAddTest( FRAC_SECOND_INTERVAL, -1000, dt("2005-05-11"), ts("2005-05-10 23:59:59.999999"),
                        null, null),
        new OneAddTest( FRAC_SECOND_INTERVAL, ONE_BILLION, tm("23:59:59"), ts( TOMORROW + " 00:00:00"), null, null),
        new OneAddTest( SECOND_INTERVAL, 60, ts("2005-05-11 15:55:00"), ts("2005-05-11 15:56:00"), null, null),
        new OneAddTest( SECOND_INTERVAL, 60, dt("2005-05-11"), ts("2005-05-11 00:01:00"), null, null),
        new OneAddTest( SECOND_INTERVAL, 60, tm("23:59:30"), ts( TOMORROW + " 00:00:30"), null, null),
        new OneAddTest( MINUTE_INTERVAL, -1, ts("2005-05-11 15:55:00"), ts("2005-05-11 15:54:00"), null, null),
        new OneAddTest( MINUTE_INTERVAL, 1, dt("2005-05-11"), ts("2005-05-11 00:01:00"), null, null),
        new OneAddTest( MINUTE_INTERVAL, 1, tm("12:00:00"), ts( TODAY + " 12:01:00"), null, null),
        new OneAddTest( HOUR_INTERVAL, 2, ts("2005-05-11 15:55:00"), ts("2005-05-11 17:55:00"), null, null),
        new OneAddTest( HOUR_INTERVAL, -2, dt("2005-05-11"), ts("2005-05-10 22:00:00"), null, null),
        new OneAddTest( HOUR_INTERVAL, 1, tm("12:00:00"), ts( TODAY + " 13:00:00"), null, null),
        new OneAddTest( DAY_INTERVAL, 1, ts("2005-05-11 15:55:00"), ts("2005-05-12 15:55:00"), null, null),
        new OneAddTest( DAY_INTERVAL, 1, dt("2005-05-11"), ts("2005-05-12 00:00:00"), null, null),
        new OneAddTest( DAY_INTERVAL, -1, tm( "12:00:00"), ts( YESTERDAY + " 12:00:00"), null, null),
        new OneAddTest( WEEK_INTERVAL, 1, ts("2005-05-11 15:55:00"), ts("2005-05-18 15:55:00"), null, null),
        new OneAddTest( WEEK_INTERVAL, 1, dt("2005-05-11"), ts("2005-05-18 00:00:00"), null, null),
        new OneAddTest( WEEK_INTERVAL, 1, tm("12:00:00"), ts( WEEK_FROM_TODAY + " 12:00:00"), null, null),
        new OneAddTest( MONTH_INTERVAL, 1, ts("2005-05-11 15:55:00"), ts("2005-06-11 15:55:00"), null, null),
        new OneAddTest( MONTH_INTERVAL, -1, dt("2005-03-29"), ts("2005-02-28 00:00:00"), null, null),
        new OneAddTest( MONTH_INTERVAL, 12, tm( "12:00:00"), ts( YEAR_FROM_TODAY + " 12:00:00"), null, null),
        new OneAddTest( QUARTER_INTERVAL, 1, ts("2005-10-11 15:55:00"), ts("2006-01-11 15:55:00"), null, null),
        new OneAddTest( QUARTER_INTERVAL, -2, dt( "2005-05-05"), ts( "2004-11-05 00:00:00"), null, null),
        new OneAddTest( QUARTER_INTERVAL, 4, tm( "12:00:00"), ts( YEAR_FROM_TODAY + " 12:00:00"), null, null),
        new OneAddTest( YEAR_INTERVAL, -10, ts("2005-10-11 15:55:00"), ts("1995-10-11 15:55:00"), null, null),
        new OneAddTest( YEAR_INTERVAL, 2, dt( "2005-05-05"), ts( "2007-05-05 00:00:00"), null, null),
        new OneAddTest( YEAR_INTERVAL, 1, tm( "12:00:00"), ts( YEAR_FROM_TODAY + " 12:00:00"), null, null),

        // String inputs
        new OneStringDiffTest( SECOND_INTERVAL, "2005-05-10 08:25:00", "2005-05-10 08:26:00", 60, null, null),
        new OneStringAddTest( DAY_INTERVAL, 1, "2005-05-11 15:55:00", ts("2005-05-12 15:55:00"), null, null),

        // Overflow
        new OneDiffTest( FRAC_SECOND_INTERVAL, ts( "2004-05-10 00:00:00.123456"), ts( "2004-05-10 00:00:10.123456"), 0,
                         "22003", "The resulting value is outside the range for the data type INTEGER."),
        new OneDiffTest( FRAC_SECOND_INTERVAL, ts( "2004-05-10 00:00:00.123456"), ts( "2005-05-10 00:00:00.123456"), 0,
                         "22003", "The resulting value is outside the range for the data type INTEGER."),
        new OneDiffTest( SECOND_INTERVAL, ts( "1904-05-10 00:00:00"), ts( "2205-05-10 00:00:00"), 0,
                         "22003", "The resulting value is outside the range for the data type INTEGER."),
        new OneAddTest( YEAR_INTERVAL, 99999, ts( "2004-05-10 00:00:00.123456"), null,
                        "22003", "The resulting value is outside the range for the data type TIMESTAMP.")
    };

    private final String[][] invalid =
    {
        {"values( {fn TIMESTAMPDIFF( SECOND, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)})", "42X01",
         "Syntax error: Encountered \"SECOND\" at line 1, column 28."},
        {"values( {fn TIMESTAMPDIFF( , CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)})", "42X01",
         "Syntax error: Encountered \",\" at line 1, column 28."},
        {"values( {fn TIMESTAMPDIFF( SQL_TSI_SECOND, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 5)})", "42X01",
         "Syntax error: Encountered \",\" at line 1, column 80."},
        {"values( {fn TIMESTAMPDIFF( SQL_TSI_SECOND, CURRENT_TIMESTAMP, 'x')})", "42X45",
         "CHAR is an invalid type for argument number 3 of TIMESTAMPDIFF."},
        {"values( {fn TIMESTAMPDIFF( SQL_TSI_SECOND, 'x', CURRENT_TIMESTAMP)})", "42X45",
         "CHAR is an invalid type for argument number 2 of TIMESTAMPDIFF."},
        {"values( {fn TIMESTAMPDIFF( SQL_TSI_SECOND, CURRENT_TIMESTAMP)})", "42X01",
         "Syntax error: Encountered \")\" at line 1, column 61."},
        {"values( {fn TIMESTAMPDIFF( SQL_TSI_SECOND)})", "42X01",
         "Syntax error: Encountered \")\" at line 1, column 42."},
        {"values( {fn TIMESTAMPADD( x, 1, CURRENT_TIMESTAMP)})", "42X01",
           "Syntax error: Encountered \"x\" at line 1, column 27."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, CURRENT_DATE, CURRENT_TIMESTAMP)})", "42X45",
           "DATE is an invalid type for argument number 2 of TIMESTAMPADD."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, 'XX', CURRENT_TIMESTAMP)})", "42X45",
           "CHAR is an invalid type for argument number 2 of TIMESTAMPADD."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, 1.1, CURRENT_TIMESTAMP)})", "42X45",
           "DECIMAL is an invalid type for argument number 2 of TIMESTAMPADD."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, 1, 2.1)})", "42X45",
           "DECIMAL is an invalid type for argument number 3 of TIMESTAMPADD."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, 1, 'XX')})", "42X45",
           "CHAR is an invalid type for argument number 3 of TIMESTAMPADD."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, 1)})", "42X01",
           "Syntax error: Encountered \")\" at line 1, column 44."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND)})", "42X01",
           "Syntax error: Encountered \")\" at line 1, column 41."}
    };

    private static java.sql.Timestamp ts( String s)
    {
        // Timestamp format must be yyyy-mm-dd hh:mm:ss.fffffffff
        if( s.length() < 29)
        {
            // Pad out the fraction with zeros
            StringBuffer sb = new StringBuffer( s);
            if( s.length() == 19)
                sb.append( '.');
            while( sb.length() < 29)
                sb.append( '0');
            s = sb.toString();
        }
        try
        {
            return java.sql.Timestamp.valueOf( s);
        }
        catch( Exception e)
        {
            System.out.println( s + " is not a proper timestamp string.");
            System.out.println( e.getClass().getName() + ": " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    private static java.sql.Date dt( String s)
    {
        return java.sql.Date.valueOf( s);
    }

    private static java.sql.Time tm( String s)
    {
        return java.sql.Time.valueOf( s);
    }

    private static String dateTimeToLiteral( Object ts)
    {
        if( ts instanceof java.sql.Timestamp)
            return "{ts '" + ((java.sql.Timestamp)ts).toString() + "'}";
        else if( ts instanceof java.sql.Time)
            return "{t '" + ((java.sql.Time)ts).toString() + "'}";
        else if( ts instanceof java.sql.Date)
            return "{d '" + ((java.sql.Date)ts).toString() + "'}";
        else if( ts instanceof String)
            return "TIMESTAMP( '" + ((String) ts) + "')";
        else
            return ts.toString();
    }

    public static void main( String[] args)
    {
        System.out.println("Test timestamp arithmetic starting.");
		try
        {
            timestampArith tester = new timestampArith( args);
            tester.doIt();
            if( tester.errorCount == 0)
                System.out.println( "PASSED.");
            else if( tester.errorCount == 1)
                System.out.println( "FAILED. 1 error.");
            else
                System.out.println( "FAILED. " + tester.errorCount + " errors.");
        }
        catch( SQLException sqle)
        {
            reportSQLException( sqle);
            System.exit(1);
        }
        catch( Exception e)
        {
            System.out.println("Unexpected exception: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    } // end of main

    String composeSqlStr( String fn, int interval, String parm1, String parm2)
    {
        return "values( {fn TIMESTAMP" + fn + "( " + intervalJdbcNames[interval] +
          ", " + parm1 + "," + parm2 + ")})";
    }
    
    private timestampArith( String[] args) throws Exception
    {
        // make the initial connection.
        ij.getPropertyArg(args);
        conn = ij.startJBMS();

        conn.setAutoCommit(false);
        for( int i = 0; i < intervalJdbcNames.length; i++)
        {
            tsAddPS[i] = conn.prepareStatement( composeSqlStr( "ADD", i, "?", "?"));
            tsDiffPS[i] = conn.prepareStatement( composeSqlStr( "DIFF", i, "?", "?"));
        }
        stmt = conn.createStatement();
    }

    private void doIt() throws SQLException
    {
        for( int i = 0; i < tests.length; i++)
            tests[i].runTest();

        testNullInputs();

        for( int i = 0; i < invalid.length; i++)
        {
            try
            {
                ResultSet rs = stmt.executeQuery( invalid[i][0]);
                rs.next();
                reportFailure( "\"" + invalid[i][0] + "\" did not throw an exception.");
            }
            catch( SQLException sqle)
            {
                checkExpectedException( sqle, invalid[i][1], invalid[i][2], "\"" + invalid[i][0] + "\"");
            }
        }

        testInvalidArgTypes();
    } // end of doIt

    private void testInvalidArgTypes() throws SQLException
    {
        expectException( tsDiffPS[ HOUR_INTERVAL], ts( "2005-05-11 15:26:00"), new Double( 2.0), "XCL12",
                         "An attempt was made to put a data value of type 'double' into a data value of type 'TIMESTAMP'.",
                         "TIMESTAMPDIFF with double ts2");
        expectException( tsDiffPS[ HOUR_INTERVAL], new Double( 2.0), ts( "2005-05-11 15:26:00"), "XCL12",
                         "An attempt was made to put a data value of type 'double' into a data value of type 'TIMESTAMP'.",
                         "TIMESTAMPDIFF with double ts1");

        expectException( tsAddPS[ MINUTE_INTERVAL], new Integer(1), new Integer(-1), "XCL12",
                         "An attempt was made to put a data value of type 'int' into a data value of type 'TIMESTAMP'.",
                         "TIMESTAMPADD with int ts");
        expectException( tsAddPS[ MINUTE_INTERVAL], ts( "2005-05-11 15:26:00"), ts( "2005-05-11 15:26:00"), "XCL12",
                         "An attempt was made to put a data value of type 'java.sql.Timestamp' into a data value of type 'INTEGER'.",
                         "TIMESTAMPADD with timestamp count");
    } // end of testInvalidArgTypes

    private void expectException( PreparedStatement ps, Object arg1, Object arg2,
                                  String expectedSQLState, String expectedMsg, String label)
    {
        try
        {
            ps.setObject( 1, arg1);
            ps.setObject( 2, arg2);
            ResultSet rs = ps.executeQuery();
            rs.next();
            reportFailure( label + " did not throw an exception.");
        }
        catch( SQLException sqle) { checkExpectedException( sqle, expectedSQLState, expectedMsg, label);};
    } // end of expectException

    private void checkExpectedException( SQLException sqle, String expectedSQLState, String expectedMsg, String label)
    {
        if( ! expectedSQLState.equals( sqle.getSQLState()))
            reportFailure( "Unexpected SQLState from \"" + label + "\". expected " +
                           expectedSQLState + " got " + sqle.getSQLState());
        else if( expectedMsg != null && ! expectedMsg.equals( sqle.getMessage()))
            reportFailure( "Unexpected message from \"" + label + "\".\n  expected \"" +
                           expectedMsg + "\"\n  got \"" + sqle.getMessage() + "\"");
    } // end of checkExpectedException

    private void testNullInputs() throws SQLException
    {
        // Null inputs, each position, each type
        tsDiffPS[ HOUR_INTERVAL].setTimestamp( 1, ts( "2005-05-11 15:26:00"));
        tsDiffPS[ HOUR_INTERVAL].setNull( 2, Types.TIMESTAMP);
        expectNullResult( tsDiffPS[ HOUR_INTERVAL], "TIMESTAMPDIFF with null timestamp in third argument");
        tsDiffPS[ HOUR_INTERVAL].setNull( 2, Types.DATE);
        expectNullResult( tsDiffPS[ HOUR_INTERVAL], "TIMESTAMPDIFF with null date in third argument");

        tsDiffPS[ HOUR_INTERVAL].setTimestamp( 2, ts( "2005-05-11 15:26:00"));
        tsDiffPS[ HOUR_INTERVAL].setNull( 1, Types.TIMESTAMP);
        expectNullResult( tsDiffPS[ HOUR_INTERVAL], "TIMESTAMPDIFF with null timestamp in second argument");
        tsDiffPS[ HOUR_INTERVAL].setNull( 1, Types.DATE);
        expectNullResult( tsDiffPS[ HOUR_INTERVAL], "TIMESTAMPDIFF with null date in second argument");

        tsAddPS[ MINUTE_INTERVAL].setTimestamp( 2, ts( "2005-05-11 15:26:00"));
        tsAddPS[ MINUTE_INTERVAL].setNull( 1, Types.INTEGER);
        expectNullResult( tsAddPS[ MINUTE_INTERVAL], "TIMESTAMPADD with null integer in second argument");

        tsAddPS[ MINUTE_INTERVAL].setInt( 1, 1);
        tsAddPS[ MINUTE_INTERVAL].setNull( 2, Types.TIMESTAMP);
        expectNullResult( tsAddPS[ MINUTE_INTERVAL], "TIMESTAMPADD with null timestamp in third argument");
        tsAddPS[ MINUTE_INTERVAL].setNull( 2, Types.DATE);
        expectNullResult( tsAddPS[ MINUTE_INTERVAL], "TIMESTAMPADD with null date in third argument");
    } // end of testNullInputs

    private void expectNullResult( PreparedStatement ps, String label)
    {
        try
        {
            ResultSet rs = ps.executeQuery();
            if( ! rs.next())
                reportFailure( label + " returned no rows.");
            else if( rs.getObject( 1) != null)
                reportFailure( label + " did not return null.");
        }
        catch (SQLException sqle)
        {
            reportFailure( "Unexpected exception from " + label);
            reportSQLException( sqle);
        }
    } // end of expectNullResult

    private static void reportSQLException( SQLException sqle)
    {
        System.out.println("Unexpected exception:");
        for(;;)
        {
            System.out.println( "    " + sqle.getMessage());
            if( sqle.getNextException() != null)
                sqle = sqle.getNextException();
            else
                break;
        }
        sqle.printStackTrace();
    } // end of reportSQLException

    private void reportFailure( String msg)
    {
        errorCount++;
        System.out.println( msg);
    }

    private static void setDateTime( PreparedStatement ps, int parameterIdx, java.util.Date dateTime)
        throws SQLException
    {
        if( dateTime instanceof java.sql.Timestamp)
            ps.setTimestamp( parameterIdx, (java.sql.Timestamp) dateTime);
        else if( dateTime instanceof java.sql.Date)
            ps.setDate( parameterIdx, (java.sql.Date) dateTime);
        else if( dateTime instanceof java.sql.Time)
            ps.setTime( parameterIdx, (java.sql.Time) dateTime);
        else
            ps.setTimestamp( parameterIdx, (java.sql.Timestamp) dateTime);
    }
    
    abstract class OneTest
    {
        final int interval; // FRAC_SECOND_INTERVAL, SECOND_INTERVAL, ... or YEAR_INTERVAL
        final String expectedSQLState; // Null if no SQLException is expected
        final String expectedMsg; // Null if no SQLException is expected
        String sql;
        
        OneTest( int interval, String expectedSQLState, String expectedMsg)
        {
            this.interval = interval;
            this.expectedSQLState = expectedSQLState;
            this.expectedMsg = expectedMsg;
        }
        
        void runTest()
        {
            sql = composeSQL();
            ResultSet rs = null;
            try
            {
                rs = stmt.executeQuery( sql);
                checkResultSet( rs, sql);
                if( expectedSQLState != null)
                    reportFailure( "Statement '" + sql + "' did not generate an exception");
            }
            catch( SQLException sqle)
            {
                checkSQLException( "Statement", sqle);
            }
            if( rs != null)
            {
                try
                {
                    rs.close();
                }
                catch( SQLException sqle){};
                rs = null;
            }
            
            try
            {
                rs = executePS();
                checkResultSet( rs, sql);
                if( expectedSQLState != null)
                    reportFailure( "PreparedStatement '" + sql + "' did not generate an exception");
            }
            catch( SQLException sqle)
            {
                checkSQLException( "PreparedStatement", sqle);
            }
            if( rs != null)
            {
                try
                {
                    rs.close();
                }
                catch( SQLException sqle){};
                rs = null;
            }
        } // end of RunTest

        private void checkResultSet( ResultSet rs, String sql) throws SQLException
        {
            if( rs.next())
            {
                checkResultRow( rs, sql);
                if( rs.next())
                    reportFailure( "'" + sql + "' returned more than one row.");
            }
            else
                reportFailure( "'" + sql + "' did not return any rows.");
        } // end of checkResultSet

        private void checkSQLException( String type, SQLException sqle)
        {
            if( expectedSQLState != null)
            {
                if( ! expectedSQLState.equals( sqle.getSQLState()))
                    reportFailure( "Incorrect SQLState from " + type + " '" + sql + "' expected " + expectedSQLState +
                                   " got " + sqle.getSQLState());
                else if( expectedMsg != null && ! expectedMsg.equals( sqle.getMessage()))
                    reportFailure( "Incorrect exception message from " + type + " '" + sql + "' expected '" + expectedMsg +
                                   "' got '" + sqle.getMessage() + "'");
            }
            else
            {
                reportFailure( "Unexpected exception from " + type + " '" + sql + "'");
                reportSQLException( sqle);
            }
        } // end of checkSQLException

        abstract String composeSQL();

        abstract void checkResultRow( ResultSet rs, String sql) throws SQLException;

        abstract ResultSet executePS() throws SQLException;
    }

    class OneDiffTest extends OneTest
    {
        private final java.util.Date ts1;
        private final java.util.Date ts2;
        final int expectedDiff;
        protected boolean expectNull;

        OneDiffTest( int interval,
                     java.util.Date ts1,
                     java.util.Date ts2,
                     int expectedDiff,
                     String expectedSQLState,
                     String expectedMsg)
        {
            super( interval, expectedSQLState, expectedMsg);
            this.ts1 = ts1;
            this.ts2 = ts2;
            this.expectedDiff = expectedDiff;
            expectNull = (ts1 == null) || (ts2 == null);
        }

        String composeSQL()
        {
            return composeSqlStr( "DIFF", interval, dateTimeToLiteral( ts1), dateTimeToLiteral( ts2));
        }
        
        void checkResultRow( ResultSet rs, String sql) throws SQLException
        {
            int actualDiff = rs.getInt(1);
            if( rs.wasNull())
            {
                if( !expectNull)
                    reportFailure( "Unexpected null result from '" + sql + "'.");
            }
            else
            {
                if( expectNull)
                    reportFailure( "Expected null result from '" + sql + "'.");
                else if( actualDiff != expectedDiff)
                    reportFailure( "Unexpected result from '" + sql + "'.  Expected " +
                        expectedDiff + " got " + actualDiff + ".");
            }
        }

        ResultSet executePS() throws SQLException
        {
            setDateTime( tsDiffPS[interval], 1, ts1);
            setDateTime( tsDiffPS[interval], 2, ts2);
            return tsDiffPS[interval].executeQuery();
        }
    } // end of class OneDiffTest

    class OneStringDiffTest extends OneDiffTest
    {
        private final String ts1;
        private final String ts2;

        OneStringDiffTest( int interval,
                           String ts1,
                           String ts2,
                           int expectedDiff,
                           String expectedSQLState,
                           String expectedMsg)
        {
            super( interval, (java.util.Date) null, (java.util.Date) null, expectedDiff, expectedSQLState, expectedMsg);
            this.ts1 = ts1;
            this.ts2 = ts2;
            expectNull = (ts1 == null) || (ts2 == null);
        }

        String composeSQL()
        {
            return composeSqlStr( "DIFF", interval, dateTimeToLiteral( ts1), dateTimeToLiteral( ts2));
        }

        ResultSet executePS() throws SQLException
        {
            tsDiffPS[interval].setString( 1, ts1);
            tsDiffPS[interval].setString( 2, ts2);
            return tsDiffPS[interval].executeQuery();
        }
    } // end of class OneStringDiffTest

    class OneAddTest extends OneTest
    {
        private final java.util.Date ts;
        final int count;
        final java.sql.Timestamp expected;

        OneAddTest( int interval,
                    int count,
                    java.util.Date ts,
                    java.sql.Timestamp expected,
                    String expectedSQLState,
                    String expectedMsg)
        {
            super( interval, expectedSQLState, expectedMsg);
            this.count = count;
            this.ts = ts;
            this.expected = expected;
        }

        String composeSQL()
        {
            return composeSqlStr( "ADD", interval, String.valueOf( count), dateTimeToLiteral( ts));
        }

        void checkResultRow( ResultSet rs, String sql) throws SQLException
        {
            java.sql.Timestamp actual = rs.getTimestamp( 1);
            if( rs.wasNull() || actual == null)
            {
                if( expected != null)
                    reportFailure( "Unexpected null result from '" + sql + "'.");
            }
            else
            {
                if( expected == null)
                    reportFailure( "Expected null result from '" + sql + "'.");
                else if( ! actual.equals( expected))
                    reportFailure( "Unexpected result from '" + sql + "'.  Expected " +
                                   expected.toString() + " got " + actual.toString() + ".");
            }
        }

        ResultSet executePS() throws SQLException
        {
            tsAddPS[interval].setInt( 1, count);
            setDateTime( tsAddPS[interval], 2, ts);
            return tsAddPS[interval].executeQuery();
        }
    } // end of class OneAddTest

    class OneStringAddTest extends OneAddTest
    {
        private final String ts;

        OneStringAddTest( int interval,
                          int count,
                          String ts,
                          java.sql.Timestamp expected,
                          String expectedSQLState,
                          String expectedMsg)
        {
            super( interval, count, (java.util.Date) null, expected, expectedSQLState, expectedMsg);
            this.ts = ts;
        }

        String composeSQL()
        {
            return composeSqlStr( "ADD", interval, String.valueOf( count), dateTimeToLiteral( ts));
        }

        ResultSet executePS() throws SQLException
        {
            tsAddPS[interval].setInt( 1, count);
            tsAddPS[interval].setString( 2, ts);
            return tsAddPS[interval].executeQuery();
        }
    } // end of class OneStringAddTest
}
