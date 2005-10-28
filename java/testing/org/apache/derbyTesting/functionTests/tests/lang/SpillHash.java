/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.bug4356

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.BitSet;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Test BackingStoreHashtable spilling to disk.
 * BackingStoreHashtable is used to implement hash joins, distinct, scroll insensitive cursors,
 * outer joins, and the HAVING clause.
 */
public class SpillHash
{
    private static PreparedStatement joinStmt;
    private static PreparedStatement distinctStmt;
    private static final int LOTS_OF_ROWS = 10000;
    private static int errorCount = 0;
    
    public static void main (String args[]) 
    {
        try {
            /* Load the JDBC Driver class */
            // use the ij utility to read the property file and
            // make the initial connection.
            ij.getPropertyArg(args);
            Connection conn = ij.startJBMS();
            Statement stmt = conn.createStatement();

            for( int i = 0; i < prep.length; i++)
                stmt.executeUpdate( prep[i]);
            PreparedStatement insA = conn.prepareStatement( "insert into ta(ca1,ca2) values(?,?)");
            PreparedStatement insB = conn.prepareStatement( "insert into tb(cb1,cb2) values(?,?)");
            insertDups( insA, insB, initDupVals);

            joinStmt =
              conn.prepareStatement( "select ta.ca1, ta.ca2, tb.cb2 from ta, tb where ca1 = cb1");
            distinctStmt =
              conn.prepareStatement( "select distinct ca1 from ta");

            runStatements( conn, 0, new String[][][] {initDupVals});

            System.out.println( "Growing database.");
            
            // Add a lot of rows so that the hash tables have to spill to disk
            conn.setAutoCommit(false);
            for( int i = 1; i <= LOTS_OF_ROWS; i++)
            {
                insA.setInt(1, i);
                insA.setString(2, ca2Val(i));
                insA.executeUpdate();
                insB.setInt(1, i);
                insB.setString(2, cb2Val(i));
                insB.executeUpdate();

                if( (i & 0xff) == 0)
                    conn.commit();
            }
            conn.commit();
            insertDups( insA, insB, spillDupVals);
            conn.commit();

            conn.setAutoCommit(true);
            runStatements( conn, LOTS_OF_ROWS, new String[][][] {initDupVals, spillDupVals});
            
            conn.close();
        } catch (Exception e) {
            System.out.println("FAIL -- unexpected exception "+e);
            JDBCDisplayUtil.ShowException(System.out, e);
            e.printStackTrace();
            errorCount++;
        }
        if( errorCount == 0)
        {
            System.out.println( "PASSED.");
            System.exit(0);
        }
        else
        {
            System.out.println( "FAILED: " + errorCount + ((errorCount == 1) ? " error" : " errors"));
            System.exit(1);
        }
    } // end of main
    
    private static final String[] prep =
    {
        "create table ta (ca1 integer, ca2 char(200))",
        "create table tb (cb1 integer, cb2 char(200))",
        "insert into ta(ca1,ca2) values(null, 'Anull')",
        "insert into tb(cb1,cb2) values(null, 'Bnull')"
    };

    private static final String[][] initDupVals =
    {
        { "0a", "0b"},
        { "1a", "1b"},
        { "2a"}
    };
    private static final String[][] spillDupVals =
    {
        {},
        { "1c"},
        { "2b"},
        { "3a", "3b", "3c"}
    };

    private static int expectedMincc2( int cc1)
    {
        return 4*cc1;
    }

    private static int expectedMaxcc2( int cc1)
    {
        return expectedMincc2( cc1) + (cc1 & 0x3);
    }
    
    private static void insertDups( PreparedStatement insA, PreparedStatement insB, String[][] dupVals)
        throws SQLException
    {
        for( int i = 0; i < dupVals.length; i++)
        {
            insA.setInt(1, -i);
            insB.setInt(1, -i);
            String[] vals = dupVals[i];
            for( int j = 0; j < vals.length; j++)
            {
                insA.setString( 2, "A" + vals[j]);
                insA.executeUpdate();
                insB.setString( 2, "B" + vals[j]);
                insB.executeUpdate();
            }
        }
    } // end of insertDups
    
    private static String ca2Val( int col1Val)
    {
        return "A" + col1Val;
    }
    
    private static String cb2Val( int col1Val)
    {
        return "B" + col1Val;
    }
    
    private static void runStatements( Connection conn, int maxColValue, String[][][] dupVals)
        throws SQLException
    {
        runJoin( conn, maxColValue, dupVals);
        runDistinct( conn, maxColValue, dupVals);
        runCursor( conn, maxColValue, dupVals);
    }

    private static void runJoin( Connection conn, int maxColValue, String[][][] dupVals)
        throws SQLException
    {
        System.out.println( "Running join");
        int expectedRowCount = maxColValue; // plus expected duplicates, to be counted below
        ResultSet rs = joinStmt.executeQuery();
        BitSet joinRowFound = new BitSet( maxColValue);
        int dupKeyCount = 0;
        for( int i = 0; i < dupVals.length; i++)
        {
            if( dupVals[i].length > dupKeyCount)
                dupKeyCount = dupVals[i].length;
        }
        BitSet[] dupsFound = new BitSet[dupKeyCount];
        int[] dupCount = new int[ dupKeyCount];
        for( int i = 0; i < dupKeyCount; i++)
        {
            // count the number of rows with column(1) == -i
            dupCount[i] = 0;
            for( int j = 0; j < dupVals.length; j++)
            {
                if( i < dupVals[j].length)
                    dupCount[i] += dupVals[j][i].length;
            }
            dupsFound[i] = new BitSet(dupCount[i]*dupCount[i]);
            expectedRowCount += dupCount[i]*dupCount[i];
        }
        
        int count;
        for( count = 0; rs.next(); count++)
        {
            int col1Val = rs.getInt(1);
            if( rs.wasNull())
            {
                System.out.println( "Null in join column.");
                errorCount++;
                continue;
            }
            if( col1Val > maxColValue)
            {
                System.out.println( "Invalid value in first join column.");
                errorCount++;
                continue;
            }
            if( col1Val > 0)
            {
                if( joinRowFound.get( col1Val - 1))
                {
                    System.out.println( "Multiple rows for value " + col1Val);
                    errorCount++;
                }
                joinRowFound.set( col1Val - 1);
                String col2Val = trim( rs.getString(2));
                String col3Val = trim( rs.getString(3));
                if( !( ca2Val( col1Val).equals( col2Val) && cb2Val( col1Val).equals( col3Val)))
                {
                    System.out.println( "Incorrect value in column 2 or 3 of join.");
                    errorCount++;
                }
            }
            else // col1Val <= 0, there are duplicates in the source tables
            {
                int dupKeyIdx = -col1Val;
                int col2Idx = findDupVal( rs, 2, 'A', dupKeyIdx, dupVals);
                int col3Idx = findDupVal( rs, 3, 'B', dupKeyIdx, dupVals);
                if( col2Idx < 0 || col3Idx < 0)
                    continue;

                int idx = col2Idx + dupCount[dupKeyIdx]*col3Idx;
                if( dupsFound[dupKeyIdx].get( idx))
                {
                    System.out.println( "Repeat of row with key value 0");
                    errorCount++;
                }
                dupsFound[dupKeyIdx].set( idx);
            }
        };
        if( count != expectedRowCount)
        {
            System.out.println( "Incorrect number of rows in join.");
            errorCount++;
        }
        rs.close();
    } // end of runJoin

    private static int findDupVal( ResultSet rs, int col, char prefix, int keyIdx, String[][][] dupVals)
        throws SQLException
    {
        String colVal = rs.getString(col);
        if( colVal != null && colVal.length() > 1 || colVal.charAt(0) == prefix)
        {
            colVal = trim( colVal.substring( 1));
            int dupIdx = 0;
            for( int i = 0; i < dupVals.length; i++)
            {
                if( keyIdx < dupVals[i].length)
                {
                    for( int j = 0; j < dupVals[i][keyIdx].length; j++, dupIdx++)
                    {
                        if( colVal.equals( dupVals[i][keyIdx][j]))
                            return dupIdx;
                    }
                }
            }
        }
        System.out.println( "Incorrect value in column " + col + " of join with duplicate keys.");
        errorCount++;
        return -1;
    } // end of findDupVal
        
    private static String trim( String str)
    {
        if( str == null)
            return str;
        return str.trim();
    }
    
    private static void runDistinct( Connection conn, int maxColValue, String[][][] dupVals)
        throws SQLException
    {
        System.out.println( "Running distinct");
        ResultSet rs = distinctStmt.executeQuery();
        checkAllCa1( rs, false, false, maxColValue, dupVals, "DISTINCT");
    }

    private static void checkAllCa1( ResultSet rs,
                                     boolean expectDups,
                                     boolean holdOverCommit,
                                     int maxColValue,
                                     String[][][] dupVals,
                                     String label)
        throws SQLException
    {
        int dupKeyCount = 0;
        for( int i = 0; i < dupVals.length; i++)
        {
            if( dupVals[i].length > dupKeyCount)
                dupKeyCount = dupVals[i].length;
        }
        int[] expectedDupCount = new int[dupKeyCount];
        int[] dupFoundCount = new int[dupKeyCount];
        for( int i = 0; i < dupKeyCount; i++)
        {
            
            dupFoundCount[i] = 0;
            if( !expectDups)
                expectedDupCount[i] = 1;
            else
            {
                expectedDupCount[i] = 0;
                for( int j = 0; j < dupVals.length; j++)
                {
                    if( i < dupVals[j].length)
                        expectedDupCount[i] += dupVals[j][i].length;
                }
            }
        }
        BitSet found = new BitSet( maxColValue);
        int count = 0;
        boolean nullFound = false;
        try
        {
            for( count = 0; rs.next();)
            {
                int col1Val = rs.getInt(1);
                if( rs.wasNull())
                {
                    if( nullFound)
                    {
                        System.out.println( "Too many nulls returned by " + label);
                        errorCount++;
                        continue;
                    }
                    nullFound = true;
                    continue;
                }
                if( col1Val <= -dupKeyCount || col1Val > maxColValue)
                {
                    System.out.println( "Invalid value returned by " + label);
                    errorCount++;
                    continue;
                }
                if( col1Val <= 0)
                {
                    dupFoundCount[ -col1Val]++;
                    if( !expectDups)
                    {
                        if( dupFoundCount[ -col1Val] > 1)
                        {
                            System.out.println( label + " returned a duplicate.");
                            errorCount++;
                            continue;
                        }
                    }
                    else if( dupFoundCount[ -col1Val] > expectedDupCount[ -col1Val])
                    {
                        System.out.println( label + " returned too many duplicates.");
                        errorCount++;
                        continue;
                    }
                }
                else
                {
                    if( found.get( col1Val))
                    {
                        System.out.println( label + " returned a duplicate.");
                        errorCount++;
                        continue;
                    }
                    found.set( col1Val);
                    count++;
                }
                if( holdOverCommit)
                {
                    rs.getStatement().getConnection().commit();
                    holdOverCommit = false;
                }
            }
            if( count != maxColValue)
            {
                System.out.println( "Incorrect number of rows in " + label);
                errorCount++;
            }
            for( int i = 0; i < dupFoundCount.length; i++)
            {
                if( dupFoundCount[i] != expectedDupCount[i])
                {
                    System.out.println( "A duplicate key row is missing in " + label);
                    errorCount++;
                    break;
                }
            }
        }
        finally
        {
            rs.close();
        }
    } // End of checkAllCa1

    private static void runCursor( Connection conn, int maxColValue, String[][][] dupVals)
        throws SQLException
    {
        System.out.println( "Running scroll insensitive cursor");
        DatabaseMetaData dmd = conn.getMetaData();
        boolean holdOverCommit = dmd.supportsOpenCursorsAcrossCommit();
        Statement stmt;
        if( holdOverCommit)
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY,
                                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
        else
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery( "SELECT ca1 FROM ta");
        checkAllCa1( rs, true, holdOverCommit, maxColValue, dupVals, "scroll insensitive cursor");
    }
}
