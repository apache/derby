/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.TestDiskHashtable

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.BitSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Vector;

import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.DiskHashtable;
import org.apache.derby.iapi.store.access.KeyHasher;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 * This program tests the org.apache.derby.iapi.store.access.DiskHashtable class.
 * The unit test interface is not used because that is undocumented and very difficult to decipher.
 * Furthermore it is difficult to diagnose problems when using the unit test interface.
 *
 * Created: Wed Feb 09 15:44:12 2005
 *
 * @author <a href="mailto:klebanof@us.ibm.com">Jack Klebanoff</a>
 * @version 1.0
 */
public class TestDiskHashtable 
{
    private TransactionController tc;
    private int failed = 0;
    
    public static void main( String args[])
    {
        int failed = 1;

		REPORT("Test DiskHashtable starting");
        try
        {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			Connection conn = ij.startJBMS();
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE FUNCTION testDiskHashtable() returns INTEGER EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.store.TestDiskHashtable.runTests' LANGUAGE JAVA PARAMETER STYLE JAVA");
            ResultSet rs = stmt.executeQuery( "values( testDiskHashtable())");
            if( rs.next())
                failed = rs.getInt(1);
            stmt.close();
            conn.close();
        }
        catch( SQLException e)
        {
			TestUtil.dumpSQLExceptions( e);
            failed = 1;
        }
        catch( Throwable t)
        {
			REPORT("FAIL -- unexpected exception:" + t.toString());
            failed = 1;
		}
        REPORT( (failed == 0) ? "OK" : "FAILED");
        System.exit( (failed == 0) ? 0 : 1);
    }

    private void REPORT_FAILURE(String msg)
    {
        failed = 1;
        REPORT( msg);
    }
    
    private static void REPORT(String msg)
    {
        System.out.println( msg);
    }
    
    public static int runTests() throws SQLException
    {
        TestDiskHashtable tester = new TestDiskHashtable();
        return tester.doIt();
    }

    private TestDiskHashtable() throws SQLException
    {
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        if( lcc == null)
            throw new SQLException( "Cannot get the LCC");
        tc = lcc.getTransactionExecute();
    }

    private int doIt() throws SQLException
    {
		try {


            REPORT( "Starting single key, keep duplicates test");
            testOneVariant( tc, false, singleKeyTemplate, singleKeyCols, singleKeyRows);
            REPORT( "Starting single key, remove duplicates test");
            testOneVariant( tc, true, singleKeyTemplate, singleKeyCols, singleKeyRows);
            REPORT( "Starting multiple key, keep duplicates test");
            testOneVariant( tc, false, multiKeyTemplate, multiKeyCols, multiKeyRows);
            REPORT( "Starting multiple key, remove duplicates test");
            testOneVariant( tc, true, multiKeyTemplate, multiKeyCols, multiKeyRows);

			tc.commit();
		}
		catch (StandardException se)
		{
            throw PublicAPI.wrapStandardException( se);
        }
        return failed;
    } // end of doIt

    private static final DataValueDescriptor[] singleKeyTemplate = { new SQLInteger(), new SQLVarchar()};
    private static final int[] singleKeyCols = {0};
    private static final DataValueDescriptor[][] singleKeyRows =
    {
        {new SQLInteger(1), new SQLVarchar("abcd")},
        {new SQLInteger(2), new SQLVarchar("abcd")},
        {new SQLInteger(3), new SQLVarchar("e")},
        {new SQLInteger(1), new SQLVarchar("zz")}
    };

    private static final DataValueDescriptor[] multiKeyTemplate = { new SQLLongint(), new SQLVarchar(), new SQLInteger()};
    private static final int[] multiKeyCols = {1, 0};
    private static final DataValueDescriptor[][] multiKeyRows =
    {
        {new SQLLongint(1), new SQLVarchar( "aa"), multiKeyTemplate[2].getNewNull()},
        {new SQLLongint(2), new SQLVarchar( "aa"), new SQLInteger(1)},
        {new SQLLongint(2), new SQLVarchar( "aa"), new SQLInteger(2)},
        {new SQLLongint(2), new SQLVarchar( "b"), new SQLInteger(1)}
    };

    private static final int LOTS_OF_ROWS_COUNT = 50000;
    
    private void testOneVariant( TransactionController tc,
                                 boolean removeDups,
                                 DataValueDescriptor[] template,
                                 int[] keyCols,
                                 DataValueDescriptor[][] rows)
        throws StandardException
    {
        DiskHashtable dht = new DiskHashtable(tc, template, keyCols, removeDups, false);
        boolean[] isDuplicate = new boolean[ rows.length];
        boolean[] found = new boolean[ rows.length];
        HashMap simpleHash = new HashMap( rows.length);

        testElements( removeDups, dht, keyCols, 0, rows, simpleHash, isDuplicate, found);

        for( int i = 0; i < rows.length; i++)
        {
            Object key = KeyHasher.buildHashKey( rows[i], keyCols);
            Vector al = (Vector) simpleHash.get( key);
            isDuplicate[i] = (al != null);
            if( al == null)
            {
                al = new Vector(4);
                simpleHash.put( key, al);
            }
            if( (!removeDups) || !isDuplicate[i])
                al.add( rows[i]);
            
            if( dht.put( key, rows[i]) != (removeDups ? (!isDuplicate[i]) : true))
                REPORT_FAILURE( "  put returned wrong value on row " + i);

            for( int j = 0; j <= i; j++)
            {
                key = KeyHasher.buildHashKey( rows[j], keyCols);
                if( ! rowsEqual( dht.get( key), simpleHash.get( key)))
                    REPORT_FAILURE( "  get returned wrong value on key " + j);
            }

            testElements( removeDups, dht, keyCols, i+1, rows, simpleHash, isDuplicate, found);
        }
        // Remove them
        for( int i = 0; i < rows.length; i++)
        {
            Object key = KeyHasher.buildHashKey( rows[i], keyCols);
            if( ! rowsEqual( dht.remove( key), simpleHash.get( key)))
                REPORT_FAILURE( "  remove returned wrong value on key " + i);
            simpleHash.remove( key);
            if( dht.get( key) != null)
                REPORT_FAILURE( "  remove did not delete key " + i);
        }
        testElements( removeDups, dht, keyCols, 0, rows, simpleHash, isDuplicate, found);

        testLargeTable( dht, keyCols, rows[0]);
        dht.close();
    } // end of testOneVariant

    private void testLargeTable( DiskHashtable dht,
                                 int[] keyCols,
                                 DataValueDescriptor[] aRow)
        throws StandardException
    {
        // Add a lot of elements
        // If there are two or more key columns then we will vary the first two key columns, using an approximately
        // square matrix of integer key values. Because the hash generator is commutative key (i,j) hashes into the
        // same bucket as key (j,i), testing the case where different keys hash into the same bucket.
        int key1Count = (keyCols.length > 1) ? ((int) Math.round( Math.sqrt( (double) LOTS_OF_ROWS_COUNT))) : 1;
        int key0Count = (LOTS_OF_ROWS_COUNT + key1Count - 1)/key1Count;

        DataValueDescriptor[] row = new DataValueDescriptor[ aRow.length];
        for( int i = 0; i < row.length; i++)
            row[i] = aRow[i].getClone();
        
        for( int key0Idx = 0; key0Idx < key0Count; key0Idx++)
        {
            row[ keyCols[0]].setValue( key0Idx);
            for( int key1Idx = 0; key1Idx < key1Count; key1Idx++)
            {
                if( keyCols.length > 1)
                    row[ keyCols[1]].setValue( key1Idx);
                Object key = KeyHasher.buildHashKey( row, keyCols);
                if( ! dht.put( key, row))
                {
                    REPORT_FAILURE( "  put returned wrong value for key(" + key0Idx + "," + key1Idx + ")");
                    key0Idx = key0Count;
                    break;
                }
            }
        }
        for( int key0Idx = 0; key0Idx < key0Count; key0Idx++)
        {
            row[ keyCols[0]].setValue( key0Idx);
            for( int key1Idx = 0; key1Idx < key1Count; key1Idx++)
            {
                if( keyCols.length > 1)
                    row[ keyCols[1]].setValue( key1Idx);
                Object key = KeyHasher.buildHashKey( row, keyCols);
                if( ! rowsEqual( dht.get( key), row))
                {
                    REPORT_FAILURE( "  large table get returned wrong value for key(" + key0Idx + "," + key1Idx + ")");
                    key0Idx = key0Count;
                    break;
                }
            }
        }
        BitSet found = new BitSet(key0Count * key1Count);
        Enumeration elements = dht.elements();
        while( elements.hasMoreElements())
        {
            Object el = elements.nextElement();
            if( ! (el instanceof DataValueDescriptor[]))
            {
                REPORT_FAILURE( "  large table enumeration returned wrong element type");
                break;
            }
            DataValueDescriptor[] fetchedRow = (DataValueDescriptor[]) el;
            
            int i = fetchedRow[ keyCols[0]].getInt() * key1Count;
            if( keyCols.length > 1)
                i += fetchedRow[ keyCols[1]].getInt();
            if( i >= key0Count * key1Count)
            {
                REPORT_FAILURE( "  large table enumeration returned invalid element");
                break;
            }
                
            if( found.get(i))
            {
                REPORT_FAILURE( "  large table enumeration returned same element twice");
                break;
            }
            found.set(i);
        }
        for( int i = key0Count * key1Count - 1; i >= 0; i--)
        {
            if( !found.get(i))
            {
                REPORT_FAILURE( "  large table enumeration missed at least one element");
                break;
            }
        }
    } // end of testLargeTable

    private void testElements( boolean removeDups,
                               DiskHashtable dht,
                               int[] keyCols,
                               int rowCount,
                               DataValueDescriptor[][] rows,
                               HashMap simpleHash,
                               boolean[] isDuplicate,
                               boolean[] found)
        throws StandardException
    {
        for( int i = 0; i < rowCount; i++)
            found[i] = false;
        
        for( Enumeration e = dht.elements(); e.hasMoreElements();)
        {
            Object el = e.nextElement();
            if( el == null)
            {
                REPORT_FAILURE( "  table enumeration returned a null element");
                return;
            }
            if( el instanceof DataValueDescriptor[])
                checkElement( (DataValueDescriptor[]) el, rowCount, rows, found);
            else if( el instanceof Vector)
            {
                Vector v = (Vector) el;
                for( int i = 0; i < v.size(); i++)
                    checkElement( (DataValueDescriptor[]) v.get(i), rowCount, rows, found);
            }
            else if( el == null)
            {
                REPORT_FAILURE( "  table enumeration returned an incorrect element type");
                return;
            }
        }
        for( int i = 0; i < rowCount; i++)
        {
            if( (removeDups && isDuplicate[i]))
            {
                if( found[i])
                {
                    REPORT_FAILURE( "  table enumeration did not remove duplicates");
                    return;
                }
            }
            else if( ! found[i])
            {
                REPORT_FAILURE( "  table enumeration missed at least one element");
                return;
            }
        }
    } // end of testElements

    private void checkElement( DataValueDescriptor[] fetchedRow,
                               int rowCount,
                               DataValueDescriptor[][] rows,
                               boolean[] found)
        throws StandardException
    {
        for( int i = 0; i < rowCount; i++)
        {
            if( rowsEqual( fetchedRow, rows[i]))
            {
                if( found[i])
                {
                    REPORT_FAILURE( "  table enumeration returned the same element twice");
                    return;
                }
                found[i] = true;
                return;
            }
        }
        REPORT_FAILURE( "  table enumeration returned an incorrect element");
    } // end of checkElement

    private boolean rowsEqual( Object r1, Object r2)
        throws StandardException
    {
        if( r1 == null)
            return r2 == null;

        if( r1 instanceof DataValueDescriptor[])
        {
            DataValueDescriptor[] row1 = (DataValueDescriptor[]) r1;
            DataValueDescriptor[] row2;
            
            if( r2 instanceof Vector)
            {
                Vector v2 = (Vector) r2;
                if( v2.size() != 1)
                    return false;
                row2 = (DataValueDescriptor[]) v2.elementAt(0);
            }
            else if( r2 instanceof DataValueDescriptor[])
                row2 = (DataValueDescriptor[]) r2;
            else
                return false;
            
            if( row1.length != row2.length)
                return false;
            for( int i = 0; i < row1.length; i++)
            {
                if( ! row1[i].compare( Orderable.ORDER_OP_EQUALS, row2[i], true, true))
                    return false;
            }
            return true;
        }
        if( r1 instanceof Vector)
        {
            if( !(r2 instanceof Vector))
                return false;
            Vector v1 = (Vector) r1;
            Vector v2 = (Vector) r2;
            if( v1.size() != v2.size())
                return false;
            for( int i = v1.size() - 1; i >= 0; i--)
            {
                if( ! rowsEqual( v1.elementAt( i), v2.elementAt(i)))
                    return false;
            }
            return true;
        }
        // What is it then?
        return r1.equals( r2);
    } // end of rowsEqual
}
