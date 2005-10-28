/*

   Derby - Class org.apache.derby.impl.services.locks.Timeout

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.services.locks;

import org.apache.derby.impl.services.locks.TableNameInfo;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.VirtualLockTable;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.util.CheapDateFormatter;

import java.util.Enumeration;
import java.util.Hashtable;

/**
 * Code to support Timeout error output.
 * @author gavin
 */

public final class Timeout
{
    public static final int TABLE_AND_ROWLOCK = VirtualLockTable.TABLE_AND_ROWLOCK;
    public static final int ALL = VirtualLockTable.ALL;

    public static final String newline = "\n";
    //FIXME: The newline might not be truely platform independent.
    // We do not want to use a system call because of security reasons.
    // LINE_SEPARATOR returns ^M for some reason, not ^M<nl>.
    //public static final String newline = String.valueOf( (char)(new Byte(Character.LINE_SEPARATOR)).intValue() );
    //public static final String newline = System.getProperty( "line.separator" );

    private TransactionController tc;
    private TableNameInfo tabInfo;

    /* the current Latch to extract info out of */
    private Latch currentLock;
    /* the current row output of the lockTable */
    private char[] outputRow;
    /* the entire lockTable as a buffer */
    private StringBuffer sb;
    /* the hashtable information of the current lock */
    private Hashtable currentRow;
    /* the time when the exception was thrown */
    private final long currentTime;
    /* the snapshot of the lockTable that timeout */
    private final Enumeration lockTable;
    
    // column1: XID varchar(10) not null
    // column2: TYPE varchar(13) not null
    // column3: MODE varchar(4) not null
    // column4: LOCKCOUNT varchar(9) not null 
    // column5: LOCKNAME varchar(80) not null
    // column6: STATE varchar(5) not null
    // column7: TABLETYPE varchar(38) not null                  / LOCKOBJ varchar(38)
    // column8: INDEXNAME varchar(50) nullable as String "NULL" / CONTAINER_ID / MODE (latch only) varchar(50)
    // column9: TABLENAME varchar(38) not null                  / CONGLOM_ID varchar(38)
    // Total length of this string is 10+1+13+1+6+1+9+1+80+1+5+1+38+1+48+1+38=256
    private final static String[] column = new String[9];
    private final static int LENGTHOFTABLE;
    static
    {
        column[0] = "XID       ";
        column[1] = "TYPE         ";
        column[2] = "MODE";
        column[3] = "LOCKCOUNT";
        column[4] = "LOCKNAME                                                                        ";
        column[5] = "STATE";
        column[6] = "TABLETYPE / LOCKOBJ                   ";
        column[7] = "INDEXNAME / CONTAINER_ID / (MODE for LATCH only)  ";
        column[8] = "TABLENAME / CONGLOM_ID                ";

        int length = 0;
        for( int i = 0 ; i < column.length; i++ )
        {
            length += column[i].length();
        }
        length += column.length; // for the separator
        if( SanityManager.DEBUG )
        {   // 256 is a good number, can be expanded or contracted if necessary
            SanityManager.ASSERT( length == 256, "TIMEOUT_MONITOR: length of the row is not 256" );
        }
        LENGTHOFTABLE = length;
    }
    private final static char LINE = '-';
    private final static char SEPARATOR = '|';

    /**
     * Constructor
     * @param myTimeoutLock The Latch that the timeout happened on
     * @param myLockTable
     * @param time The time when the lockTable was cloned.
     */
    private Timeout( Latch myTimeoutLock, Enumeration myLockTable, long time )
    {
        currentLock = myTimeoutLock;
        lockTable = myLockTable;
        currentTime = time;

        if( SanityManager.DEBUG )
        {
            SanityManager.ASSERT( currentTime > 0, "TIMEOUT_MONITOR: currentTime is not set correctly" );
        }
    }
    
    /**
     * createException creates a StandardException based on:
     *          currentLock
     *          a snapshot of the lockTable
     * @return StandardException The exception with the lockTable snapshot in it
     */
    private StandardException createException()
    {
        try
        {
            buildLockTableString();
        }
        catch( StandardException se )
        {
            return se;
        }

        StandardException se = StandardException.newException( SQLState.LOCK_TIMEOUT_LOG, sb.toString() );
        se.setReport( StandardException.REPORT_ALWAYS );
        return se;
    }

    /**
     * buildLockTableString creates a LockTable info String
     */
    private String buildLockTableString() throws StandardException
    {
        sb = new StringBuffer(8192);
        outputRow = new char[ LENGTHOFTABLE ];
        int i; // counter

        // need language here to print out tablenames
        LanguageConnectionContext lcc = (LanguageConnectionContext)
            ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
        if( lcc != null )
            tc = lcc.getTransactionExecute();

        try
        {
            tabInfo = new TableNameInfo( lcc, true );
        }
        catch (Exception se)
        {   //just don't do anything
        }

        sb.append( newline );
        sb.append(CheapDateFormatter.formatDate(currentTime));
        sb.append( newline );
        for( i = 0; i < column.length; i++ )
        {
            sb.append( column[i] );
            sb.append( SEPARATOR );
        }
        sb.append( newline );

        for( i = 0; i < LENGTHOFTABLE; i++ )
            sb.append( LINE );

        sb.append( newline );

        // get the timeout lock info
        if( currentLock != null )
        {
            dumpLock( );
            if( timeoutInfoHash() )
            {
                sb.append( "*** The following row is the victim ***" );
                sb.append( newline );
                sb.append( outputRow );
                sb.append( newline );
                sb.append( "*** The above row is the victim ***" );
                sb.append( newline );
            }
            else
            {
                sb.append( "*** A victim was chosen, but it cannot be printed because the lockable object, " + currentLock + ", does not want to participate ***" );
                sb.append( newline );
            }
        }

        // get lock info from the rest of the table
        if( lockTable != null )
        {
            while( lockTable.hasMoreElements() )
            {
                currentLock = (Latch)lockTable.nextElement();
                dumpLock( );
                if( timeoutInfoHash() )
                {
                    sb.append( outputRow );
                    sb.append( newline );
                }
                else
                {
                    sb.append( "*** A latch/lock, " + currentLock + ", exist in the lockTable that cannot be printed ***" );
                    sb.append( newline );
                }
            }
            for( i = 0; i < LENGTHOFTABLE; i++ )
                sb.append( LINE );

            sb.append( newline );
        }
        return sb.toString();
    }

    /**
     * The static entry way to get the LockTable in the system.
     * @param timeoutLock The Latch that the timeout happened on
     * @param table The lockTable
     * @param time The time when the lockTable was cloned
     * @return StandardException The exception with the lockTable snapshot in it
     */
    static StandardException buildException( Latch timeoutLock, Enumeration table, long time )
    {
        Timeout myTimeout = new Timeout( timeoutLock, table, time );
        return myTimeout.createException();
    }

    /*
     * A static entry way to get the LockTable in the system.
     * For track 3311
     */
    public static String buildString( Enumeration table, long time ) throws StandardException
    {
        Timeout myTimeout = new Timeout( null, table, time);
        return myTimeout.buildLockTableString();
    }

    /**
     * dumpLock puts information about currentLock into currentRow for output later.
     * @throws StandardException
     */
    private void dumpLock() throws StandardException
    {
        Hashtable attributes = new Hashtable(17);
        Object lock_type = currentLock.getQualifier();

        // want containerId, segmentId, pageNum, recId from locktable
        Lockable lockable = currentLock.getLockable();

        // See if the lockable object wants to participate
        if( !lockable.lockAttributes(ALL, attributes) )
        {
            currentRow = null;
            return;
        }

        // if it does, the lockable object must have filled in the following 
        // fields
        if( SanityManager.DEBUG )
        {
            SanityManager.ASSERT(attributes.get(VirtualLockTable.LOCKNAME) != null, "lock table can only represent locks that have a LOCKNAME" );
            SanityManager.ASSERT(attributes.get(VirtualLockTable.LOCKTYPE) != null, "lock table can only represent locks that have a LOCKTYPE" );
            if( attributes.get(VirtualLockTable.CONTAINERID ) == null &&
                attributes.get(VirtualLockTable.CONGLOMID ) == null )
                SanityManager.THROWASSERT("lock table can only represent locks that are associated with a container or conglomerate");
        }

        Long conglomId = (Long) attributes.get(VirtualLockTable.CONGLOMID);
        
        if( conglomId == null )
        {
            if( attributes.get(VirtualLockTable.CONTAINERID) != null && tc != null )
            {   
                Long value = (Long)attributes.get(VirtualLockTable.CONTAINERID);
                conglomId = new Long( tc.findConglomid( value.longValue() ) );
                attributes.put( VirtualLockTable.CONGLOMID, conglomId );
            }
        }

        Long containerId = (Long) attributes.get(VirtualLockTable.CONTAINERID);

        if( containerId == null )
        {
            if( conglomId != null && tc != null )
            {
                try
                {
                    containerId = new Long( tc.findContainerid( conglomId.longValue() ) );
                    attributes.put( VirtualLockTable.CONTAINERID, containerId );
                }
                catch( Exception e )
                {
                    // just don't do anything
                }
            }
        }
        
        attributes.put( VirtualLockTable.LOCKOBJ, currentLock );
        attributes.put( VirtualLockTable.XACTID, currentLock.getCompatabilitySpace().toString() );
        attributes.put( VirtualLockTable.LOCKMODE, lock_type.toString() );
        attributes.put( VirtualLockTable.LOCKCOUNT, Integer.toString( currentLock.getCount() ) );
        attributes.put( VirtualLockTable.STATE, (currentLock.getCount() != 0 ) ? "GRANT" : "WAIT" );
        
        if( tabInfo != null && conglomId != null )
        {
            try{
                String tableName = tabInfo.getTableName( conglomId );
                attributes.put( VirtualLockTable.TABLENAME, tableName );
            }
            catch( NullPointerException e )
            {
                attributes.put( VirtualLockTable.TABLENAME, conglomId );
            }
        
            try
            {
                String indexName = tabInfo.getIndexName( conglomId );
                if( indexName != null )
                    attributes.put( VirtualLockTable.INDEXNAME, indexName );
                else
                {
                    if( attributes.get(VirtualLockTable.LOCKTYPE).equals("LATCH") )
                    {   // because MODE field is way to short to display this,
                        // just put it in the indexname field for LATCH only.
                        attributes.put( VirtualLockTable.INDEXNAME, attributes.get(VirtualLockTable.LOCKMODE) );
                    }
                    else
                        attributes.put( VirtualLockTable.INDEXNAME, "NULL" );
                }
            }catch( Exception e )
            {   // we are here because tabInfo.indexCache is null
                if( VirtualLockTable.CONTAINERID != null )
                    attributes.put( VirtualLockTable.INDEXNAME, VirtualLockTable.CONTAINERID );
                else
                    attributes.put( VirtualLockTable.INDEXNAME, "NULL" );
            }

            String tableType = tabInfo.getTableType( conglomId );
            attributes.put( VirtualLockTable.TABLETYPE, tableType );
        }
        else
        { 
            if( conglomId != null )
                attributes.put( VirtualLockTable.TABLENAME, VirtualLockTable.CONGLOMID );
            else
                attributes.put( VirtualLockTable.TABLENAME, "NULL" );

            if( VirtualLockTable.CONTAINERID != null )
                attributes.put( VirtualLockTable.INDEXNAME, VirtualLockTable.CONTAINERID );
            else
                attributes.put( VirtualLockTable.INDEXNAME, "NULL" );
                
            attributes.put( VirtualLockTable.TABLETYPE, currentLock.toString() );
        }
        currentRow = attributes;
    }

    /**
     * cpArray helps built the output string (outputRow).
     * @param toCp the String to be copied into outputRow
     * @param start the start place
     * @param end the end place
     */
    private void cpArray( String toCp, int start, int end )
    {   // build a field in the output string
        int i = 0;
        int totalAllowWrite = end - start;

        if( toCp != null )
        {
            for( ; i < toCp.length() ; i++ )
            {
                if( (totalAllowWrite-i) == 0 )
                    break;

                outputRow[ i + start ] = toCp.charAt(i);
            }
        }
        for( ; i + start != end; i++ )
            outputRow[ i + start ] = ' ';

        outputRow[ end ] = SEPARATOR;
    }
    
    /**
     * Copies the needed information from currentRow into the StringBuffer for output
     * @return true if successful
     */
    private boolean timeoutInfoHash( )
    {
        if( currentRow == null )
            return false;

        String[] myData = new String[9];
        myData[0] = VirtualLockTable.XACTID;
        myData[1] = VirtualLockTable.LOCKTYPE;
        myData[2] = VirtualLockTable.LOCKMODE;
        myData[3] = VirtualLockTable.LOCKCOUNT;
        myData[4] = VirtualLockTable.LOCKNAME;
        myData[5] = VirtualLockTable.STATE;
        myData[6] = VirtualLockTable.TABLETYPE;
        myData[7] = VirtualLockTable.INDEXNAME;
        myData[8] = VirtualLockTable.TABLENAME;


        int currentLength = 0;
        for( int i = 0; i < myData.length; i++ )
        {
            cpArray( currentRow.get( myData[i] ).toString(), currentLength , currentLength + column[i].length() );
            // the next beginning position
            currentLength = currentLength + column[i].length() + 1;
        }
        return true;
    }
}

