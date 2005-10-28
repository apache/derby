/*

   Derby - Class org.apache.derby.diag.LockTable

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

package org.apache.derby.diag;

// temp
import org.apache.derby.impl.services.locks.TableNameInfo;

import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.VirtualLockTable;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.error.PublicAPI;

import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;

import java.util.Hashtable;
import java.util.Enumeration;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.derby.vti.VTITemplate;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

/**
	LockTable is a virtual table that shows all locks currently held in
	the database.
	
	This virtual table can be invoked by calling it directly
	<PRE> select * from new org.apache.derby.diag.LockTable() t; </PRE>
	or through the system alias LOCKTABLE
	<PRE> select * from new LOCKTABLE() t; </PRE> 
	
	<P>The LockTable virtual table takes a snap shot of the lock table while
	the system is in flux, so it is possible that some locks may be in
	transition state while the snap shot is taken. We choose to do this rather
	then impose extranous timing restrictions so that the use of this tool will
	not alter the normal timing and flow of execution in the application.

	<P>The LockTable virtual table has the following columns:
	<UL><LI>XID varchar(15) - not nullable.  The transaction id, this can be joined with the
	TransactionTable virtual table's XID.</LI>
	<LI>TYPE varchar(5) - nullable.  The type of lock, ROW, TABLE, or LATCH</LI>
	<LI>MODE varchar(4) - not nullable.  The mode of the lock, "S", "U", "X", "IS", "IX".</LI>
		<UL><LI>S is shared lock (N/A to Latch) </LI>
			<LI>U is update lock (N/A to Latch) </LI>
			<LI>X is exclusive lock </LI>
			<LI>IS is intent shared lock (N/A to Latch or Row lock) </LI>
			<LI>IX is intent exclusive lock (N/A to Latch or Row lock) </LI>
		</UL>
	<LI>TABLENAME varchar(128) - not nullable. The name of the base table the lock is for </LI>
	<LI>LOCKNAME varchar(20) - not nullable.  The name of the lock </LI>
	<LI>STATE varchar(5) - nullable.  GRANT or WAIT </LI>
	<LI>TABLETYPE varchar(9) - not nullable.  'T' for user table, 'S' for system table </LI>
	<LI>LOCKCOUNT varchar(5) - not nullable.  Internal lock count.</LI>
	<LI>INDEXNAME varchar(128) - normally null.  If non-null, a lock is held on 
	the index, this can only happen if this is not a user transaction.</LI>
	</UL>

 */
public class LockTable extends VTITemplate implements VTICosting  {

	/** return only latches */
	public static final int LATCH = VirtualLockTable.LATCH;

	/** return only table and row locks */
	public static final int TABLE_AND_ROWLOCK = VirtualLockTable.TABLE_AND_ROWLOCK;

	/** return all locks and latches */
	public static final int ALL = VirtualLockTable.ALL;

	/*
	** private 
	*/
	private TransactionController tc;
	private LanguageConnectionFactory lcf;
	private Hashtable currentRow;		// an entry in the lock table
	private Enumeration lockTable;	
	private boolean wasNull;
	private boolean initialized;
	private final int flag;
	private TableNameInfo tabInfo;

	/**
		The normal way of instantiating a LockTable, equivalent to
		LockTable(org.apache.derby.diag.LockTable->TABLE_AND_ROWLOCK).
		Only shows row and table lock and not latches.  Latches are generally
		held for very short duration and are not of interest to Cloudscape
		users.  Only under abnormal circumstances will one be interested in
		looking at latches.
	 */
	public LockTable()
	{
		flag = TABLE_AND_ROWLOCK;
	}

	/**
		This call is intrusive and should only be used under the supervision of
		technical support.  Create an instance of the lock table which
		has transient latches as well as locks.
	 */
	public LockTable(int flag)
	{
		this.flag = flag;
	}

	/**
		@see java.sql.ResultSet#getMetaData
	 */
	public ResultSetMetaData getMetaData()
	{
		return metadata;
	}

	/**
		@see java.sql.ResultSet#next
		@exception SQLException if no transaction context can be found, or other
		Cloudscape internal errors are encountered.
	 */
	public boolean next() throws SQLException
	{
		try
		{
			if (!initialized)
			{
				LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

				tc = lcc.getTransactionExecute();
				LanguageConnectionFactory lcf = lcc.getLanguageConnectionFactory();
				LockFactory lf = lcf.getAccessFactory().getLockFactory();
				lockTable = lf.makeVirtualLockTable();
				initialized = true;
				tabInfo = new TableNameInfo(lcc, true);
			}

			currentRow = null;
			if (lockTable != null) {
				while (lockTable.hasMoreElements() && (currentRow == null)) {
					currentRow = dumpLock((Latch) lockTable.nextElement());
				}
			}
		}
		catch (StandardException se)
		{
			throw PublicAPI.wrapStandardException(se);
		}

		return (currentRow != null);
	}

	/**
		@see java.sql.ResultSet#close
	 */
	public void close()
	{
		lockTable = null;
	}

	/**
		All columns in TransactionTable VTI are of String type.
		@see java.sql.ResultSet#getString
	 */
	public String getString(int columnNumber)
	{		
		String val = (String)currentRow.get(columnInfo[columnNumber-1].getName());
		wasNull = (val == null);

		return  val;
	}


	/**
		@see java.sql.ResultSet#wasNull
	 */
	public boolean wasNull()
	{
		return wasNull;
	}

	/**  VTI costing interface */
	
	/**
		@see VTICosting#getEstimatedRowCount
	 */
	public double getEstimatedRowCount(VTIEnvironment vtiEnvironment)
	{
		return VTICosting.defaultEstimatedRowCount;
	}
	
	/**
		@see VTICosting#getEstimatedCostPerInstantiation
	 */
	public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment)
	{
		return VTICosting.defaultEstimatedCost;
	}
	/**
		@return false
		@see VTICosting#supportsMultipleInstantiations
	 */
	public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment)
	{
		return false;
	}

	/*
	** Private methods
	*/

	/**
		Convert the lock information into a hashtable.
	*/
    private Hashtable dumpLock(
    Latch                   lock)
        throws StandardException
    {
		Hashtable	attributes = new Hashtable(17);
        Object      lock_type =  lock.getQualifier();


		// 4 things we are interested in from the lockable:
		// containerId, segmentId, pageNum, recId

		Lockable lockable = lock.getLockable();

		// see if this lockable object wants to participate
		if (!lockable.lockAttributes(flag, attributes))
			return null;				

		// if it does, the lockable object must have filled in the following
		// fields
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(attributes.get(VirtualLockTable.LOCKNAME) != null,
			 "lock table can only represent locks that have a LOCKNAME");

			SanityManager.ASSERT(attributes.get(VirtualLockTable.LOCKTYPE) != null,
			 "lock table can only represent locks that have a LOCKTYPE");

			if (attributes.get(VirtualLockTable.CONTAINERID) == null &&
				attributes.get(VirtualLockTable.CONGLOMID) == null)
				SanityManager.THROWASSERT(
			 "lock table can only represent locks that are associated with a container or conglomerate");
		}

		if (attributes.get(VirtualLockTable.LOCKNAME) == null ||
			attributes.get(VirtualLockTable.LOCKTYPE) == null)
			return null;				// can't deal with this for now

		// if the lock has zero count and is an instance of Lock then it
		// is a lock that has just been released. Therefore do put it into
		// the lock table. This occurs because the Lock object is the real
		// live object in the LockTable. Thus when we copied the lock table
		// it had a non-zero count, but since then it has been released
		// (after we dropped the sync). Note if it is of type ActiveLock
		// with zero count there is stil the chance it has been released.
		// Less likely, but we still need to fix that at some time.
		int lockCount = lock.getCount();
		String state;
		if (lockCount != 0)
			state = "GRANT";
		else if (!(lock instanceof org.apache.derby.impl.services.locks.ActiveLock))
			return null;
		else
			state = "WAIT";

		Long conglomId = (Long) attributes.get(VirtualLockTable.CONGLOMID);

		if (conglomId == null)
		{
			// we need to figure this out
			if (attributes.get(VirtualLockTable.CONTAINERID) == null)
				return null; // can't deal with this for now

			Long value = (Long)attributes.get(VirtualLockTable.CONTAINERID);
			conglomId = new Long(tc.findConglomid(value.longValue()));
			attributes.put(VirtualLockTable.CONGLOMID, conglomId);
		}

		attributes.put(VirtualLockTable.LOCKOBJ, lock);
		attributes.put(VirtualLockTable.XACTID, lock.getCompatabilitySpace().toString());
		attributes.put(VirtualLockTable.LOCKMODE, lock_type.toString());

		attributes.put(VirtualLockTable.LOCKCOUNT, Integer.toString(lockCount));

		attributes.put(VirtualLockTable.STATE, state);

		String tableName = tabInfo.getTableName(conglomId);

		attributes.put(VirtualLockTable.TABLENAME, tableName);

		String indexName = tabInfo.getIndexName(conglomId);

		if (indexName != null)
			attributes.put(VirtualLockTable.INDEXNAME, indexName);

		String tableType = tabInfo.getTableType(conglomId);
		attributes.put(VirtualLockTable.TABLETYPE, tableType);
		return attributes;

    }

	/*
	** Metadata
	*/
	private static final ResultColumnDescriptor[] columnInfo = {

		EmbedResultSetMetaData.getResultColumnDescriptor(VirtualLockTable.XACTID,    Types.VARCHAR, false, 15),
		EmbedResultSetMetaData.getResultColumnDescriptor(VirtualLockTable.LOCKTYPE,  Types.VARCHAR, true, 5),
		EmbedResultSetMetaData.getResultColumnDescriptor(VirtualLockTable.LOCKMODE,  Types.VARCHAR, false, 4),
		EmbedResultSetMetaData.getResultColumnDescriptor(VirtualLockTable.TABLENAME, Types.VARCHAR, false, 128),
		EmbedResultSetMetaData.getResultColumnDescriptor(VirtualLockTable.LOCKNAME,  Types.VARCHAR, false, 20),
		EmbedResultSetMetaData.getResultColumnDescriptor(VirtualLockTable.STATE,     Types.VARCHAR, true, 5),
		EmbedResultSetMetaData.getResultColumnDescriptor(VirtualLockTable.TABLETYPE, Types.VARCHAR, false, 9),
		EmbedResultSetMetaData.getResultColumnDescriptor(VirtualLockTable.LOCKCOUNT, Types.VARCHAR, false, 5),
		EmbedResultSetMetaData.getResultColumnDescriptor(VirtualLockTable.INDEXNAME, Types.VARCHAR, true,  128)
	};
	
	private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);
}

