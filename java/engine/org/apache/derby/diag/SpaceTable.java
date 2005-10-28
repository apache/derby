/*

   Derby - Class org.apache.derby.diag.SpaceTable

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

package org.apache.derby.diag;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.SpaceInfo;
import org.apache.derby.iapi.error.PublicAPI;

import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.derby.vti.VTITemplate;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

/**
	SpaceTable is a virtual table that shows the space usage of a particular
    table and its indexes.
	
	This virtual table can be invoked by calling it
	directly, and supplying the schema name and table name as arguments.
	<PRE> select * from new org.apache.derby.diag.SpaceTable('MYSCHEMA','MYTABLE') t; </PRE>
    If the schema name is not supplied, the default schema is used.
    <PRE> select * from new org.apache.derby.diag.SpaceTable('MYTABLE') t; </PRE>
    Alternatively, the table can be invoked through the system alias SpaceTable
    <PRE> select * from new SPACETABLE('MYTABLE') t; </PRE>
    <P>
    NOTE: Both the schema name and the table name must be any expression that evaluates to a
    string data type. If you created a schema or table name as a non-delimited identifier,
    you must present their names in all upper case.


	<P>The SpaceTable virtual table can be used to estimate whether space
    might be saved by compressing a table and its indexes.

	<P>The SpaceTable virtual table has the following columns:
	<UL>
	<LI>CONGLOMERATENAME varchar(128) - nullable.  The name of the conglomerate,
    which is either the table name or the index name. (Unlike the 
    SYSCONGLOMERATES column of the same name, table ID's do not appear 
    here).</LI>
	<LI>ISINDEX SMALLINT - not nullable.  Is not zero if the conglomerate is an 
    index, 0 otherwise.</LI>
	<LI>NUMALLOCATEDPAGES bigint - not nullable.  The number of pages actively
    linked into the table.  The total number of pages in the file is the
    sum of NUMALLOCATEDPAGES + NUMFREEPAGES.</LI>
	<LI>NUMFREEPAGES bigint - not nullable. The number of free pages that 
    belong to the table.  When a new page is to be linked into the table the
    system will move a page from the NUMFREEPAGES list to the NUMALLOCATEDPAGES
    list.  The total number of pages in the file is the sum of 
    NUMALLOCATEDPAGES + NUMFREEPAGES.</LI>
	<LI>NUMUNFILLEDPAGES bigint - not nullable.  The number of unfilled pages 
    that belong to the table. Unfilled pages are allocated pages that are not 
    completely full. Note that the number of unfilled pages is an estimate and 
    is not exact. Running the same query twice can give different results on 
    this column. </LI>
	<LI>PAGESIZE integer - not nullable.  The size of the page in bytes for 
    that conglomerate.
	</LI>
	<LI>ESTIMSPACESAVING bigint - not nullable.  The estimated space which 
    could possibly be saved by compressing the conglomerate, in bytes.</LI>
	</UL>


    <P>
    To get space information on all schemas and tables, use a query such as
    <PRE>
    select v.*
    from SYS.SYSSCHEMAS s,
         SYS.SYSTABLES t,
         new org.apache.derby.diag.SpaceTable(SCHEMANAME,TABLENAME) v
    where s.SCHEMAID = t.SCHEMAID;
    </PRE>
*/
public class SpaceTable extends VTITemplate implements VTICosting {

	private ConglomInfo[] conglomTable;
	boolean initialized;
	int currentRow;
	private boolean wasNull;
    private String schemaName;
    private String tableName;
    private SpaceInfo spaceInfo;
    private TransactionController tc;


    public SpaceTable(String schemaName, String tableName)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public SpaceTable(String tableName)
    {
        this.tableName = tableName;
    }

    private void getConglomInfo(LanguageConnectionContext lcc)
        throws StandardException
    {
        DataDictionary dd = lcc.getDataDictionary();
		
		if (schemaName == null)
			schemaName = lcc.getCurrentSchemaName();

        // if schemaName is null, it gets the default schema
        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, lcc.getTransactionExecute(), true);
        TableDescriptor td = dd.getTableDescriptor(tableName,sd);
        if (td == null)  // table does not exist
        {
            conglomTable = new ConglomInfo[0];   // make empty conglom table
            return;
        }
        ConglomerateDescriptor[] cds = td.getConglomerateDescriptors();
        // initialize spaceTable
        conglomTable = new ConglomInfo[cds.length];
        for (int i = 0; i < cds.length; i++)
            conglomTable[i] = new ConglomInfo(
                cds[i].getConglomerateNumber(),
                cds[i].isIndex() ? cds[i].getConglomerateName() : tableName,
                cds[i].isIndex());
    }


    private void getSpaceInfo(int index)
        throws StandardException
    {
            ConglomerateController cc = tc.openConglomerate(
                conglomTable[index].getConglomId(),
                false,
                0,            // not for update
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_READ_COMMITTED
                );
            spaceInfo = cc.getSpaceInfo();
            cc.close();
            cc = null;
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
		@exception SQLException if no transaction context can be found
	 */
	public boolean next() throws SQLException
	{
        try
        {
     		if (!initialized)
    		{
				LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
               getConglomInfo(lcc);
                tc = lcc.getTransactionExecute();
			    initialized = true;
			    currentRow = -1;
		    }
		    if (conglomTable == null)
		    	return false;
            currentRow++;
            if (currentRow >= conglomTable.length)
                return false;
            spaceInfo = null;
            getSpaceInfo(currentRow);
            return true;
        }
        catch (StandardException se)
        {
            throw PublicAPI.wrapStandardException(se);
        }
    }


	/**
		@see java.sql.ResultSet#close
	 */
	public void close()
	{
		conglomTable = null;
        spaceInfo = null;
        tc = null;
	}

	/**
		@see java.sql.ResultSet#getString
	 */
	public String getString(int columnNumber)
	{
		ConglomInfo conglomInfo = conglomTable[currentRow];
		String str = conglomInfo.getConglomName();
   		wasNull = (str == null);
		return str;
	}

    /**
    @see java.sql.ResultSet#getLong
    */
    public long getLong(int columnNumber)
	{
        long longval;
        ConglomInfo conglomInfo = conglomTable[currentRow];
		switch(columnNumber)
		{
		    case 3:
			    longval = spaceInfo.getNumAllocatedPages();
                break;
    		case 4:
			    longval = spaceInfo.getNumFreePages();
                break;
    		case 5:
			    longval = spaceInfo.getNumUnfilledPages();
                break;
    		case 7:
                int psize = spaceInfo.getPageSize();
			    longval = (spaceInfo.getNumFreePages() * psize);
                // unfilled page estimate is not reproducible/too unstable
                // + ((spaceInfo.getNumUnfilledPages() * psize) / 2);
                break;
		    default:
			    longval = -1;
		}
		wasNull = false;
        if (SanityManager.DEBUG)
            if (longval < 0)
                SanityManager.THROWASSERT("SpaceTable column number " + columnNumber +
                    " has a negative value at row " + currentRow);
		return longval;
    }

    /**
    @see java.sql.ResultSet#getShort
    */
    public short getShort(int columnNumber)
	{
        ConglomInfo conglomInfo = conglomTable[currentRow];
		wasNull = false;
		return (short) (conglomInfo.getIsIndex() ? 1 : 0);
    }


    /**
    @see java.sql.ResultSet#getInt
    */
    public int getInt(int columnNumber)
	{
		return spaceInfo.getPageSize();
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
		@return true
		@see VTICosting#supportsMultipleInstantiations
	 */
	public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment)
	{
		return true;
	}

	/*
	** Metadata
	*/
	private static final ResultColumnDescriptor[] columnInfo = {

		EmbedResultSetMetaData.getResultColumnDescriptor("CONGLOMERATENAME",  Types.VARCHAR, true, 128),
		EmbedResultSetMetaData.getResultColumnDescriptor("ISINDEX",           Types.SMALLINT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("NUMALLOCATEDPAGES", Types.BIGINT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("NUMFREEPAGES",      Types.BIGINT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("NUMUNFILLEDPAGES",  Types.BIGINT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("PAGESIZE",          Types.INTEGER, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("ESTIMSPACESAVING",  Types.BIGINT, false),
	};
	
	private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);

}

class ConglomInfo
{
    private long conglomId;
    private String conglomName;
    private boolean isIndex;

    public ConglomInfo(long conglomId, String conglomName, boolean isIndex)
    {
        this.conglomId = conglomId;
        this.conglomName = conglomName;
        this.isIndex = isIndex;
    }

    public long getConglomId()
    {
        return conglomId;
    }

    public String getConglomName()
    {
        return conglomName;
    }

    public boolean getIsIndex()
    {
        return isIndex;
    }
}




