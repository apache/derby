/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.RowList

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.ConglomerateController;

import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.sql.dictionary.TabInfo;

import org.apache.derby.iapi.sql.Row;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import java.util.Vector;
import java.util.Enumeration;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * This interface wraps a list of Rows.
 *
 * @version 0.1
 * @author Rick Hillegas
 */

public class RowList extends Vector implements Formatable
{
	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	///////////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	///////////////////////////////////////////////////////////////////////

	private		String		tableName;

	protected	transient	TabInfo		tableInfo;


	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	RowList() {}

	/**
	 * Constructor. Creates a list of rows for a table.
	 *
	 * @param tableInfo	Table information
	 *
	 */
    public RowList( TabInfo tableInfo )
	{
		this.tableInfo = tableInfo;

		tableName = tableInfo.getTableName();
	}


	/**
	 * Constructor used for testing.
	 *
	 * @param tableName	name of table that this RowList buffers tuples for.
	 *
	 */
    public RowList( String tableName )
	{
		this.tableName = tableName;
	}

	///////////////////////////////////////////////////////////////////////
	//
	//	ROW LIST INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Add a row to the list for this table.
	 *
	 * @param row   Row to chain onto list.
	 *
	 * @return	Nothing
	 *
	 */

	public void add(ExecRow row)
	{
		super.addElement(row);
	}

	/**
	 * Get the name of the table that this list is for.
	 *
	 *
	 * @return  name of table that this Rowlist holds tuples for.
	 *
	 * @exception StandardException		Thrown on error
	 */

    public String getTableName() throws StandardException
	{
	    return getTableInfo().getTableName();
	}

	/**
	 * Get the Conglomerate ID of the table that this list is for.
	 *
	 *
	 * @return	conglomerate id of table that this Rowlist holds tuples for.
	 *
	 * @exception StandardException		Thrown on error
	 */

    public long getTableID() throws StandardException
	{
	    return getTableInfo().getHeapConglomerate();
	}

	/**
	 * Execution-time routine to delete all the keys on the list from the
	 * corresponding system table.
	 *
	 *	@param	lcc			language state variable
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void	deleteFromCatalog(LanguageConnectionContext lcc)
					throws StandardException
	{
		getTableInfo().deleteRowList( this, lcc );
	}


	/**
	 * Execution-time routine to stuff all the rows on the list into the
	 * corresponding system table.
	 *
	 *	@param	lcc			language state variable
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void	stuffCatalog(LanguageConnectionContext lcc)
					throws StandardException
	{
		getTableInfo().insertRowList( this, lcc );
	}


	private TabInfo getTableInfo() throws StandardException
	{
 		if ( tableInfo == null )
		{
 			DataDictionaryContext		ddc = (DataDictionaryContext)
 			                            ContextService.getContext(DataDictionaryContext.CONTEXT_ID);
 			DataDictionary				dd = ddc.getDataDictionary();
 
 			tableInfo = dd.getTabInfo( tableName );
 		}
  		return	tableInfo;
 	}

	///////////////////////////////////////////////////////////////////////
	//
	//	FORMATABLE INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		tableName = (String) in.readObject();

		int			rowCount = in.readInt();
		ExecRow		row;
		for ( int ictr = 0; ictr < rowCount; ictr++ )
		{
			row = (ExecRow) in.readObject();
			add( row );
		}
	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		out.writeObject( tableName );

		int			rowCount = size();
		out.writeInt( rowCount );
		for ( int ictr = 0; ictr < rowCount; ictr++ )
		{
			out.writeObject( elementAt( ictr ) );
		}
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.ROW_LIST_V01_ID; }


	///////////////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	///////////////////////////////////////////////////////////////////////


}








