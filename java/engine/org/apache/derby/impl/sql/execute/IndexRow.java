/*

   Derby - Class org.apache.derby.impl.sql.execute.IndexRow

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;


/**
	Basic implementation of ExecIndexRow.

	@author jeff
 */
public class IndexRow extends ValueRow implements ExecIndexRow
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
	//	STATE
	//
	///////////////////////////////////////////////////////////////////////


	private boolean[]	orderedNulls;

	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	IndexRow() {}

	public IndexRow(int ncols) {
		 super(ncols);
		 orderedNulls = new boolean[ncols];	/* Initializes elements to false */
	}

	///////////////////////////////////////////////////////////////////////
	//
	//	EXECINDEXROW INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	/* Column positions are one-based, arrays are zero-based */
	public void orderedNulls(int columnPosition) {
		orderedNulls[columnPosition] = true;
	}

	public boolean areNullsOrdered(int columnPosition) {
		return orderedNulls[columnPosition];
	}

	/**
	 * Turn the ExecRow into an ExecIndexRow.
	 *
	 * @return Nothing.
	 */
	public void execRowToExecIndexRow(ExecRow valueRow)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"execRowToExecIndexRow() not expected to be called for IndexRow");
		}
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
		super.readExternal( in );

		int colCount = nColumns();

		orderedNulls = new boolean[ colCount ];
		for ( int ictr = 0; ictr < colCount; ictr++ ) { orderedNulls[ ictr ] = in.readBoolean(); }
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
		super.writeExternal( out );
		int colCount = nColumns();

		for ( int ictr = 0; ictr < colCount; ictr++ ) { out.writeBoolean( orderedNulls[ ictr ] ); }
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.INDEX_ROW_V01_ID; }

	ExecRow cloneMe() {
		return new IndexRow(nColumns());
	}
}
