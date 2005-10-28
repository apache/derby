/*

   Derby - Class org.apache.derby.impl.sql.execute.ValueRow

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

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
	Basic implementation of ExecRow.

	@author ames
 */
public class ValueRow implements ExecRow, Formatable
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

	private DataValueDescriptor[] column;
	private int ncols;

	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	ValueRow() {}

	/**
	  *	Make a value row with a designated number of column slots.
	  *
	  *	@param	ncols	number of columns to allocate
	  */
	public ValueRow(int ncols)
	{
		 column = new DataValueDescriptor[ncols];
		 this.ncols = ncols;
	}


	///////////////////////////////////////////////////////////////////////
	//
	//	EXECROW INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	// this is the actual current # of columns
	public int nColumns() {
		return ncols;
	}

	// get a new Object[] for the row
	public void getNewObjectArray()
	{
		column = new DataValueDescriptor[ncols];
	}

	/*
	 * Row interface
	 */
	// position is 1-based
	public DataValueDescriptor	getColumn (int position) {
		if (position <= column.length)
			return (DataValueDescriptor) (column[position-1]);
		else
			return (DataValueDescriptor)null;
	}

	// position is 1-based.
	public void setColumn(int position, DataValueDescriptor col) {
 
		if (position > column.length)
			realloc(position); // enough for this column
		column[position-1] = col;
	}


	/*
	** ExecRow interface
	*/

	// position is 1-based
	public ExecRow getClone() 
	{
		return getClone((FormatableBitSet) null);
	}

	public ExecRow getClone(FormatableBitSet clonedCols)
	{
		int numColumns = column.length;

		/* Get the right type of row */
		ExecRow rowClone = cloneMe();

		for (int colCtr = 0; colCtr < numColumns; colCtr++) 
		{
			// Copy those columns whose bit isn't set (and there is a FormatableBitSet)
			if (clonedCols != null && !(clonedCols.get(colCtr + 1)))
			{
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr + 1, (DataValueDescriptor) column[colCtr]);
				continue;
			}

			if (column[colCtr] != null)
			{
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr + 1, column[colCtr].getClone());
			}
		}
		return rowClone;
	}

	// position is 1-based
	public ExecRow getNewNullRow()
	{
		int numColumns = column.length;
		ExecRow rowClone = cloneMe();


		for (int colCtr = 0; colCtr < numColumns; colCtr++) 
		{
			if (column[colCtr] != null)
			{
				/* Rows are 1-based, column[] is 0-based */
				if (column[colCtr] instanceof RowLocation)
				{
					/*
					** The getClone() method for a RowLocation has the same
					** name as for DataValueDescriptor, but it's on a different
					** interface, so the cast must be different.
					**
					*/
					rowClone.setColumn(colCtr + 1, column[colCtr].getClone());
				}
				else
				{
					// otherwise, get a new null
					rowClone.setColumn(colCtr + 1,
						((DataValueDescriptor) (column[colCtr])).getNewNull());
				}
			}
		}
		return rowClone;
	}

	ExecRow cloneMe() {
		return new ValueRow(ncols);
	}

	// position is 1-based
	public final DataValueDescriptor cloneColumn(int columnPosition)
	{
		return column[columnPosition -1].getClone();
	}

	/*
	 * class interface
	 */
	public String toString() {
		// NOTE: This method is required for external functionality (the
		// consistency checker), so do not put it under SanityManager.DEBUG.
		String s = "{ ";
		for (int i = 0; i < column.length; i++)
		{
			if (column[i] == null)
				s += "null";
			else
				s += column[i].toString();
			if (i < (column.length - 1))
				s += ", ";
		}
		s += " }";
		return s;
	}


	/**
		Get the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArray() {
		return column;
	}

	/**
		Get a clone of the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArrayClone() 
	{
		int numColumns = column.length;
		DataValueDescriptor[] columnClones = new DataValueDescriptor[numColumns];

		for (int colCtr = 0; colCtr < numColumns; colCtr++) 
		{
			if (column[colCtr] != null)
			{
				columnClones[colCtr] = column[colCtr].getClone();
			}
		}

		return columnClones;
	}

	/**
	 * Set the row array
	 *
	 * @see ExecRow#setRowArray
	 */
	public void setRowArray(DataValueDescriptor[] value)
	{
		column = value;
	}

	public void setRowArray(Storable[] value) {
		if (value instanceof DataValueDescriptor[]) {
			column = (DataValueDescriptor[]) value;
			return;
		}

		if ((column == null) || (column.length != value.length))
			column = new DataValueDescriptor[value.length];


		System.arraycopy(value, 0, column, 0, column.length);
	}
		
	// Set the number of columns in the row to ncols, preserving
	// the existing contents.
	protected void realloc(int ncols) {
		DataValueDescriptor[] newcol = new DataValueDescriptor[ncols];

		System.arraycopy(column, 0, newcol, 0, column.length);
		column = newcol;
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
		column = new DataValueDescriptor[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, column);
		ncols = column.length;
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
		ArrayUtil.writeArrayLength(out, column);
		ArrayUtil.writeArrayItems(out, column);
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int getTypeFormatId()	{ return StoredFormatIds.VALUE_ROW_V01_ID; }

}
