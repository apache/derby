/*

   Derby - Class org.apache.derby.impl.sql.execute.ValueRow

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
	Basic implementation of ExecRow.

 */
class ValueRow implements ExecRow
{
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
			return column[position-1];
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
                rowClone.setColumn(colCtr + 1, column[colCtr]);
				continue;
			}

			if (column[colCtr] != null)
			{
				/* Rows are 1-based, column[] is 0-based */
                rowClone.setColumn(colCtr +1, column[colCtr].cloneValue(false));
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
                rowClone.setColumn(colCtr + 1, column[colCtr].getNewNull());
			}
		}
		return rowClone;
	}

	ExecRow cloneMe() {
		return new ValueRow(ncols);
	}

    /**
     * Reset all columns in the row array to null values.
     */
    public void resetRowArray() {
        for (int i = 0; i < column.length; i++) {
            if (column[i] != null) {
                column[i] = column[i].recycle();
            }
        }
    }

	// position is 1-based
	public final DataValueDescriptor cloneColumn(int columnPosition)
	{
        return column[columnPosition -1].cloneValue(false);
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
                columnClones[colCtr] = column[colCtr].cloneValue(false);
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
		
	// Set the number of columns in the row to ncols, preserving
	// the existing contents.
	protected void realloc(int ncols) {
		DataValueDescriptor[] newcol = new DataValueDescriptor[ncols];

		System.arraycopy(column, 0, newcol, 0, column.length);
		column = newcol;
	}
}
