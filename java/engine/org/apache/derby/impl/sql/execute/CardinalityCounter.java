/*

   Derby - Class org.apache.derby.impl.sql.execute.CardinalityCounter

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.RowLocationRetRowSource;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

/**
 * This is a decorator (in Design Patterns Terminology)
 * class to enhance the functionality
 * of a RowLocationRetRowSource. It assumes that the rows are coming
 * in sorted order from the row source and it simply keeps track of
 * the cardinality of all the leading columns.
 */

public class CardinalityCounter implements RowLocationRetRowSource
{
	private RowLocationRetRowSource rowSource;
	private DataValueDescriptor[] prevKey;
	private long[] cardinality;
	private long numRows;

	public CardinalityCounter(RowLocationRetRowSource rowSource)
	{
		this.rowSource = rowSource;
	}

	/** @see RowLocationRetRowSource#needsRowLocation */
	public boolean needsRowLocation() 
	{ 
		return rowSource.needsRowLocation();
	}

	/** @see RowLocationRetRowSource#rowLocation */
	public void rowLocation(RowLocation rl) throws StandardException
	{ 
		rowSource.rowLocation(rl);
	}

	/** 
	 * Gets next row from the row source and update the count of unique values
	 * that are returned.
	 * @see RowLocationRetRowSource#getNextRowFromRowSource 
	 */
	public DataValueDescriptor[] getNextRowFromRowSource() throws StandardException
	{
		DataValueDescriptor[] nextRow;
		nextRow = rowSource.getNextRowFromRowSource();
		if (nextRow != null)
			keepCount(nextRow);
		return nextRow;
	}


	/** @see RowLocationRetRowSource#needsToClone */
	public boolean needsToClone()
	{
		return rowSource.needsToClone();
	}

	/** @see RowLocationRetRowSource#getValidColumns */
	public FormatableBitSet getValidColumns()
	{
		return rowSource.getValidColumns();
	}

	/** @see RowLocationRetRowSource#closeRowSource */
	public void closeRowSource()
	{
		rowSource.closeRowSource();
	}
	
	private DataValueDescriptor[] clone(DataValueDescriptor[] clonee)
	{
		DataValueDescriptor[] cloned;

		cloned = new DataValueDescriptor[clonee.length];
		for (int i = 0; i < clonee.length - 1; i++)
		{
			cloned[i] = ((DataValueDescriptor)clonee[i]).getClone();
		}
		return cloned;
	}

	public void keepCount(DataValueDescriptor[] currentKey) throws StandardException
	{
		int numKeys = currentKey.length - 1; // always row location.
		numRows++;
		if (prevKey == null)
		{
			prevKey = clone(currentKey);
			cardinality = new long[currentKey.length - 1];
			for (int i = 0; i < numKeys; i++)
				cardinality[i] = 1;
			return;
		}
		
		int i;
		for (i = 0; i < numKeys; i++)
		{
			if (((DataValueDescriptor)prevKey[i]).isNull())
				break;

			if ((prevKey[i]).compare(currentKey[i]) != 0)
			{
				// null out prevKey, so that the object gets 
				// garbage collected. is this too much object
				// creation? can we do setColumn or some such
				// in the object that already exists in prevKey?
				// xxxstatRESOLVE--
				prevKey = null; 
				prevKey = clone(currentKey);
				break;
			}
		} // for
		
		for (int j = i; j < numKeys; j++)
			cardinality[j]++;
	}
	
	/** return the array of cardinalities that are kept internally. One value
	 * for each leading key; i.e c1, (c1,c2), (c1,c2,c3) etc.
	 * @return 	an array of unique values.
	 */
	public long[] getCardinality() { return cardinality; }

	/**
	 * get the number of rows seen in the row source thus far.
	 * @return total rows seen from the row source.
	 */
	public long getRowCount() { return numRows; }
}
