/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_RowSource

   Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.unitTests.store;

import org.apache.derby.iapi.store.access.*;

import org.apache.derby.iapi.types.SQLInteger;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;


/**
  A RowSource is the mechanism for iterating over a set of rows.  The RowSource
  is the interface through which access recieved a set of rows from the client
  for the purpose of inserting into a single container.

  <p>
  A RowSource can come from many sources - from rows that are from fast path
  import, to rows coming out of a sort for index creation.

  @see org.apache.derby.iapi.store.access.RowSource
*/ 
public class T_RowSource implements RowSource {

	static public final int INTEGER_ROW_TYPE = 1;
	static public final int STRING_ROW_TYPE = 2;

	static protected final String REC_001 = "McLaren";
	static protected final String REC_002 = "Ferrari";
	static protected final String REC_003 = "Benetton";
	static protected final String REC_004 = "Prost";
	static protected final String REC_005 = "Tyrell";
	static protected final String REC_006 = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
	static protected final String REC_007 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
	static protected final String REC_008 = "z";

	static protected final int DEFAULT_ROW_COUNT = 500000;
	static protected final int DEFAULT_COLUMN_COUNT = 13;
	static protected final int DEFAULT_SEED = 53;	// some random number

	private int rowCount;
	private int columnCount;
	private DataValueDescriptor row[];
	private FormatableBitSet validColumns;
	private boolean forceAbort;
	private Transaction t;

	/*
	 *	constructor
	 */
	public T_RowSource() {

		// default will create DEFAULT_ROW_COUNT rows,
		// of DEFAULT_COLUMN_COUNT columns string type rows
		// validColumns will be set to null.
		this.rowCount = DEFAULT_ROW_COUNT;
		this.columnCount = DEFAULT_COLUMN_COUNT;
		this.row = new DataValueDescriptor[DEFAULT_COLUMN_COUNT];
		row = setStringRow();
	}

	// if the caller does not pass in a validColumn, we will set it here
	public T_RowSource(int count, int columnCount, int rowType, boolean forceAbort, Transaction t) {

		this.rowCount = count;
		this.columnCount = columnCount;
		validColumns = new FormatableBitSet(columnCount);
		for (int i = 0; i < columnCount; i++)
			validColumns.set(i);

		this.row = new DataValueDescriptor[columnCount];
		if (rowType == INTEGER_ROW_TYPE)
			setIntegerRow();
		else
			row = setStringRow();

		this.forceAbort = forceAbort;
		this.t = t;
	}

	// the caller has a chance to set the valisColumns to anything they want.
	public T_RowSource(int count, int columnCount, int rowType, FormatableBitSet validColumns) {

		this.rowCount = count;
		this.columnCount = columnCount;
		this.validColumns = validColumns;

		this.row = new DataValueDescriptor[columnCount];
		if (rowType == INTEGER_ROW_TYPE)
			setIntegerRow();
		else
			row = setStringRow();
	}

	/*
	 *	methods for RowSource
	 */

	/**
	    @return true if more rows are coming, false if there is no more rows
		in the RowSource
	 * @exception StandardException		Thrown on error
	 */
	public boolean hasMoreRows() throws StandardException {
		if (rowCount > 0)
			return true;
		else
			return false;
	}

	/**
		Get the next row as an array of column objects. The column objects can
		be a JBMS Storable or any
		Serializable/Externalizable/Formattable/Streaming type.  

		@exception StandardException Derby Standard Error Policy
	 */
	public DataValueDescriptor[] getNextRowFromRowSource() 
        throws StandardException {

		if (this.rowCount <= 0)
			return null;

		// if we are testing error condition, force an abort now
		if (forceAbort && (this.rowCount < 3))
			t.abort();

		this.rowCount--;
		return row;
	}

	/**
	  getValidColumns describes the DataValueDescriptor[] returned by all calls
      to the getNextRowFromRowSource() call. 
	*/
	public FormatableBitSet getValidColumns() {
		return validColumns;
	} 

	/**
		closeRowSource tells the RowSource that it will no longer need to
		return any rows and it can release any resource it may have.
		Subsequent call to any method on the RowSource will result in undefined
		behavior.  A closed rowSource can be closed again.
	*/
	public void closeRowSource() {

		this.rowCount = 0;
	}


	/**
		needsRowLocation returns true iff this the row source expects the
		drainer of the row source to call rowLocation after getting a row from
		getNextRowFromRowSource.

		@return true iff this row source expects some row location to be
		returned 
		@see #rowLocation
	 */
	public boolean needsRowLocation() {
		return false;
	}

	/**
	 * @see RowSource#needsToClone
	 */
	public boolean needsToClone()
	{
		return true;
	}

	/**
		rowLocation  is not implemented here
	 */
	public void rowLocation(RowLocation rl) {

		rl = null;
	}

	/**
		Get a copy of the template row.  Cast each column to
		a CloneableObject and clone it.

		@exception StandardException Derby Standard Error Policy
	**/
	public DataValueDescriptor[] getTemplate() throws StandardException {

		return row;

	}

	// set all column of the row to integer object
	private void setIntegerRow() {
		for (int i = 0; i < columnCount; i++)
			this.row[i] = new SQLInteger(i + DEFAULT_SEED);
	}

	private DataValueDescriptor[] setStringRow() {

		T_RawStoreRow row = new T_RawStoreRow(columnCount);

		for (int i = 0; i < columnCount; i++) {
			switch (i % 13) {
			case 0:
				row.setColumn(i, (String) null);
				break;
			case 1:			
				row.setColumn(i, REC_001);
				break;
			case 2:
				row.setColumn(i, REC_002);
				break;
			case 3:
				row.setColumn(i, REC_003);
				break;
			case 4:
				row.setColumn(i, REC_004);
				break;
			case 5:
				row.setColumn(i, REC_005);
				break;
			case 6:
				row.setColumn(i, REC_006);
				break;
			case 7:
				row.setColumn(i, REC_007);
				break;
			case 8:
				row.setColumn(i, (String) null);
				break;
			case 9:
				row.setColumn(i, REC_008);
				break;
			case 10:
				row.setColumn(i, REC_007);
				break;
			case 11:
				row.setColumn(i, (String) null);
				break;
			case 12:
				row.setColumn(i, REC_006);
				break;
			default:
				row.setColumn(i, REC_008);
			}
		}
		return row.getRow();
	}
}
