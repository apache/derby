/*

   Derby - Class org.apache.derby.iapi.sql.execute.ExecRow

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

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.sql.Row;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 * Execution sees this extension of Row that provides connectivity
 * to the Storage row interface and additional methods for manipulating
 * Rows in execution's ResultSets.
 *
 * @author ames
 */
public interface ExecRow extends Row {

	/**
	 * Clone the Row and its contents.
	 *
	 *
	 * @return Row	A clone of the Row and its contents.
	 */
	ExecRow getClone();

	/**
	 * Clone the Row.  The cloned row will contain clones of the
	 * specified columns and the same object as the original row
	 * for the other columns.
	 *
	 * @param clonedCols	1-based FormatableBitSet representing the columns to clone.
	 *
	 * @return Row	A clone of the Row and its contents.
	 */
	ExecRow getClone(FormatableBitSet clonedCols);

	/**
	 * Get a new row with the same columns type as this one, containing nulls.
	 *
	 */
	ExecRow	getNewNullRow();

	/**
	 * Get a clone of a DataValueDescriptor from an ExecRow.
	 *
	 * @param int columnPosition (1 based)
	 */
	DataValueDescriptor cloneColumn(int columnPosition);

	/**
		Get a clone of the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArrayClone();

	/**
		Return the array of objects that the store needs.
	*/
	public DataValueDescriptor[] getRowArray();

	/**
		Set the array of objects
	*/
	public void setRowArray(Storable[] rowArray);

	// temp overload
	public void setRowArray(DataValueDescriptor[] rowArray);

	/**
		Get a new DataValueDescriptor[]
	 */
	public void getNewObjectArray();
}
