/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

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
