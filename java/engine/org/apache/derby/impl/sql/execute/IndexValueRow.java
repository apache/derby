/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.sql.ResultSet;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
	Mapper of ValueRow into ExecIndexRow. 

	@author ames
 */
class IndexValueRow implements ExecIndexRow {

	private ExecRow valueRow;

	IndexValueRow(ExecRow valueRow) {
		 this.valueRow = valueRow;
	}

	/*
	 * class interface
	 */
	public String toString() {
		return valueRow.toString();
	}


	/**
		Get the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArray() {
		return valueRow.getRowArray();
	}

	/**	@see ExecRow#getRowArray */
	public void setRowArray(DataValueDescriptor[] value) 
	{
		valueRow.setRowArray(value);
	}
	public void setRowArray(Storable[] value) 
	{
		valueRow.setRowArray(value);
	}

	/**
		Get a clone of the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArrayClone() 
	{
		return valueRow.getRowArrayClone();
	}

	// this is the actual current # of columns
	public int nColumns() {
		return valueRow.nColumns();
	}

	/*
	 * Row interface
	 */
	// position is 1-based
	public DataValueDescriptor	getColumn (int position) throws StandardException {
		return valueRow.getColumn(position);
	}

	// position is 1-based.
	public void setColumn(int position, DataValueDescriptor col) {
		valueRow.setColumn(position, col);
	}

	// position is 1-based
	public ExecRow getClone() {
		return new IndexValueRow(valueRow.getClone());
	}

	public ExecRow getClone(FormatableBitSet clonedCols) {
		return new IndexValueRow(valueRow.getClone(clonedCols));
	}

	public ExecRow getNewNullRow() {
		return new IndexValueRow(valueRow.getNewNullRow());
	}

	// position is 1-based
	public DataValueDescriptor cloneColumn(int columnPosition)
	{
		return valueRow.cloneColumn(columnPosition);
	}

	/*
	 * ExecIndexRow interface
	 */

	public void orderedNulls(int columnPosition) {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("Not expected to be called");
		}
	}

	public boolean areNullsOrdered(int columnPosition) {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("Not expected to be called");
		}

		return false;
	}

	/**
	 * Turn the ExecRow into an ExecIndexRow.
	 *
	 * @return Nothing.
	 */
	public void execRowToExecIndexRow(ExecRow valueRow)
	{
		this.valueRow = valueRow;
	}

	public void getNewObjectArray() 
	{
		valueRow.getNewObjectArray();
	}
}
