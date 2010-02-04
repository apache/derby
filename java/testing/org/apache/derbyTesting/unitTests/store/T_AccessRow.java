/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_AccessRow

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.unitTests.store;

import org.apache.derby.iapi.store.access.*;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.SQLInteger;

import org.apache.derby.iapi.error.StandardException;

public class T_AccessRow
{ 

	protected DataValueDescriptor column[];

	/**
	Construct a new row which can hold the provided number of columns.
	**/
	public T_AccessRow(int ncols)
	{
		 column = new DataValueDescriptor[ncols];
		 for (int i = 0; i < ncols; i++)
			 column[i] = new SQLInteger(0);
	}

	/**
	Construct a new row with three integer columns which
	have the column values provided.
	**/
	public T_AccessRow(int col0value, int col1value, int col2value)
	{
		column = new DataValueDescriptor[3];
		column[0] = new SQLInteger(col0value);
		column[1] = new SQLInteger(col1value);
		column[2] = new SQLInteger(col2value);
	}

	public DataValueDescriptor getCol(int colid)
	{
		if (colid >= column.length)
			return null;
		else
			return column[colid];
	}

	public void setCol(int colid, DataValueDescriptor val)
	{
		if (colid >= column.length)
			realloc(colid + 1);
		column[colid] = val;
	}

	public boolean equals(T_AccessRow other) throws StandardException
	{
		if (other == null)
			return false;
		if (other.column.length != this.column.length)
			return false;
		for (int i = 0; i < this.column.length; i++)
			if (this.column[i].compare(other.column[i]) != 0)
				return false;
		return true;
	}

	public String toString()
	{
		String s = "{ ";
		for (int i = 0; i < column.length; i++)
		{
			s += column[i].toString();
			if (i < (column.length - 1))
				s += ", ";
		}
		s += " }";
		return s;
	}

	// Set the number of columns in the row to ncols, preserving
	// the existing contents.
	protected void realloc(int ncols)
	{
		DataValueDescriptor newcol[] = new DataValueDescriptor[ncols];
		for (int i = 0; i < column.length; i++)
			newcol[i] = column[i];
		column = newcol;
	}

	public Storable getStorableColumn(int colid)
	{
		return column[colid];
	}

	public void setStorableColumn(int colid, Storable value) {
		column[colid] = (DataValueDescriptor) value;
	}

	public int nColumns()
	{
		return column.length;
	}

	public DataValueDescriptor[] getRowArray() {
		return column;
	}

	public DataValueDescriptor[] getRowArrayClone() {
		DataValueDescriptor[] retval = new DataValueDescriptor[column.length];
		for (int index = 0; index < column.length; index++)
			retval[index] = column[index].cloneValue(false);
		return retval;
	}
}




