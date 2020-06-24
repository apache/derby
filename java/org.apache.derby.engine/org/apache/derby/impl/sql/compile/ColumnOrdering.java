/*

   Derby - Class org.apache.derby.impl.sql.compile.ColumnOrdering

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

package org.apache.derby.impl.sql.compile;

import java.util.ArrayList;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.shared.common.sanity.SanityManager;

class ColumnOrdering {

    /** See {@link RowOrdering} for possible values. */
	int	myDirection;

    /** A list of column numbers (Integers). */
    private final ArrayList<Integer> columns = new ArrayList<Integer>();

    /**
     * A list of table numbers (Integers), corresponding to the {@code columns}
     * list by position.
     */
    private final ArrayList<Integer> tables = new ArrayList<Integer>();

	/**
	 * @param direction	See RowOrdering for possible values
	 */
	ColumnOrdering(int direction) {
		myDirection = direction;
	}

	/**
	 * Does this ColumnOrdering contain the given column in the given table
	 * in the right direction?
	 *
	 * @param direction		See RowOrdering for possible values
	 * @param tableNumber	The number of the table in question
	 * @param columnNumber	The column number in the table (one-based)
	 *
	 * @return	true if the column is found here in the right direction
	 */
	boolean ordered(int direction, int tableNumber, int columnNumber) {
		/*
		** Check the direction only if the direction isn't DONTCARE
		*/
		if (direction != RowOrdering.DONTCARE) {
			if (direction != myDirection)
				return false;
		}

		/* The direction matches - see if the column is in this ordering */
		return contains(tableNumber, columnNumber);
	}

	/**
	 * Does this ColumnOrdering contain the given column?
	 *
	 * @param tableNumber	The number of table in question
	 * @param columnNumber	The column number in the table (one-based)
	 *
	 * @return	true if the column is found here in the right direction
	 */
	boolean contains(int tableNumber, int columnNumber)
	{
		for (int i = 0; i < columns.size(); i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			Integer col = columns.get(i);
			Integer tab = tables.get(i);

			if (tab.intValue() == tableNumber &&
				col.intValue() == columnNumber) {

				return true;
			}
		}

		return false;
	}

	/**
	 * Get the direction of this ColumnOrdering
	 */
	int direction()
	{
		return myDirection;
	}

	/**
	 * Add a column in a table to this ColumnOrdering
	 *
	 * @param tableNumber	The number of table in question
	 * @param columnNumber	The column number in the table (one-based)
	 */
	void addColumn(int tableNumber, int columnNumber)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6885
        tables.add(tableNumber);
        columns.add(columnNumber);
	}

	/**
	 * Remove all columns with the given table number
	 */
	void removeColumns(int tableNumber)
	{
		/*
		** Walk the list backwards, so we can remove elements
		** by position.
		*/
		for (int i = tables.size() - 1; i >= 0; i--)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			Integer tab = tables.get(i);
			if (tab.intValue() == tableNumber)
			{
				tables.remove(i);
				columns.remove(i);
			}
		}
	}

	/**
	 * Tell whether this ColumnOrdering has no elements.
	 */
	boolean empty()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6075
		return tables.isEmpty();
	}

	/** Return a clone of this ColumnOrdering */
	ColumnOrdering cloneMe() {
		ColumnOrdering retval = new ColumnOrdering(myDirection);

		for (int i = 0; i < columns.size(); i++) {
			/* Integers are immutable, so just copy the pointers */
//IC see: https://issues.apache.org/jira/browse/DERBY-6075
			retval.columns.add(columns.get(i));
			retval.tables.add(tables.get(i));
		}

		return retval;
	}

	/** Is the given table number in this ColumnOrdering? */
	boolean hasTable(int tableNumber) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6885
        return tables.contains(tableNumber);
	}

	/** Is there any table other than the given one in this ColumnOrdering? */
	boolean hasAnyOtherTable(int tableNumber) {

		for (int i = 0; i < tables.size(); i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			Integer tab = tables.get(i);
			
			if (tab.intValue() != tableNumber)
				return true;
		}

		return false;
	}

    @Override
	public String toString() {
		String retval = "";

		if (SanityManager.DEBUG) {
			retval += "Direction: " + myDirection;

			for (int i = 0; i < columns.size(); i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5060
				retval += " Table " + tables.get(i) +
							", Column " + columns.get(i);
			}
		}

		return retval;
	}
}
