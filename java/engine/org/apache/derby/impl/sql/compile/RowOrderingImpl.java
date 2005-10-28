/*

   Derby - Class org.apache.derby.impl.sql.compile.RowOrderingImpl

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.compile.Optimizable;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import java.util.Vector;

class RowOrderingImpl implements RowOrdering {

	/* This vector contains ColumnOrderings */
	Vector ordering;

	/*
	** This ColumnOrdering represents the columns that can be considered
	** ordered no matter what.  For example, columns that are compared to
	** constants with = are always ordered.  Also, all columns in a one-row
	** result set are ordered.
	*/
	ColumnOrdering	columnsAlwaysOrdered;

	/*
	** This vector contains table numbers for tables that are always ordered.
	** This happens for one-row tables.
	*/
	Vector alwaysOrderedOptimizables;

	ColumnOrdering	currentColumnOrdering;

	/* This vector contains unordered Optimizables */
	Vector unorderedOptimizables;

	RowOrderingImpl() {
		ordering = new Vector();
		unorderedOptimizables = new Vector();
		columnsAlwaysOrdered = new ColumnOrdering(RowOrdering.DONTCARE);
		alwaysOrderedOptimizables = new Vector();
	}

	/**
	 * @see RowOrdering#orderedOnColumn
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean orderedOnColumn(int direction,
									int orderPosition,
									int tableNumber,
									int columnNumber)
				throws StandardException {

		/*
		** Return true if the table is always ordered.
		*/
		if (vectorContainsOptimizable(tableNumber, alwaysOrderedOptimizables))
		{
			return true;
		}

		/*
		** Return true if the column is always ordered.
		*/
		if (columnsAlwaysOrdered.contains(tableNumber, columnNumber)) {
			return true;
		}

		/*
		** Return false if we're looking for an ordering position that isn't
		** in this ordering.
		*/
		if (orderPosition >= ordering.size())
			return false;

		ColumnOrdering co = (ColumnOrdering) ordering.elementAt(orderPosition);

		/*
		** Is the column in question ordered with the given direction at
		** this position?
		*/
		return co.ordered(direction, tableNumber, columnNumber);
	}

	/**
	 * @see RowOrdering#orderedOnColumn
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean orderedOnColumn(int direction,
									int tableNumber,
									int columnNumber)
				throws StandardException {

		/*
		** Return true if the table is always ordered.
		*/
		if (vectorContainsOptimizable(tableNumber, alwaysOrderedOptimizables))
		{
			return true;
		}

		/*
		** Return true if the column is always ordered.
		*/
		if (columnsAlwaysOrdered.contains(tableNumber, columnNumber)) {
			return true;
		}

		boolean ordered = false;

		for (int i = 0; i < ordering.size(); i++) {
			ColumnOrdering co = (ColumnOrdering) ordering.elementAt(i);

			/*
			** Is the column in question ordered with the given direction at
			** this position?
			*/
			boolean thisOrdered = co.ordered(direction,
											tableNumber,
											columnNumber);

			if (thisOrdered) {
				ordered = true;
				break;
			}
		}

		return ordered;
	}

	/**
	 * Return true if the given vector of Optimizables contains an Optimizable
	 * with the given table number.
	 */
	private boolean vectorContainsOptimizable(int tableNumber, Vector vec)
	{
		int i;

		for (i = vec.size() - 1; i >= 0; i--)
		{
			Optimizable optTable =
							(Optimizable) vec.elementAt(i);

			if (optTable.hasTableNumber())
			{
				if (optTable.getTableNumber() == tableNumber)
				{
					return true;
				}
			}
		}

		return false;
	}

	/** @see RowOrdering#addOrderedColumn */
	public void addOrderedColumn(int direction,
								int tableNumber,
								int columnNumber)
	{
		if (unorderedOptimizables.size() > 0)
			return;

		ColumnOrdering currentColumnOrdering;

		if (ordering.size() == 0)
		{
			currentColumnOrdering = new ColumnOrdering(direction);
			ordering.addElement(currentColumnOrdering);
		}
		else
		{
			currentColumnOrdering =
				(ColumnOrdering) ordering.elementAt(ordering.size() - 1);
		}

		if (SanityManager.DEBUG)
		{
			if (currentColumnOrdering.direction() != direction)
			{
				SanityManager.THROWASSERT("direction == " + direction +
					", currentColumnOrdering.direction() == " +
					currentColumnOrdering.direction());
			}
		}

		currentColumnOrdering.addColumn(tableNumber, columnNumber);
	}

	/** @see RowOrdering#nextOrderPosition */
	public void nextOrderPosition(int direction)
	{
		if (unorderedOptimizables.size() > 0)
			return;

		currentColumnOrdering = new ColumnOrdering(direction);
		ordering.addElement(currentColumnOrdering);
	}

	public void optimizableAlwaysOrdered(Optimizable optimizable)
	{
		// A table can't be ordered if there is an outer unordered table
		if (unorderedOptimizablesOtherThan(optimizable))
		{
			return;
		}

		/*
		** A table is not "always ordered" if any of the other ordered tables
		** in the join order are not also "always ordered".  In other words,
		** if any outer table is not a one-row table, this table is not
		** always ordered.
		**
		** The table that was passed in as a parameter may have already been
		** added as a table with ordered columns.  If it is the first table
		** in the list of ordered columns, then there should be no other
		** tables in this list, so we remove it from the list and add it
		** to the list of always-ordered tables.
		*/
		boolean hasTableNumber = optimizable.hasTableNumber();
		int tableNumber = (hasTableNumber ? optimizable.getTableNumber() : 0);
		if (
			(
				(ordering.size() == 0) ||
				(
					hasTableNumber &&
					((ColumnOrdering) ordering.elementAt(0)).hasTable(
																	tableNumber)
				)
			)
			&&
			(
				hasTableNumber &&
				! columnsAlwaysOrdered.hasAnyOtherTable(tableNumber)
			)
		   )
		{
			if (optimizable.hasTableNumber())
				removeOptimizable(optimizable.getTableNumber());

			alwaysOrderedOptimizables.addElement(optimizable);
		}
	}

	/** @see RowOrdering#columnAlwaysOrdered */
	public void columnAlwaysOrdered(Optimizable optimizable, int columnNumber)
	{
		columnsAlwaysOrdered.addColumn(optimizable.getTableNumber(),
										columnNumber);
	}

	/** @see RowOrdering#alwaysOrdered */
	public boolean alwaysOrdered(int tableNumber)
	{
		return vectorContainsOptimizable(
										tableNumber,
										alwaysOrderedOptimizables
										);
	}

	/** @see RowOrdering#removeOptimizable */
	public void removeOptimizable(int tableNumber)
	{
		int i;

		/*
		** Walk the list backwards, so we can remove elements
		** by position.
		*/
		for (i = ordering.size() - 1; i >= 0; i--)
		{
			/*
			** First, remove the table from all the ColumnOrderings
			*/
			ColumnOrdering ord = (ColumnOrdering) ordering.elementAt(i);
			ord.removeColumns(tableNumber);
			if (ord.empty())
				ordering.removeElementAt(i);
		}

		/* Remove from list of always-ordered columns */
		columnsAlwaysOrdered.removeColumns(tableNumber);

		/* Also remove from list of unordered optimizables */
		removeOptimizableFromVector(tableNumber, unorderedOptimizables);

		/* Also remove from list of always ordered optimizables */
		removeOptimizableFromVector(tableNumber, alwaysOrderedOptimizables);
	}

	/**
	 * Remove all optimizables with the given table number from the
	 * given vector of optimizables.
	 */
	private void removeOptimizableFromVector(int tableNumber, Vector vec)
	{
		int i;

		for (i = vec.size() - 1; i >= 0; i--)
		{
			Optimizable optTable =
							(Optimizable) vec.elementAt(i);

			if (optTable.hasTableNumber())
			{
				if (optTable.getTableNumber() == tableNumber)
				{
					vec.removeElementAt(i);
				}
			}
		}
	}

	/** @see RowOrdering#addUnorderedOptimizable */
	public void addUnorderedOptimizable(Optimizable optimizable)
	{
		unorderedOptimizables.addElement(optimizable);
	}

	/** @see RowOrdering#copy */
	public void copy(RowOrdering copyTo) {
		if (SanityManager.DEBUG) {
			if ( ! (copyTo instanceof RowOrderingImpl) ) {
				SanityManager.THROWASSERT(
					"copyTo should be a RowOrderingImpl, is a " +
					copyTo.getClass().getName());
			}
		}

		RowOrderingImpl dest = (RowOrderingImpl) copyTo;

		/* Clear the ordering of what we're copying to */
		dest.ordering.removeAllElements();
		dest.currentColumnOrdering = null;

		dest.unorderedOptimizables.removeAllElements();
		for (int i = 0; i < unorderedOptimizables.size(); i++) {
			dest.unorderedOptimizables.addElement(
											unorderedOptimizables.elementAt(i));
		}

		dest.alwaysOrderedOptimizables.removeAllElements();
		for (int i = 0; i < alwaysOrderedOptimizables.size(); i++) {
			dest.alwaysOrderedOptimizables.addElement(
										alwaysOrderedOptimizables.elementAt(i));
		}

		for (int i = 0; i < ordering.size(); i++) {
			ColumnOrdering co = (ColumnOrdering) ordering.elementAt(i);

			dest.ordering.addElement(co.cloneMe());

			if (co == currentColumnOrdering)
				dest.rememberCurrentColumnOrdering(i);
		}

		dest.columnsAlwaysOrdered = null;
		if (columnsAlwaysOrdered != null)
			dest.columnsAlwaysOrdered = columnsAlwaysOrdered.cloneMe();
	}

	private void rememberCurrentColumnOrdering(int posn) {
		currentColumnOrdering = (ColumnOrdering) ordering.elementAt(posn);
	}

	public String toString() {
		String retval = null;

		if (SanityManager.DEBUG) {
			int i;

			retval = "Unordered optimizables: ";

			for (i = 0; i < unorderedOptimizables.size(); i++) 
			{
				Optimizable opt = (Optimizable) unorderedOptimizables.elementAt(i);
				if (opt.getBaseTableName() != null)
				{
					retval += opt.getBaseTableName();
				}
				else
				{
					retval += unorderedOptimizables.elementAt(i).toString();
				}
				retval += " ";
			}
			retval += "\n";

			retval += "\nAlways ordered optimizables: ";

			for (i = 0; i < alwaysOrderedOptimizables.size(); i++) 
			{
				Optimizable opt = (Optimizable) alwaysOrderedOptimizables.elementAt(i);
				if (opt.getBaseTableName() != null)
				{
					retval += opt.getBaseTableName();
				}
				else
				{
					retval += alwaysOrderedOptimizables.elementAt(i).toString();
				}
				retval += " ";
			}
			retval += "\n";

			for (i = 0; i < ordering.size(); i++) {
				retval += " ColumnOrdering " + i + ": " + ordering.elementAt(i);
			}
		}

		return retval;
	}

	/**
	 * Returns true if there are unordered optimizables in the join order
	 * other than the given one.
	 */
	private boolean unorderedOptimizablesOtherThan(Optimizable optimizable)
	{
		for (int i = 0; i < unorderedOptimizables.size(); i++)
		{
			Optimizable thisOpt =
				(Optimizable) unorderedOptimizables.elementAt(i);

			if (thisOpt != optimizable)
				return true;
		}

		return false;
	}
}
