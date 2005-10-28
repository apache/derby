/*

   Derby - Class org.apache.derby.iapi.sql.compile.RowOrdering

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.error.StandardException;

/**
 * This interface provides a representation of the ordering of rows in a
 * ResultSet.
 */
public interface RowOrdering
{
	static final int	ASCENDING = 1;
	static final int	DESCENDING = 2;
	static final int	DONTCARE = 3;

	/**
	 * Tell whether this ordering is ordered on the given column in
	 * the given position
	 *
	 * @param direction		One of ASCENDING, DESCENDING, or DONTCARE
	 *						depending on the requirements of the caller.
	 *						An ORDER BY clause cares about direction,
	 *						while DISTINCT and GROUP BY do not.
	 * @param orderPosition	The position in the ordering list.  For example,
	 *						for ORDER BY A, B, position 0 has column A,
	 *						and position 1 has column B.  Note that for an
	 *						ordering, more than one column can be in a single
	 *						ordering position: for example, in the query
	 *						SELECT * FROM S, T WHERE S.A = T.B ORDER BY T.B
	 *						columns S.A and T.B will be in the same ordering
	 *						positions because they are equal.  Also, constant
	 *						values are considered ordered in all positions
	 *						(consider SELECT A FROM T WHERE A = 1 ORDER BY A).
	 * @param tableNumber	The table number of the Optimizable containing
	 *						the column in question
	 * @param columnNumber	The column number in the table (one-based).
	 *
	 * @return	true means this ordering is ordered on the given column
	 *			in the given position.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean orderedOnColumn(int direction,
							int orderPosition,
							int tableNumber,
							int columnNumber)
			throws StandardException;

	/**
	 * Tell whether this ordering is ordered on the given column.
	 * This is similar to the method above, but it checks whether the
	 * column is ordered in any position, rather than a specified position.
	 * This is useful for operations like DISTINCT and GROUP BY.
	 *
	 * @param direction		One of ASCENDING, DESCENDING, or DONTCARE
	 *						depending on the requirements of the caller.
	 *						An ORDER BY clause cares about direction,
	 *						while DISTINCT and GROUP BY do not.
	 * @param tableNumber	The table number of the Optimizable containing
	 *						the column in question
	 * @param columnNumber	The column number in the table (one-based).
	 *
	 * @return	true means this ordering is ordered on the given column
	 *			in the given position.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean orderedOnColumn(int direction,
							int tableNumber,
							int columnNumber)
			throws StandardException;

	/**
	 * Add a column to this RowOrdering in the current order position.
	 * This is a no-op if there are any unordered optimizables in the
	 * join order (see below).
	 *
	 * @param direction		One of ASCENDING, DESCENDING, or DONTCARE.
	 *						DONTCARE can be used for things like columns
	 *						with constant value, and for one-row tables.
	 * @param tableNumber	The table the column is in.
	 * @param columnNumber	The column number in the table (one-based)
	 */
	void addOrderedColumn(int direction,
							int tableNumber,
							int columnNumber);

	/**
	 * Move to the next order position for adding ordered columns.
	 * This is a no-op if there are any unordered optimizables in the
	 * join order (see below).
	 *
	 * @param direction		One of ASCENDING, DESCENDING, or DONTCARE.
	 *						DONTCARE can be used for things like columns
	 *						with constant value, and for one-row tables.
	 */
	void nextOrderPosition(int direction);

	/**
	 * Tell this RowOrdering that it is always ordered on the given optimizable
	 * This is useful when considering a unique index where there is an
	 * equality match on the entire key - in this case, all the columns
	 * are ordered, regardless of the direction or position,
	 * or even whether the columns are in the index.
	 *
	 * @param optimizable	The table in question
	 */
	void optimizableAlwaysOrdered(Optimizable optimizable);

	/**
	 * Tell this RowOrdering that it is always ordered on the given column
	 * of the given optimizable.  This is useful when a column in the
	 * optimizable has an equals comparison with a constant expression.
	 * This is reset when the optimizable is removed from this RowOrdering.
	 *
	 * @param optimizable	The table in question
	 * @param columnNumber	The number of the column in question.
	 */
	void columnAlwaysOrdered(Optimizable optimizable, int columnNumber);

	/**
	 * Ask whether the given table is always ordered.
	 */
	boolean alwaysOrdered(int tableNumber);

	/**
	 * Tell this row ordering that it is no longer ordered on the given
	 * table.  Also, adjust the current order position, if necessary.
	 * This only works to remove ordered columns from the end of the
	 * ordering.
	 *
	 * @param tableNumber	The number of the table to remove from 
	 *						this RowOrdering.
	 */
	void removeOptimizable(int tableNumber);

	/**
	 * Add an unordered optimizable to this RowOrdering.  This is to
	 * solve the following problem:
	 *
	 * Suppose we have the query:
	 *
	 *		select * from r, s, t order by r.a, t.b
	 *
	 * Also suppose there are indexes on r.a and t.b.  When the
	 * optimizer considers the join order (r, s, t) using the index
	 * on r.a, the heap on s, and the index on t.b, the rows from the
	 * join order will *NOT* be ordered on t.b, because there is an
	 * unordered result set between r and t.  So, when s is added to
	 * the partial join order, and we then add table t to the join order,
	 * we want to ensure that we don't add column t.b to the RowOrdering.
	 */
	void addUnorderedOptimizable(Optimizable optimizable);

	/**
	 * Copy the contents of this RowOrdering to the given RowOrdering.
	 */
	void copy(RowOrdering copyTo);
}
