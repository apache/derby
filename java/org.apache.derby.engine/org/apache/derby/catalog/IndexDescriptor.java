/*

   Derby - Class org.apache.derby.catalog.IndexDescriptor

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

package org.apache.derby.catalog;

/**
 *	
 * This interface describes an index.
 * 
 * It is used in the column SYS.SYSCONGLOMERATES.DESCRIPTOR
 * and describes everything about an index except the index name and 
 * the table on which the index is defined.
 * That information is available 
 * in the columns NAME and TABLEID of the table SYS.SYSCONGLOMERATES.
 * <p>
 * Whereas non-deferrable constraints are backed by UNIQUE indexes,
 * deferrable constraints are backed by non-unique indexes. The duplicate
 * checking on inserts and updates for deferrable constraints are handled at
 * the language level, not by the store level. The following table shows
 * the correspondence between the constraint types and the index attributes
 * used:
 * <ul>
 *  <li>Non-deferrable PRIMARY KEY and UNIQUE NOT NULL on all constraint
 *     columns
 *  <pre>
 *                            \  Value  | Number of index columns | Check
 *   Attribute                 \        | in physical BTree key   | in
 *   --------------------------------------------------------------------
 *   unique                     | true  | N - 1 (row location     |
 *   isUniqueWithDuplicateNulls | false |        not part of key) | Store
 *   uniqueDeferrable           | false |                         | Btree
 *   hasDeferrableChecking      | false |                         | code
 *  </pre>
 *  <li>Non-deferrable UNIQUE, where at least one constraint column is
 *      nullable.
 *  <pre>
 *                            \  Value  | Number of index columns | Check
 *   Attribute                 \        | in physical BTree key   | in
 *   ------------------------------------------------------------ -------
 *   unique                     | false | N                       |
 *   isUniqueWithDuplicateNulls | true  |                         | Store
 *   uniqueDeferrable           | false |                         | Btree
 *   hasDeferrableChecking      | false |                         | code
 *  </pre>
 *  <li>Deferrable PRIMARY KEY and UNIQUE NOT NULL on all constraint
 *     columns
 *  <pre>
 *                            \  Value  | Number of index columns | Check
 *   Attribute                 \        | in physical BTree key   | in
 *   ------------------------------------------------------------ -------
 *   unique                     | false | N                       |
 *   isUniqueWithDuplicateNulls | false |                         | Lang.
 *   uniqueDeferrable           | true  |                         | code
 *   hasDeferrableChecking      | true  |                         |
 *  </pre>
 *  <li>Deferrable UNIQUE, where at least one constraint column is
 *      nullable.
 *  <pre>
 *                            \  Value  | Number of index columns | Check
 *   Attribute                 \        | in physical BTree key   | in
 *   ------------------------------------------------------------ -------
 *   unique                     | false | N                       |
 *   isUniqueWithDuplicateNulls | true  |                         | Lang.
 *   uniqueDeferrable           | false |                         | code
 *   hasDeferrableChecking      | true  |                         |
 *  </pre>
 *  </ul>
 */
public interface IndexDescriptor
{
	/**
	 * Returns true if the index is unique.
	 */
	boolean			isUnique();
	/**
	 * Returns true if the index is duplicate keys only for null key parts. 
     * This is effective only if isUnique is false.
	 */
	boolean			isUniqueWithDuplicateNulls();

    /**
     * The index represents a PRIMARY KEY or a UNIQUE NOT NULL constraint which
     * is deferrable.
     * {@code true} implies {@code isUnique() == false} and
     * {@code isUniqueWithDuplicateNulls() == false} and
     * {@code hasDeferrableChecking() == true}.

     * @return {@code true} if the index represents such a constraint
     */
    boolean isUniqueDeferrable();

    /**
     * Returns true if the index is used to support a deferrable constraint.
     */
    boolean hasDeferrableChecking();

	/**
	 * Returns an array of column positions in the base table.  Each index
	 * column corresponds to a column position in the base table, except
	 * the column representing the location of the row in the base table.
	 * The returned array holds the column positions in the
	 * base table, so, if entry 2 is the number 4, the second
	 * column in the index is the fourth column in the table.
	 */
	public int[]	baseColumnPositions();

	/**
     * Returns the postion of a column.
     * <p>
	 * Returns the position of a column within the key (1-based).
	 * 0 means that the column is not in the key.  Same as the above
	 * method, but it uses int instead of Integer.
	 */
	public int getKeyColumnPosition(int heapColumnPosition);

	/**
	 * Returns the number of ordered columns.  
     * <p>
	 * In the future, it will be
	 * possible to store non-ordered columns in an index.  These will be
	 * useful for covered queries.  The ordered columns will be at the
	 * beginning of the index row, and they will be followed by the
	 * non-ordered columns.
	 *
	 * For now, all columns in an index must be ordered.
	 */
	int				numberOfOrderedColumns();

	/**
	 * Returns the type of the index.  For now, we only support B-Trees,
	 * so the value "BTREE" is returned.
	 */
	String			indexType();

	/**
	 * Returns array of boolean telling asc/desc info for each index
	 * key column for convenience of using together with baseColumnPositions
	 * method.  Both methods return an array with subscript starting from 0.
	 */
	public boolean[]	isAscending();

	/**
	 * Returns true if the specified column is ascending in the index
	 * (1-based).
	 */
	boolean			isAscending(Integer keyColumnPosition);

	/**
	 * Returns true if the specified column is descending in the index
	 * (1-based).  In the current release, only ascending columns are
	 * supported.
	 */
	boolean			isDescending(Integer keyColumnPosition);

	/**
	 * set the baseColumnPositions field of the index descriptor.  This
	 * is for updating the field in operations such as "alter table drop
	 * column" where baseColumnPositions is changed.
	 */
	public void     setBaseColumnPositions(int[] baseColumnPositions);

	/**
	 * set the isAscending field of the index descriptor.  This
	 * is for updating the field in operations such as "alter table drop
	 * column" where isAscending is changed.
	 */
	public void     setIsAscending(boolean[] isAscending);

	/**
	 * set the numberOfOrderedColumns field of the index descriptor.  This
	 * is for updating the field in operations such as "alter table drop
	 * column" where numberOfOrderedColumns is changed.
	 */
	public void     setNumberOfOrderedColumns(int numberOfOrderedColumns);
}
