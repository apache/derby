/*

   Derby - Class org.apache.derby.iapi.sql.execute.ExecIndexRow

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

package org.apache.derby.iapi.sql.execute;

/**
 * This is an extension of ExecRow for use
 * with indexes and sorting.
 *
 */
public interface ExecIndexRow extends ExecRow  {

	/**
	 * These two methods are a sort of a hack.  The store implements ordered
	 * null semantics for start and stop positioning, which is correct for
	 * IS NULL and incorrect for everything else.  To work around this,
	 * TableScanResultSet will check whether the start and stop positions
	 * have NULL in any column position other than for an IS NULL check.
	 * If so, it won't do the scan (that is, it will return no rows).
	 *
	 * This method is to inform this ExecIndexRow (which can be used for
	 * start and stop positioning) that the given column uses ordered null
	 * semantics.
	 *
	 * @param columnPosition	The position of the column that uses ordered
	 *							null semantics (zero-based).
	 */
	void orderedNulls(int columnPosition);

	/**
	 * Return true if orderedNulls was called on this ExecIndexRow for
	 * the given column position.
	 *
	 * @param columnPosition	The position of the column (zero-based) for
	 *							which we want to check if ordered null semantics
	 *							are used.
	 *
	 * @return	true if we are to use ordered null semantics on the given column
	 */
	boolean areNullsOrdered(int columnPosition);

	/**
	 * Turn the ExecRow into an ExecIndexRow.
	 */
	public void execRowToExecIndexRow(ExecRow valueRow);
}
