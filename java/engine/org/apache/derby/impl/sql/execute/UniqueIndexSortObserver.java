/*

   Derby - Class org.apache.derby.impl.sql.execute.UniqueIndexSortObserver

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.SortObserver;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Unique index aggregator.  Enforces uniqueness when
 * creating a unique index or constraint.
 *
 * @author jerry
 */
class UniqueIndexSortObserver extends BasicSortObserver 
{
	private boolean		isConstraint;
	private String		indexOrConstraintName;
	private String 		tableName;

	public UniqueIndexSortObserver(boolean doClone, boolean isConstraint, 
				String indexOrConstraintName, ExecRow execRow, 
				boolean reuseWrappers, String tableName)
	{
		super(doClone, true, execRow, reuseWrappers);
		this.isConstraint = isConstraint;
		this.indexOrConstraintName = indexOrConstraintName;
		this.tableName = tableName;
	}

	/*
	 * Overridden from BasicSortObserver
	 */

	/**
	 * @see AggregateSortObserver#insertDuplicateKey
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public DataValueDescriptor[] insertDuplicateKey(
    DataValueDescriptor[]   in, 
    DataValueDescriptor[]   dup)
		throws StandardException
	{
		StandardException se = null;
		se = StandardException.newException(
				SQLState.LANG_DUPLICATE_KEY_CONSTRAINT, indexOrConstraintName, tableName);
		throw se;
	}

}
