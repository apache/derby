/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
