/*

   Derby - Class org.apache.derby.impl.sql.compile.GroupByColumn

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Vector;

/**
 * A GroupByColumn is a column in the GROUP BY clause.
 *
 * @author jerry
 */
public class GroupByColumn extends OrderedColumn 
{
	private ColumnReference	colRef;

	/**
	 * Initializer.
	 *
	 * @param colRef	The ColumnReference for the grouping column
	 */
	public void init(Object colRef) 
	{
		this.colRef = (ColumnReference) colRef;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
	public String toString() 
	{
		if (SanityManager.DEBUG)
		{
			return "Column Reference: "+colRef+super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 *
	 * @return	Nothing
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (colRef != null)
			{
				printLabel(depth, "colRef: ");
				colRef.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Get the name of this column
	 *
	 * @return	The name of this column
	 */
	public String getColumnName() 
	{
		return colRef.getColumnName();
	}

	/**
	 * Get the ColumnReference from this GroupByColumn.
	 *
	 * @return ColumnReference	The ColumnReference from this node.
	 */
	public ColumnReference getColumnReference()
	{
		return colRef;
	}

	/**
	 * Set the ColumnReference for this GroupByColumn.
	 *
	 * @param colRef	The new ColumnReference for this node.
	 *
	 * @return Nothing.
	 */
	public void setColumnReference(ColumnReference colRef)
	{
		this.colRef = colRef;
	}

	/**
	 * Get the table number for this GroupByColumn.
	 *
	 * @return	int The table number for this GroupByColumn
	 */

	public int getTableNumber()
	{
		return colRef.getTableNumber();
	}

	/**
	 * Get the source this GroupByColumn
	 *
	 * @return	The source of this GroupByColumn
	 */

	public ResultColumn getSource()
	{
		return colRef.getSource();
	}

	/**
	 * Bind this grouping column.
	 *
	 * @param fromList			The FROM list to use for binding
	 * @param subqueryList		The SubqueryList we are building as we hit
	 *							SubqueryNodes.
	 * @param aggregateVector	The aggregate vector we build as we hit 
	 *							AggregateNodes.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException	Thrown on error
	 */

	public void bindExpression(
			FromList fromList, 
			SubqueryList subqueryList,
			Vector	aggregateVector) 
				throws StandardException
	{
		/* Bind the ColumnReference to the FromList */
		colRef = (ColumnReference) colRef.bindExpression(fromList,
							  subqueryList,
							  aggregateVector);

		// Verify that we can group on the column

		/*
		 * Do not check to see if we can map user types
		 * to built-in types.  The ability to do so does
		 * not mean that ordering will work.  In fact,
		 * as of version 2.0, ordering does not work on
		 * user types.
		 */
		TypeId ctid = colRef.getTypeId();
		if (! ctid.orderable(getClassFactory()))
		{
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION, 
							ctid.getSQLTypeName());
		}
	}
}
