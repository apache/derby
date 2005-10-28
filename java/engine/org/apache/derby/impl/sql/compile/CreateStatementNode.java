/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateStatementNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A CreateStatementNode represents a DDL statement that creates something.
 * It contains the name of the object to be created.
 *
 * @author Jeff Lichtman
 */

public abstract class CreateStatementNode extends DDLStatementNode
{
	// Synchronization stubs

	/**
	 * Add or drop an item in a given list.
	 *
	 * @param newClause		The item to add or drop
	 * @param listNumber	The identifying number of the list to add to
	 *						or drop from.
	 * @param ddlType		ADD_TYPE or DROP_TYPE
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void alterItem
	(
		QueryTreeNode			newClause,
		int					listNumber,
		int					ddlType
	)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"alterItem() not expected to be called on a " +
				getClass().getName());
		}
	}

	/**
	  Return true if this is a create publication node ad false if it is
	  an alter publication node.
	  */
	public boolean isCreate()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"isCreate() not expected to be called on a " +
				getClass().getName());
		}

		return false;
	}
}
