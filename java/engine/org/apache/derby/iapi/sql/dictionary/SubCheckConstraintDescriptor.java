/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.SubCheckConstraintDescriptor

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

package org.apache.derby.iapi.sql.dictionary;


import org.apache.derby.catalog.ReferencedColumns;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.sanity.SanityManager;
/**
 * This interface is used to get information from a SubCheckConstraintDescriptor.
 * A SubCheckConstraintDescriptor is used within the DataDictionary to 
 * get auxiliary constraint information from the system table
 * that is auxiliary to sysconstraints.
 *
 * @version 0.1
 * @author Jerry Brenner
 */

public class SubCheckConstraintDescriptor extends SubConstraintDescriptor
{
	/** public interface to this class:
		<ol>
		<li>public String getConstraintText();</li>
		<li>public ReferencedColumns getReferencedColumnsDescriptor();</li>
		</ol>
	*/

	// Implementation
	private ReferencedColumns referencedColumns;
	private String						constraintText;

	/**
	 * Constructor for a SubCheckConstraintDescriptor
	 *
	 * @param constraintID		The UUID of the constraint.
	 * @param constraintText	The text of the constraint definition.
	 * @param referencedColumns	The columns referenced by the check constraint
	 */

	public SubCheckConstraintDescriptor(UUID constraintId, String constraintText,
									 ReferencedColumns referencedColumns)
	{
		super(constraintId);
		this.constraintText = constraintText;
		this.referencedColumns = referencedColumns;
	}

	/**
	 * Get the text of the check constraint definition.
	 *
	 * @return The text of the check constraint definition.
	 */
	public String getConstraintText()
	{
		return constraintText;
	}

	/**
	 * Get the ReferencedColumns.
	 *
	 * @return The ReferencedColumns.
	 */
	public ReferencedColumns getReferencedColumnsDescriptor()
	{
		return referencedColumns;
	}

	/**
	 * Does this constraint have a backing index?
	 *
	 * @return boolean	Whether or not there is a backing index for this constraint.
	 */
	public boolean hasBackingIndex()
	{
		return false;
	}

	/**
	 * Convert the SubCheckConstraintDescriptor to a String.
	 *
	 * @return	A String representation of this SubCheckConstraintDescriptor
	 */

	public String	toString()
	{
		if (SanityManager.DEBUG)
		{
			return "constraintText: " + constraintText + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

}
