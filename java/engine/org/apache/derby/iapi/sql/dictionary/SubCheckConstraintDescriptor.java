/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.dictionary
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
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
