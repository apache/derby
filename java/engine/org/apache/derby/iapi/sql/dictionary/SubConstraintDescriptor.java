/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.dictionary
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.dictionary;


import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * This interface is used to get information from a SubConstraintDescriptor.
 * A SubKeyConstraintDescriptor is used within the DataDictionary to 
 * get auxiliary constraint information from the system table
 * that is auxiliary to sysconstraints.
 *
 * @version 0.1
 * @author Jerry Brenner
 */

public abstract class SubConstraintDescriptor extends TupleDescriptor
	implements UniqueTupleDescriptor
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
	   public interface for this class:
	   <ol>
	   <li> public void	setConstraintId(UUID constraintId);</li>
	   <li>public boolean hasBackingIndex();</li>
	   <li>public void	setTableDescriptor(TableDescriptor td);</li>
	   <li>public TableDescriptor getTableDescriptor();</li>
	   </ol>
	*/

	// Implementation
	TableDescriptor			td;
	UUID					constraintId;

	/**
	 * Constructor for a SubConstraintDescriptorImpl
	 *
	 * @param constraintID		The UUID of the constraint.
	 */

	SubConstraintDescriptor(UUID constraintId)
	{
		this.constraintId = constraintId;
	}

	/**
	 * Sets the UUID of the constraint.
	 *
	 * @param constraintId	The constraint Id.
	 * @return	Nothing.
	 */
	public void	setConstraintId(UUID constraintId)
	{
		this.constraintId = constraintId;
	}

	/**
	 * Gets the UUID of the constraint.
	 *
	 * @return	The UUID of the constraint.
	 */
	public UUID	getUUID()
	{
		return constraintId;
	}

	/**
	 * Does this constraint have a backing index?
	 *
	 * @return boolean	Whether or not there is a backing index for this constraint.
	 */
	public abstract boolean hasBackingIndex();

	/**
	 * Caches the TableDescriptor of the 
	 * table that the constraint is on.
	 *
	 * @param td	The TableDescriptor.
	 * @return	Nothing.
	 */
	public void	setTableDescriptor(TableDescriptor td)
	{
		this.td = td;
	}

	/** 
	 * Returns the cached TableDescriptor, if
	 * supplied, that the constraint is on.
	 *
	 * @return The cached TableDescriptor, 
	 * if supplied.
	 */
	public TableDescriptor getTableDescriptor()
	{
		return td;
	}

	/**
	 * Convert the SubConstraintDescriptor to a String.
	 *
	 * @return	A String representation of this SubConstraintDescriptor
	 */

	public String	toString()
	{
		if (SanityManager.DEBUG)
		{
			return "constraintId: " + constraintId + "\n";
		}
		else
		{
			return "";
		}
	}

}
