/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.SubKeyConstraintDescriptor

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

package org.apache.derby.iapi.sql.dictionary;


import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * This interface is used to get information from a SubKeyConstraintDescriptor.
 * A SubKeyConstraintDescriptor is used within the DataDictionary to 
 * get auxiliary constraint information from the system table
 * that is auxiliary to sysconstraints.
 *
 * @version 0.1
 * @author Jerry Brenner
 */

public class SubKeyConstraintDescriptor extends SubConstraintDescriptor
{
	/** Interface for SubKeyConstraintDescriptor is 
		<ol>
		<li>public UUID getIndexId();</li>
		<li>public UUID getKeyConstraintId();</li>
		</ol>
	*/

	// Implementation
	UUID					indexId;
	UUID					keyConstraintId;

	int                     raDeleteRule; //referential action rule for a DELETE 
	int                     raUpdateRule; //referential action rule for a UPDATE


	/**
	 * Constructor for a SubConstraintDescriptorImpl
	 *
	 * @param constraintID		The UUID of the constraint.
	 * @param indexId			The UUID of the backing index.
	 */
	public SubKeyConstraintDescriptor(UUID constraintId, UUID indexId)
	{
		super(constraintId);
		this.indexId = indexId;
	}

	/**
	 * Constructor for a SubConstraintDescriptor
	 *
	 * @param constraintID		The UUID of the constraint.
	 * @param indexId			The UUID of the backing index.
	 * @param keyConstraintId	The UUID of the referenced constraint (fks)
	 */
	public SubKeyConstraintDescriptor(UUID constraintId, UUID indexId, UUID keyConstraintId)
	{
		this(constraintId, indexId);
		this.keyConstraintId = keyConstraintId;
	}


	/**
	 * Constructor for a SubConstraintDescriptor
	 *
	 * @param constraintID		The UUID of the constraint.
	 * @param indexId			The UUID of the backing index.
	 * @param keyConstraintId	The UUID of the referenced constraint (fks)
	 * @param raDeleteRule      The referential action for delete
	 * @param raUpdateRule      The referential action for update
	 */
	public SubKeyConstraintDescriptor(UUID constraintId, UUID indexId, UUID
									  keyConstraintId, int raDeleteRule, int raUpdateRule)
	{
		this(constraintId, indexId);
		this.keyConstraintId = keyConstraintId;
		this.raDeleteRule = raDeleteRule;
		this.raUpdateRule = raUpdateRule;
	}





	/**
	 * Gets the UUID of the backing index.
	 *
	 * @return	The UUID of the backing index.
	 */
	public UUID	getIndexId()
	{
		return indexId;
	}

	/**
	 * Gets the UUID of the referenced key constraint
	 *
	 * @return	The UUID of the referenced key constraint
	 */
	public UUID	getKeyConstraintId()
	{
		return keyConstraintId;
	}

	/**
	 * Does this constraint have a backing index?
	 *
	 * @return boolean	Whether or not there is a backing index for this constraint.
	 */
	public boolean hasBackingIndex()
	{
		return true;
	}

	/**
	 * Gets a referential action rule on a  DELETE
	 * @return referential rule defined by the user during foreign key creattion
	 * for a delete (like CASCDE , RESTRICT ..etc)
	 */
	public int	getRaDeleteRule()
	{
		return raDeleteRule;
	}
	
	
	/**
	 * Gets a referential action rule on a UPDATE
	 * @return referential rule defined by the user during foreign key creattion
	 * for an UPDATE (like CASCDE , RESTRICT ..etc)
	 */
	public int	getRaUpdateRule()
	{
		return raUpdateRule;
	}
	


	/**
	 * Convert the SubKeyConstraintDescriptor to a String.
	 *
	 * @return	A String representation of this SubConstraintDescriptor
	 */

	public String	toString()
	{
		if (SanityManager.DEBUG)
		{
			return "indexId: " + indexId + "\n" +
				"keyConstraintId: " + keyConstraintId + "\n" +
				"raDeleteRule: " + raDeleteRule + "\n" +
				"raUpdateRule: " + raUpdateRule + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

}
