/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.KeyConstraintDescriptor

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.sanity.SanityManager;
/**
 * This interface is used to get information from a KeyConstraintDescriptor.
 * A KeyConstraintDescriptor can represent a primary/unique/foreign key
 * constraint.
 *
 * @version 0.1
 * @author Jerry Brenner
 */

public abstract class KeyConstraintDescriptor extends ConstraintDescriptor
{
	/** interface to this class:
		<ol>
		<li>public UUID getIndexId();</li>
		<li>public ConglomerateDescriptor getIndexConglomerateDescriptor(DataDictionary dd)</li>
		throws StandardException;</li>
		<li>public String getIndexUUIDString();</li>
		<li>public int[]	getKeyColumns();</li>
		</ol>
	*/

	// implementation
	UUID			indexId;

	private	ConglomerateDescriptor	indexConglom;

	/**
	 * Constructor for a KeyConstraintDescriptor
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param table		The descriptor of the table the constraint is on
	 * @param constraintName	The name of the constraint.
	 * @param deferrable		If the constraint can be deferred.
	 * @param initiallyDeferred If the constraint starts life deferred.
	 * @param referencedColumns columns that the constraint references
	 * @param constraintId		UUID of constraint
	 * @param indexId			The UUID for the backing index
	 * @param schemaDesc		The SchemaDescriptor for the constraint
	 * @param isEnabled			is this constraint enabled
	 */
	KeyConstraintDescriptor(
		    DataDictionary dataDictionary,
			TableDescriptor table,
			String constraintName,
			boolean deferrable,
			boolean initiallyDeferred,
			int[] referencedColumns,
			UUID constraintId,
			UUID indexId,
			SchemaDescriptor schemaDesc,
			boolean isEnabled
			)							
	{
		super(dataDictionary, table, constraintName, deferrable,
			  initiallyDeferred, referencedColumns,
			  constraintId, schemaDesc, isEnabled);
		this.indexId = indexId;
	}

	/**
	 * Gets the UUID of the backing index for the constraint.
	 *
	 * @return	The UUID of the backing index for the constraint.
	 */
	public UUID getIndexId()
	{
		return indexId;
	}

	/**
	 * Gets the index conglomerate descriptor
 	 *
	 * @return the index conglomerate descriptor
	 * 
	 * @exception StandardException on error
	 */
	public ConglomerateDescriptor getIndexConglomerateDescriptor(DataDictionary dd)
		throws StandardException
	{
		if (indexConglom == null)
		{
			indexConglom = getTableDescriptor().getConglomerateDescriptor(indexId);	
		}
		return indexConglom;
	}		
	
	/**
	 * Gets the UUID String of the backing index for the constraint.
	 *
	 * @return	The UUID String of the backing index for the constraint.
	 */
	public String getIndexUUIDString()
	{
		return indexId.toString();
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
	 * Get the UUID of the backing index, if one exists.
	 *
	 * @return The UUID of the backing index, if one exists, else null.
	 */
	public UUID getConglomerateId()
	{
		return indexId;
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
			return "indexId: " + indexId + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

}
