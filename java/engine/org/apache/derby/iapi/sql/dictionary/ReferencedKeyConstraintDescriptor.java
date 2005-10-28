/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.catalog.UUID;
/**
 * A ReferencedConstraintDeescriptor is a primary key or a unique
 * key that is referenced by a foreign key.
 *
 * @author Jamie
 */
public class ReferencedKeyConstraintDescriptor extends KeyConstraintDescriptor
{
	/**
	   public interface to this descriptor:
	   <ol>
	   <li>public boolean hasSelfReferencingFK(ConstraintDescriptorList cdl, int type) 
		throws StandardException;</li>
		<li>public ConstraintDescriptorList getForeignKeyConstraints(int type) throws StandardException;</li>
		<li>public boolean isReferenced();</li>
		<li>public int getReferenceCount();</li>
		<li>public int incrementReferenceCount();</li>
		<li>public int decrementReferenceCount();</li>
		</ol>
	*/

	//Implementation
	private final int constraintType;

	int			referenceCount;

	// enabled foreign keys
	private	ConstraintDescriptorList fkEnabledConstraintList;
	// all foreign keys
	private	ConstraintDescriptorList fkConstraintList;

	private boolean checkedSelfReferencing;
	private boolean hasSelfReferencing;

	/**
	 * Constructor for a KeyConstraintDescriptorImpl
	 *
	 * @param constraintType	The type of the constraint
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param table		The descriptor of the table the constraint is on
	 * @param constraintName	The name of the constraint.
	 * @param deferrable		If the constraint can be deferred.
	 * @param initiallyDeferred If the constraint starts life deferred.
	 * @param columns			columns involved in the constraint
	 * @param constraintId		UUID of constraint
	 * @param indexId			The UUID for the backing index
	 * @param schemaDesc		The SchemaDescriptor for the constraint
	 * @param isEnabled			is the constraint enabled?
	 * @param referenceCount	number of FKs (enabled only)
	 */
	protected ReferencedKeyConstraintDescriptor(int constraintType,
		    DataDictionary dataDictionary,
			TableDescriptor table,
			String constraintName,
			boolean deferrable,
			boolean initiallyDeferred,
			int[] columns,
			UUID constraintId,
			UUID indexId,
			SchemaDescriptor schemaDesc,
			boolean	isEnabled,
			int referenceCount
			)							
	{
		super(dataDictionary, table, constraintName, deferrable,
			  initiallyDeferred, columns, 
			  constraintId, indexId, schemaDesc, isEnabled);
		this.referenceCount = referenceCount;
		this.constraintType = constraintType;
	}

	public final int getConstraintType() {
		return constraintType;
	}

	/**
	 * Am I referenced by a FK on the same table?
	 *
	 * @param cdl	ConstraintDescriptorList for the table
	 * @param type ConstraintDescriptor.(ENABLED|DISABLED|ALL)
	 *
	 * @return	true/false
	 *
	 * @exception StandardException on error
	 */
	public boolean hasSelfReferencingFK(ConstraintDescriptorList cdl, int type) 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			checkType(type);
		}
	
		if (checkedSelfReferencing)
		{
			return hasSelfReferencing;
		}
	
		ConstraintDescriptor cd;
		ForeignKeyConstraintDescriptor fkcd;
		/* Get a full list of referencing keys, if caller
		 * passed in null CDL.
		 */
		if (cdl == null)
		{
			cdl = getForeignKeyConstraints(type);
		}
		int cdlSize = cdl.size();

		for (int index = 0; index < cdlSize; index++)
		{
			cd = (ConstraintDescriptor) cdl.elementAt(index);
			if (! (cd instanceof ForeignKeyConstraintDescriptor))
			{
				continue;
			}

			fkcd = (ForeignKeyConstraintDescriptor) cd;
			if (fkcd.getReferencedConstraintId().equals(getUUID()))
			{
				hasSelfReferencing = true;
				break;
			}
		}
		return hasSelfReferencing;
	}


	/**
	 * Am I referenced by a FK on another table?
	 * @param type ConstraintDescriptor.(ENABLED|DISABLED|ALL)
	 * @return	true/false
	 * @exception StandardException on error
	 */
	public boolean hasNonSelfReferencingFK(int type) 
		throws StandardException
	{

		boolean hasNonSelfReferenceFk = false;

		if (SanityManager.DEBUG)
		{
			checkType(type);
		}
	
		ConstraintDescriptor cd;
		ForeignKeyConstraintDescriptor fkcd;
		// Get a full list of referencing keys,
		ConstraintDescriptorList cdl = getForeignKeyConstraints(type);
 		int cdlSize = cdl.size();

		for (int index = 0; index < cdlSize; index++)
		{
			cd = (ConstraintDescriptor) cdl.elementAt(index);
			if (! (cd instanceof ForeignKeyConstraintDescriptor))
			{
				continue;
			}

			fkcd = (ForeignKeyConstraintDescriptor) cd;
			if(!(fkcd.getTableId().equals(getTableId())))
			{
				hasNonSelfReferenceFk = true;
				break;
			}
		}
		return hasNonSelfReferenceFk;
	}



	/**
	 * Get the referencing foreign key constraints
	 *
	 * @param type ConstraintDescriptor.(ENABLED|DISABLED|ALL)
	 *
	 * @return	the list of constraints (ConstraintDescriptorListImpl)
	 *
	 * @exception StandardException on error
	 */
	public ConstraintDescriptorList getForeignKeyConstraints(int type)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			checkType(type);
		}

		// optimized for this case
		if (type == ENABLED)
		{
			// optimization to avoid any lookups if we know we
			// aren't referenced.
			if (!isReferenced())
			{
				return new ConstraintDescriptorList();
			}
			else if (fkEnabledConstraintList != null)
			{
				return fkEnabledConstraintList;
			}
			else if (fkConstraintList == null)
			{
				fkConstraintList = getDataDictionary().getForeignKeys(constraintId);
			}
			fkEnabledConstraintList = fkConstraintList.getConstraintDescriptorList(true);
			return fkEnabledConstraintList;
		}

		// not optimized for this case
		else if (type == DISABLED)
		{
			if (fkConstraintList == null)
			{
				fkConstraintList = getDataDictionary().getForeignKeys(constraintId);
			}
			return fkConstraintList.getConstraintDescriptorList(false);
		}
		else
		{
			if (fkConstraintList == null)
			{
				fkConstraintList = getDataDictionary().getForeignKeys(constraintId);
			}
			return fkConstraintList;
		}
	}
		
	/**
	 * Is this constraint referenced? Returns
	 * true if there are enabled fks that 
	 * reference this constraint.
	 *
	 * @return false
	 */
	public boolean isReferenced()
	{
		return referenceCount != 0;
	}

	/**
	 * Get the number of enabled fks that
	 * reference this key.
	 *
	 * @return the number of fks
	 */
	public int getReferenceCount()
	{
		return referenceCount;
	}

	/**
	 * Bump the reference count by one.
	 *
	 * @return the number of fks
	 */
	public int incrementReferenceCount()
	{
		return referenceCount++;
	}

	/**
	 * Decrement the reference count by one.
	 *
	 * @return the number of fks
	 */
	public int decrementReferenceCount()
	{
		return referenceCount--;
	}

	/**
	 * Does this constraint need to fire on this type of
	 * DML?  For referenced keys, fire if referenced by
	 * a fk, and stmt is delete or bulk insert replace, 
	 * or stmt is update and columns intersect.
	 *
	 * @param dmlType	the type of DML 
	 * (StatementType.INSERT|StatementType.UPDATE|StatementType.DELETE)
	 * @param modifiedCols	the columns modified, or null for all
	 *
	 * @return true/false
	 */
	public boolean needsToFire(int stmtType, int[] modifiedCols)
	{
		/*
		** If we are disabled, we never fire
		*/
		if (!isEnabled)
		{
			return false;
		}

		if (!isReferenced() ||
			(stmtType == StatementType.INSERT))
		{
			return false;
		}

		if (stmtType == StatementType.DELETE ||
			stmtType == StatementType.BULK_INSERT_REPLACE)
		{
			return true;
		}

		// if update, only relevant if columns intersect
		return doColumnsIntersect(modifiedCols, getReferencedColumns());
	}

	private void checkType(int type) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			switch (type)
			{
			  case ENABLED:
			  case DISABLED:
			  case ALL:
				break;
			  default:
				SanityManager.THROWASSERT("constraint type "+type+" is invalid");
			}
		}
	}
		
}
