/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * A foreign key.
 *
 */
public class ForeignKeyConstraintDescriptor extends KeyConstraintDescriptor
{
	/**
	   interface to this descriptor
	   <ol>
	   <li>public ReferencedKeyConstraintDescriptor getReferencedConstraint()
	   throws StandardException;
	   <li>public UUID getReferencedConstraintId()  
	   throws StandardException;
	   <li>public boolean isSelfReferencingFK()
	   throws StandardException;
	   </ol>
	*/

	// Implementation
	ReferencedKeyConstraintDescriptor	referencedConstraintDescriptor;
	UUID								referencedConstraintId;
	int                                 raDeleteRule;
	int                                 raUpdateRule;
	/**
	 * Constructor for a ForeignKeyConstraintDescriptor
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param table		The descriptor of the table the constraint is on
	 * @param constraintName	The name of the constraint.
	 * @param deferrable		If the constraint can be deferred.
	 * @param initiallyDeferred If the constraint starts life deferred.
	 * @param fkColumns 			columns in the foreign key
	 * @param constraintId		UUID of constraint
	 * @param indexId			The UUID for the backing index
	 * @param schemaDesc		The SchemaDescriptor for the constraint
	 * @param referencedConstraintDescriptor	is referenced constraint descriptor
     * @param enforced          is the constraint enforced?
     * @param raDeleteRule      The {@code ON DELETE} action rule
     * @param raUpdateRule      The {@code ON UPDATE} action rule
	 */
	protected ForeignKeyConstraintDescriptor(
		    DataDictionary dataDictionary,
			TableDescriptor table,
			String constraintName,
			boolean deferrable,
			boolean initiallyDeferred,
			int[] fkColumns,
			UUID constraintId,
			UUID indexId,
			SchemaDescriptor schemaDesc,
			ReferencedKeyConstraintDescriptor referencedConstraintDescriptor,
            boolean enforced,
			int raDeleteRule,
			int raUpdateRule
			)							
	{
		super(dataDictionary, table, constraintName, deferrable,
			  initiallyDeferred, fkColumns,
              constraintId, indexId, schemaDesc, enforced);
//IC see: https://issues.apache.org/jira/browse/DERBY-532

		this.referencedConstraintDescriptor = referencedConstraintDescriptor;
		this.raDeleteRule = raDeleteRule;
		this.raUpdateRule = raUpdateRule;
	}

	/**
	 * Constructor for a ForeignKeyConstraintDescriptor
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param table		The descriptor of the table the constraint is on
	 * @param constraintName	The name of the constraint.
	 * @param deferrable		If the constraint can be deferred.
	 * @param initiallyDeferred If the constraint starts life deferred.
	 * @param fkColumns 			columns in the foreign key
	 * @param constraintId		UUID of constraint
	 * @param indexId			The UUID for the backing index
	 * @param schemaDesc		The SchemaDescriptor for the constraint
	 * @param referencedConstraintId	is referenced constraint id
     * @param enforced          {@code true} if this constraint is enforced
     * @param raDeleteRule      The {@code ON DELETE} action rule 
     * @param raUpdateRule      The {@code ON UPDATE} action rule 
	 */
	ForeignKeyConstraintDescriptor(
		    DataDictionary dataDictionary,
			TableDescriptor table,
			String constraintName,
			boolean deferrable,
			boolean initiallyDeferred,
			int[] fkColumns,
			UUID constraintId,
			UUID indexId,
			SchemaDescriptor schemaDesc,
			UUID referencedConstraintId,
            boolean enforced,
			int raDeleteRule,
			int raUpdateRule
			)							
	{
		super(dataDictionary, table, constraintName, deferrable,
			  initiallyDeferred, fkColumns,
//IC see: https://issues.apache.org/jira/browse/DERBY-532
              constraintId, indexId, schemaDesc, enforced);
		this.referencedConstraintId = referencedConstraintId;
		this.raDeleteRule = raDeleteRule;
		this.raUpdateRule = raUpdateRule;

	}

	/**
	 * Get the constraint that this FK references.  Will
	 * return either a primary key or a unique key constraint.
	 *
	 * @return	the constraint
	 *
	 * @exception StandardException on error
	 */
	public ReferencedKeyConstraintDescriptor getReferencedConstraint() 
		throws StandardException
	{
		if (referencedConstraintDescriptor != null)
		{
			return referencedConstraintDescriptor;
		}

		if (referencedConstraintId == null)
		{
			getReferencedConstraintId();
		}

		TableDescriptor refTd = getDataDictionary().getConstraintTableDescriptor(referencedConstraintId);

		if (SanityManager.DEBUG)
		{
			if (refTd == null)
			{
				SanityManager.THROWASSERT("not able to find "+referencedConstraintId+
							" in SYS.SYSCONSTRAINTS");
			}
		}

		ConstraintDescriptorList cdl = getDataDictionary().getConstraintDescriptors(refTd);
		referencedConstraintDescriptor = (ReferencedKeyConstraintDescriptor)
									cdl.getConstraintDescriptorById(referencedConstraintId);

		if (SanityManager.DEBUG)
		{
			if (referencedConstraintDescriptor == null)
			{
				SanityManager.THROWASSERT("not able to find "
					+referencedConstraintDescriptor+ " off of table descriptor "
					+refTd.getName());
			}
		}

		return referencedConstraintDescriptor;
	}

	
	/**
	 * Get the constraint id for the constraint that this FK references.  
	 * Will return either a primary key or a unique key constriant.
	 *
	 * @return	the constraint id
	 *
	 * @exception StandardException on error
	 */
	public UUID getReferencedConstraintId()  throws StandardException
	{
		if (referencedConstraintDescriptor != null)
		{
			return referencedConstraintDescriptor.getUUID();
		}

		SubKeyConstraintDescriptor subKey;
		subKey = getDataDictionary().getSubKeyConstraint(constraintId,
										DataDictionary.FOREIGNKEY_CONSTRAINT);
		if (SanityManager.DEBUG)
		{
			if (subKey == null)
			{
				SanityManager.THROWASSERT("not able to find "+constraintName+
							" in SYS.SYSFOREIGNKEYS");
			}
		}
		referencedConstraintId = subKey.getKeyConstraintId();
		return referencedConstraintId;
	}

	/**
	 * Gets an identifier telling what type of descriptor it is
	 * (UNIQUE, PRIMARY KEY, FOREIGN KEY, CHECK).
	 *
	 * @return	An identifier telling what type of descriptor it is
	 *		(UNIQUE, PRIMARY KEY, FOREIGN KEY, CHECK).
	 */
    @Override
	public int	getConstraintType()
	{
		return DataDictionary.FOREIGNKEY_CONSTRAINT;
	}

	/**
	 * Does this constraint need to fire on this type of
	 * DML?  True if insert or update and columns intersect
	 *
	 * @param stmtType	the type of DML 
	 * (StatementType.INSERT|StatementType.UPDATE|StatementType.DELETE)
	 * @param modifiedCols	the columns modified, or null for all
	 *
	 * @return true/false
	 */
    @Override
	public boolean needsToFire(int stmtType, int[] modifiedCols)
	{
		/*
		** If we are disabled, we never fire
		*/
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        if (!enforced())
		{
			return false;
		}

		if (stmtType == StatementType.DELETE)
		{
			return false;
		}
		if (stmtType == StatementType.INSERT)
		{
			return true;
		}

		// if update, only relevant if columns intersect
		return doColumnsIntersect(modifiedCols, getReferencedColumns());
	}

	/**
	 * Am I a self-referencing FK?  True if my referenced
	 * constraint is on the same table as me.
	 *
	 * @return	true/false
	 *
	 * @exception StandardException on error
	 */
	public boolean isSelfReferencingFK()
		throws StandardException
	{
		ReferencedKeyConstraintDescriptor refcd = getReferencedConstraint();
		return (refcd.getTableId().equals(getTableId()));
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
	
}







