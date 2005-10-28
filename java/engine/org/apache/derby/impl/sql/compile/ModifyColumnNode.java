/*

   Derby - Class org.apache.derby.impl.sql.compile.ModifyColumnNode

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.KeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.UUID;

/**
 * A ModifyColumnNode represents a modify column in an ALTER TABLE statement.
 *
 * @author Jerry Brenner
 */

public class ModifyColumnNode extends ColumnDefinitionNode
{
	int		columnPosition = -1;
	UUID	oldDefaultUUID;

	/**
	 * Get the UUID of the old column default.
	 *
	 * @return The UUID of the old column default.
	 */
	UUID getOldDefaultUUID()
	{
		return oldDefaultUUID;
	}

	/**
	 * Get the column position for the column.
	 *
	 * @return The column position for the column.
	 */
	public int getColumnPosition()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(columnPosition > 0,
				"columnPosition expected to be > 0");
		}
		return columnPosition;
	}

	/**
	 * Check the validity of a user type.  Checks that
	 * 1. the column type is either varchar, ....
	 * 2. is the same type after the alter.
	 * 3. length is greater than the old length.
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void checkUserType(TableDescriptor td)
		throws StandardException
	{
		ColumnDescriptor cd;
		TypeDescriptor oldType;
		TypeDescriptor newType = dataTypeServices;
		TypeId oldTypeId;
		TypeId newTypeId;

		if (getNodeType() != C_NodeTypes.MODIFY_COLUMN_TYPE_NODE)
			return;				// nothing to do if user not changing length

		cd = td.getColumnDescriptor(name);
		if (cd == null)
		{
			throw StandardException.newException(
				SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, name, td.getName());
		}
		
		oldType = cd.getType();
		oldTypeId = cd.getType().getTypeId();
		newTypeId = dataTypeServices.getTypeId();

		// can't change types yet.
		if (!(oldTypeId.equals(newTypeId)))
		{
			throw StandardException.newException(
					 SQLState.LANG_MODIFY_COLUMN_CHANGE_TYPE, name);
		}			
		
		// can only alter the length of varchar, nvarchar, bitvarying columns
		String typeName = dataTypeServices.getTypeName();
		if (!(typeName.equals(TypeId.NATIONAL_VARCHAR_NAME)) &&
			!(typeName.equals(TypeId.VARCHAR_NAME)) &&
			!(typeName.equals(TypeId.VARBIT_NAME)))
		{
			throw StandardException.newException(
						 SQLState.LANG_MODIFY_COLUMN_INVALID_TYPE);
		}
		
		// cannot decrease the length of a column
		if (newType.getMaximumWidth() < oldType.getMaximumWidth())
		{
			throw StandardException.newException(
						 SQLState.LANG_MODIFY_COLUMN_INVALID_LENGTH, name);
		}
	}
	
	/**
	 * If the type of a column is being changed (for mulan, the length of the
	 * column is being increased then make sure that this does not violate
	 * any key constraints; 
	 * the column being altered is 
	 *   1. part of foreign key constraint 
	 *         ==> ERROR. This references a Primary Key constraint and the
	 *             type & lengths of the pkey/fkey must match exactly.
	 *   2. part of a unique/primary key constraint
	 *         ==> OK if no fkey references this constraint.
	 *         ==> ERROR if any fkey in the system references this constraint.
	 *
	 * @param td		The Table Descriptor on which the ALTER is being done.
	 *
	 * @exception StandardException		Thrown on Error.
	 *
	 */
	public void checkExistingConstraints(TableDescriptor td)
	             throws StandardException
	{
		if ((getNodeType() != C_NodeTypes.MODIFY_COLUMN_TYPE_NODE) &&
			(getNodeType() != C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE) &&
			(getNodeType() != C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE))
			return;

		DataDictionary dd = getDataDictionary();
		ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
		int intArray[] = new int[1];
		intArray[0] = columnPosition;

		for (int index = 0; index < cdl.size(); index++)
		{
			ConstraintDescriptor existingConstraint =
				                                cdl.elementAt(index);
			if (!(existingConstraint instanceof KeyConstraintDescriptor))
				continue;

			if (!existingConstraint.columnIntersects(intArray))
				continue;
															 
			int constraintType = existingConstraint.getConstraintType();

			// cannot change the length of a column that is part of a 
			// foreign key constraint. Must be an exact match between pkey
			// and fkey columns.
			if ((constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT) 
				&&
				(getNodeType() == C_NodeTypes.MODIFY_COLUMN_TYPE_NODE))
			{
				throw StandardException.newException(
					 SQLState.LANG_MODIFY_COLUMN_FKEY_CONSTRAINT, name, existingConstraint.getConstraintName());
			}	
			
			else
			{
				// a column that is part of a primary key is being made
				// nullable; can't be done.
				if ((getNodeType() == 
					 C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE) &&
					(existingConstraint.getConstraintType() == 
					 DataDictionary.PRIMARYKEY_CONSTRAINT))
				{
				throw StandardException.newException(
					 SQLState.LANG_MODIFY_COLUMN_PKEY_CONSTRAINT, name);
				}
				// unique key or primary key.
				ConstraintDescriptorList 
					refcdl = dd.getForeignKeys(existingConstraint.getUUID());
				 
				if (refcdl.size() > 0)
				{
					throw StandardException.newException(
						 SQLState.LANG_MODIFY_COLUMN_REFERENCED, name, refcdl.elementAt(0).getConstraintName());
				}
				
				// Make the statement dependent on the primary key constraint.
				getCompilerContext().createDependency(existingConstraint);
			}
		}

	}
	/**
	 * Get the action associated with this node.
	 *
	 * @return The action associated with this node.
	 */
	int getAction()
	{
		switch (getNodeType())
		{
		case C_NodeTypes.MODIFY_COLUMN_DEFAULT_NODE:
			return ColumnInfo.MODIFY_COLUMN_DEFAULT;
		case C_NodeTypes.MODIFY_COLUMN_TYPE_NODE:
			return ColumnInfo.MODIFY_COLUMN_TYPE;
		case C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE:
			return ColumnInfo.MODIFY_COLUMN_CONSTRAINT;
		case C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE:
			return ColumnInfo.MODIFY_COLUMN_CONSTRAINT_NOT_NULL;
		default:
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Unexpected nodeType = " + 
										  getNodeType());
			}
			return 0;
		}
	}

	/**
	 * Check the validity of the default, if any, for this node.
	 *
	 * @param dd		The DataDictionary.
	 * @param td		The TableDescriptor.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void bindAndValidateDefault(DataDictionary dd, TableDescriptor td) 
		throws StandardException
	{
		ColumnDescriptor cd;

		// First verify that the column exists
		cd = td.getColumnDescriptor(name);
		if (cd == null)
		{
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, name, td.getName());
		}


		// Get the UUID for the old default
		DefaultDescriptor defaultDescriptor = cd.getDefaultDescriptor(dd);
		
		oldDefaultUUID = (defaultDescriptor == null) ? null : defaultDescriptor.getUUID();

		// Remember the column position
		columnPosition = cd.getPosition();

		// No other work to do if no user specified default
		if (getNodeType() != C_NodeTypes.MODIFY_COLUMN_DEFAULT_NODE)
		{
			return;
		}

		/* Fill in the DataTypeServices from the DataDictionary */
		dataTypeServices = cd.getType();

		// Now validate the default
		validateDefault(dd, td);
	}
	
	private ColumnDescriptor getLocalColumnDescriptor(String name, TableDescriptor td)
	         throws StandardException
	{
		ColumnDescriptor cd;

		// First verify that the column exists
		cd = td.getColumnDescriptor(name);
		if (cd == null)
		{
			throw StandardException.newException(
				SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, name, td.getName());
		}

		return cd;
	}
	/**
	 * check the validity of autoincrement values in the case that we are 
	 * modifying an existing column (includes checking if autoincrement is set
	 * when making a column nullable)
	 */
	public void validateAutoincrement(DataDictionary dd, TableDescriptor td, int tableType)
	         throws StandardException
	{
		ColumnDescriptor cd;

		// a column that has an autoincrement default can't be made nullable
		if (getNodeType() == C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE)
		{
			cd = getLocalColumnDescriptor(name, td);
			if (cd.isAutoincrement())
			{
				throw StandardException.newException(SQLState.LANG_AI_CANNOT_NULL_AI,
						getColumnName());
			}
		}

		if (autoincrementVerify)
		{
			cd = getLocalColumnDescriptor(name, td);
			if (!cd.isAutoincrement())
				throw StandardException.newException(SQLState.LANG_INVALID_ALTER_TABLE_ATTRIBUTES,
								td.getQualifiedName(), name);
		}
		if (isAutoincrement == false)
			return;
		
		super.validateAutoincrement(dd, td, tableType);
		if (dataTypeServices.isNullable())
			throw StandardException.newException(SQLState.LANG_AI_CANNOT_ADD_AI_TO_NULLABLE,
												getColumnName());
	}
}
