/*

   Derby - Class org.apache.derby.impl.sql.compile.ModifyColumnNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.DefaultInfoImpl;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.KeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.impl.sql.execute.ColumnInfo;

/**
 * A ModifyColumnNode represents a modify column in an ALTER TABLE statement.
 *
 */

class ModifyColumnNode extends ColumnDefinitionNode
{
	int		columnPosition = -1;
	UUID	oldDefaultUUID;

    // Allowed kinds
    final static int K_MODIFY_COLUMN_TYPE = 0;
    final static int K_MODIFY_COLUMN_DEFAULT = 1;
    final static int K_MODIFY_COLUMN_CONSTRAINT = 2;
    final static int K_MODIFY_COLUMN_CONSTRAINT_NOT_NULL = 3;
    final static int K_DROP_COLUMN = 4;
    final static int K_MODIFY_COLUMN_GENERATED_ALWAYS = 5;
    final static int K_MODIFY_COLUMN_GENERATED_BY_DEFAULT = 6;

    /**
     * This class is used to hold logically different objects for
     * space efficiency. {@code kind} represents the logical object
     * type. See also {@link ValueNode#isSameNodeKind}.
     */
    final int kind;

    ModifyColumnNode(int kind,
            String name,
            ValueNode defaultNode,
            DataTypeDescriptor dataTypeServices,
            long[] autoIncrementInfo,
            ContextManager cm) throws StandardException {
    		super(name, defaultNode, dataTypeServices, autoIncrementInfo, cm);
        	this.kind = kind;
	}
	/**
	 * Get the UUID of the old column default.
	 *
	 * @return The UUID of the old column default.
	 */
    @Override
	UUID getOldDefaultUUID()
	{
		return oldDefaultUUID;
	}

	/**
	 * Get the column position for the column.
	 *
	 * @return The column position for the column.
	 */
    int getColumnPosition()
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
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void checkUserType(TableDescriptor td)
		throws StandardException
	{
        if (kind != K_MODIFY_COLUMN_TYPE) {
            return; // nothing to do if user not changing length
        }

        ColumnDescriptor cd = td.getColumnDescriptor(name);
		if (cd == null)
		{
			throw StandardException.newException(
				SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, name, td.getName());
		}
		
		DataTypeDescriptor oldType = cd.getType();
        setNullability(oldType.isNullable());

		// can't change types yet.
		if (!(oldType.getTypeId().equals(getType().getTypeId())))
		{
			throw StandardException.newException(
					 SQLState.LANG_MODIFY_COLUMN_CHANGE_TYPE, name);
		}			
		
		// can only alter the length of varchar, bitvarying columns
		String typeName = getType().getTypeName();
		if (!(typeName.equals(TypeId.VARCHAR_NAME)) &&
			!(typeName.equals(TypeId.VARBIT_NAME))&&
			!(typeName.equals(TypeId.BLOB_NAME))&&
			!(typeName.equals(TypeId.CLOB_NAME)))
		{
			throw StandardException.newException(
						 SQLState.LANG_MODIFY_COLUMN_INVALID_TYPE);
		}
		
		// cannot decrease the length of a column
		if (getType().getMaximumWidth() < oldType.getMaximumWidth())
		{
			throw StandardException.newException(
						 SQLState.LANG_MODIFY_COLUMN_INVALID_LENGTH, name);
		}
	}
	
	/**
     * Check if the the column can be modified, and throw error if not.
     *
	 * If the type of a column is being changed (for instance if the length 
     * of the column is being increased) then make sure that this does not 
     * violate any key constraints; 
	 * the column being altered is 
	 *   1. part of foreign key constraint 
	 *         ==&gt; ERROR. This references a Primary Key constraint and the
	 *             type and lengths of the pkey/fkey must match exactly.
	 *   2. part of a unique/primary key constraint
	 *         ==&gt; OK if no fkey references this constraint.
	 *         ==&gt; ERROR if any fkey in the system references this constraint.
	 *
	 * @param td		The Table Descriptor on which the ALTER is being done.
	 *
	 * @exception StandardException		Thrown on Error.
	 *
	 */
    void checkExistingConstraints(TableDescriptor td)
	             throws StandardException
	{
        if ((kind != K_MODIFY_COLUMN_TYPE) &&
            (kind != K_MODIFY_COLUMN_CONSTRAINT) &&
            (kind != K_MODIFY_COLUMN_CONSTRAINT_NOT_NULL))
			return;

		DataDictionary           dd          = getDataDictionary();
		ConstraintDescriptorList cdl         = dd.getConstraintDescriptors(td);
		int                      intArray[]  = new int[1];
		intArray[0]                          = columnPosition;

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
                (kind == K_MODIFY_COLUMN_TYPE))
			{
				throw StandardException.newException(
					 SQLState.LANG_MODIFY_COLUMN_FKEY_CONSTRAINT, 
                     name, existingConstraint.getConstraintName());
			}	
			else
			{
				if (!dd.checkVersion(
					DataDictionary.DD_VERSION_DERBY_10_4, null)) 
				{
                    // If a column is part of unique constraint it can't be
                    // made nullable in soft upgrade mode from a pre-10.4 db.
                    if (kind == K_MODIFY_COLUMN_CONSTRAINT &&
						(existingConstraint.getConstraintType() == 
							DataDictionary.UNIQUE_CONSTRAINT)) 
					{
						throw StandardException.newException(
							SQLState.LANG_MODIFY_COLUMN_EXISTING_CONSTRAINT,
							name);
					}
				}

                // A column that is part of a primary key
                // is being made nullable; can't be done.
                if ((kind == K_MODIFY_COLUMN_CONSTRAINT) &&
					((existingConstraint.getConstraintType() == 
					 DataDictionary.PRIMARYKEY_CONSTRAINT)))
				{
					String errorState = 
						(getLanguageConnectionContext().getDataDictionary()
								.checkVersion(DataDictionary.DD_VERSION_DERBY_10_4, 
								null))
						? SQLState.LANG_MODIFY_COLUMN_EXISTING_PRIMARY_KEY
						: SQLState.LANG_MODIFY_COLUMN_EXISTING_CONSTRAINT;
					throw StandardException.newException(errorState, name);
				}
				// unique key or primary key.
				ConstraintDescriptorList 
					refcdl = dd.getForeignKeys(existingConstraint.getUUID());
				 
				if (refcdl.size() > 0)
				{
					throw StandardException.newException(
						 SQLState.LANG_MODIFY_COLUMN_REFERENCED, 
                         name, refcdl.elementAt(0).getConstraintName());
				}
				
				// Make the statement dependent on the primary key constraint.
				getCompilerContext().createDependency(existingConstraint);
			}
		}
    }

	/**
	 * If the column being modified is of character string type, then it should
	 * get its collation from the corresponding column in the TableDescriptor.
	 * This will ensure that at alter table time, the existing character string
	 * type columns do not loose their collation type. If the alter table is 
	 * doing a drop column, then we do not need to worry about collation info.
	 * 
	 * @param td Table Descriptor that holds the column which is being altered
	 * @throws StandardException
	 */
    void useExistingCollation(TableDescriptor td)
    throws StandardException
    {
		ColumnDescriptor cd;

		// First verify that the column exists
		cd = td.getColumnDescriptor(name);
		if (cd == null)
		{
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, name, td.getName());
		}
		//getType() == null means we are dealing with drop column and hence 
		//no need to worry about collation info
		if (getType() != null) {
			if (getType().getTypeId().isStringTypeId()) {
				setCollationType(cd.getType().getCollationType());			
			}
		}
    }

	/**
	 * Get the action associated with this node.
	 *
	 * @return The action associated with this node.
	 */
    @Override
	int getAction()
	{
        switch (kind) {
            case K_MODIFY_COLUMN_DEFAULT:
                if (autoinc_create_or_modify_Start_Increment ==
                        ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE) {
                    return ColumnInfo.MODIFY_COLUMN_DEFAULT_RESTART;

                } else if (autoinc_create_or_modify_Start_Increment ==
                        ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE) {
                    return ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT;

                } else if (autoinc_create_or_modify_Start_Increment ==
                        ColumnDefinitionNode.MODIFY_AUTOINCREMENT_CYCLE_VALUE) {
                    return ColumnInfo.MODIFY_COLUMN_DEFAULT_CYCLE;
                } else {
                    return ColumnInfo.MODIFY_COLUMN_DEFAULT_VALUE;
                }
            case K_MODIFY_COLUMN_TYPE:
                return ColumnInfo.MODIFY_COLUMN_TYPE;

            case K_MODIFY_COLUMN_CONSTRAINT:
                return ColumnInfo.MODIFY_COLUMN_CONSTRAINT;

            case K_MODIFY_COLUMN_CONSTRAINT_NOT_NULL:
                return ColumnInfo.MODIFY_COLUMN_CONSTRAINT_NOT_NULL;

            case K_DROP_COLUMN:
                return ColumnInfo.DROP;

            case K_MODIFY_COLUMN_GENERATED_ALWAYS:
                return ColumnInfo.MODIFY_COLUMN_GENERATED_ALWAYS;

            case K_MODIFY_COLUMN_GENERATED_BY_DEFAULT:
                return ColumnInfo.MODIFY_COLUMN_GENERATED_BY_DEFAULT;

            default:
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT("Unexpected type = " + kind);
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
	 * @exception StandardException		Thrown on error
	 */
    @Override
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
        if (kind != K_MODIFY_COLUMN_DEFAULT) {
			return;
		}

		// If the statement is not setting the column's default, then
		// recover the old default and re-use it. If the statement is
		// changing the start value for the auto-increment, then recover
		// the old increment-by value and re-use it. If the statement is
		// changing the increment-by value, then recover the old start value
		// and re-use it. This way, the column alteration only changes the
		// aspects of the autoincrement settings that it intends to change,
		// and does not lose the other aspecs.
		if (keepCurrentDefault)
        { defaultInfo = (DefaultInfoImpl)cd.getDefaultInfo(); }
        else
        {
            if ( cd.hasGenerationClause() || cd.isAutoincrement() )
            {
				throw StandardException.newException( SQLState.LANG_GEN_COL_DEFAULT, cd.getColumnName() );
            }
        }
		if (autoinc_create_or_modify_Start_Increment ==
				ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE){
			autoincrementIncrement = cd.getAutoincInc();
			autoincrementCycle = cd.getAutoincCycle();
			}
		if (autoinc_create_or_modify_Start_Increment ==
				ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE){
			autoincrementStart = cd.getAutoincStart();
			autoincrementCycle = cd.getAutoincCycle();
		}
		if (autoinc_create_or_modify_Start_Increment ==
				ColumnDefinitionNode.MODIFY_AUTOINCREMENT_CYCLE_VALUE){
			autoincrementIncrement = cd.getAutoincInc();
			autoincrementStart = cd.getAutoincStart();
		}			

		/* Fill in the DataTypeServices from the DataDictionary */
		type = cd.getType();

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
    @Override
    void validateAutoincrement(DataDictionary dd,
                               TableDescriptor td,
                               int tableType) throws StandardException
	{
		ColumnDescriptor cd;

		// only autoincrement columns can have their generation property changed
        if (
            (kind == K_MODIFY_COLUMN_GENERATED_ALWAYS) ||
            (kind == K_MODIFY_COLUMN_GENERATED_BY_DEFAULT)
            )
		{
			cd = getLocalColumnDescriptor(name, td);
			if (!cd.isAutoincrement())
			{
				throw StandardException.newException(SQLState.LANG_AI_CANNOT_ALTER_IDENTITYNESS,
						getColumnName());
			}

            if (kind == K_MODIFY_COLUMN_GENERATED_BY_DEFAULT)
            {
                defaultInfo = createDefaultInfoOfAutoInc();
            }

            // nothing more to do here
            return;
		}

		// a column that has an autoincrement default can't be made nullable
        if (kind == K_MODIFY_COLUMN_CONSTRAINT)
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
		if (getType().isNullable())
			throw StandardException.newException(SQLState.LANG_AI_CANNOT_ADD_AI_TO_NULLABLE,
												getColumnName());
	}
}
