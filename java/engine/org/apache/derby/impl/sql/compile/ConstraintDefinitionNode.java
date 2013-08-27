/*

   Derby - Class org.apache.derby.impl.sql.compile.ConstraintDefinitionNode

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

import java.util.Properties;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.depend.ProviderList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * A ConstraintDefintionNode is a class for all nodes that can represent
 * constraint definitions.
 *
 */

public class ConstraintDefinitionNode extends TableElementNode
{
	
	private TableName constraintName;
	protected int constraintType;
	protected Properties properties;
	ProviderList apl;

	UUIDFactory		uuidFactory;

	String			backingIndexName;
	UUID			backingIndexUUID;
	ResultColumnList columnList;
	String			 constraintText;
	ValueNode		 checkCondition;
	private int				 behavior;
    private int verifyType;

    public final static boolean DEFERRABLE_DEFAULT = false;
    public final static boolean INITIALLY_DEFERRED_DEFAULT = false;
    public final static boolean ENFORCED_DEFAULT = true;

    /**
     * boolean[3]: {deferrable?, initiallyDeferred?, enforced?}
     */
    private boolean[] characteristics;

    ConstraintDefinitionNode(
                    TableName constraintName,
                    int constraintType,
                    ResultColumnList rcl,
                    Properties properties,
                    ValueNode checkCondition,
                    String constraintText,
                    int behavior,
                    int verifyType,
                    ContextManager cm)
	{
		/* We need to pass null as name to TableElementNode's constructor 
		 * since constraintName may be null.
		 */
        super(null, cm);

        this.constraintName = constraintName;

		if (this.constraintName != null)
		{
			this.name = this.constraintName.getTableName();
		}
        this.constraintType = constraintType;
        this.properties = properties;
        this.columnList = rcl;
        this.checkCondition = checkCondition;
        this.constraintText = constraintText;
        this.behavior = behavior;
        this.verifyType = verifyType;
	}


    void setCharacteristics(boolean[] cc) {
        characteristics = cc.clone();
    }

    boolean[] getCharacteristics() {
        if (characteristics == null) {
            characteristics = new boolean[]{
                ConstraintDefinitionNode.DEFERRABLE_DEFAULT,
                ConstraintDefinitionNode.INITIALLY_DEFERRED_DEFAULT,
                ConstraintDefinitionNode.ENFORCED_DEFAULT
            };
        }

        return characteristics.clone();
    }

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "constraintName: " + 
				( ( constraintName != null) ?
						constraintName.toString() : "null" ) + "\n" +
				"constraintType: " + constraintType + "\n" + 
				"properties: " +
				((properties != null) ? properties.toString() : "null") + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Bind this constraint definition. 
	 *
	   @param ddlNode the create or alter table node
	 * @param dd the dd
	 *
	 * @exception StandardException on error
	 */
    void bind(DDLStatementNode ddlNode, DataDictionary dd) throws StandardException
	{
		// we need to allow drops on constraints with different schemas
		// to support removing constraints created pre 5.2.
		if (constraintType == DataDictionary.DROP_CONSTRAINT)
			return;

		// ensure the schema of the constraint matches the schema of the table
		if (constraintName != null) {

			String constraintSchema = constraintName.getSchemaName();


			if (constraintSchema != null) {



				TableName tableName = ddlNode.getObjectName();
				String tableSchema = tableName.getSchemaName();
				if (tableSchema == null) {
					tableSchema = getSchemaDescriptor((String) null).getSchemaName();
					tableName.setSchemaName(tableSchema);
				}
				if (!constraintSchema.equals(tableSchema)) {
					throw StandardException.newException(SQLState.LANG_CONSTRAINT_SCHEMA_MISMATCH,
												constraintName, tableName);

				}
			}
		}
		else {
			name = getBackingIndexName(dd);
		}
	}

	/**
	  *	Get the name of the constraint. If the user didn't provide one, we make one up. This allows Replication
	  *	to agree with the core compiler on the names of constraints.
	  *
	  *	@return	constraint name
	  */
	String	getConstraintMoniker()
	{
		return name;
	}

	/**
        To support dropping existing constraints that may have mismatched schema names
		we need to support ALTER TABLE S1.T DROP CONSTRAINT S2.C.
		If a constraint name was specified this returns it, otherwise it returns null.
	*/
	String getDropSchemaName() {
		if (constraintName != null)
			return constraintName.getSchemaName();
		return null;
	}

	/**
	  *	Allocates a UUID if one doesn't already exist for the index backing this constraint. This allows Replication
	  *	logic to agree with the core compiler on what the UUIDs of indices are.
	  *
	  *	@return	a UUID for the constraint. allocates one if this is the first time this method is called.
	  */
	UUID	getBackingIndexUUID()
	{
		if ( backingIndexUUID == null )
		{
			backingIndexUUID = getUUIDFactory().createUUID();
		}

		return	backingIndexUUID;
	}

	/**
	  *	Gets a unique name for the backing index for this constraint of the form SQLyymmddhhmmssxxn
	  *	  yy - year, mm - month, dd - day of month, hh - hour, mm - minute, ss - second,
	  *	  xx - the first 2 digits of millisec because we don't have enough space to keep the exact millisec value,
	  *	  n - number between 0-9
	  *
	  *	@return	name of backing index
	  */
	String	getBackingIndexName(DataDictionary dd)
	{
		if ( backingIndexName == null )
			backingIndexName = dd.getSystemSQLName();

		return	backingIndexName;
	}

	/**
	 * Set the auxiliary provider list.
	 *
	 * @param apl	The new auxiliary provider list.
	 */
	void setAuxiliaryProviderList(ProviderList apl)
	{
		this.apl = apl;
	}

	/**
	 * Return the auxiliary provider list.
	 *
	 * @return	The auxiliary provider list.
	 */
    ProviderList getAuxiliaryProviderList()
	{
		return apl;
	}

	/**
	 * Is this a primary key constraint.
	 *
	 * @return boolean	Whether or not this is a primary key constraint
	 */
    @Override
	boolean hasPrimaryKeyConstraint()
	{
		return constraintType == DataDictionary.PRIMARYKEY_CONSTRAINT;
	}

	/**
	 * Is this a unique key constraint.
	 *
	 * @return boolean	Whether or not this is a unique key constraint
	 */
    @Override
	boolean hasUniqueKeyConstraint()
	{
		return constraintType == DataDictionary.UNIQUE_CONSTRAINT;
	}

	/**
	 * Is this a foreign key constraint.
	 *
	 * @return boolean	Whether or not this is a unique key constraint
	 */
    @Override
	boolean hasForeignKeyConstraint()
	{
		return constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT;
	}

	/**
	 * Does this element have a check constraint.
	 *
	 * @return boolean	Whether or not this element has a check constraint
	 */
    @Override
	boolean hasCheckConstraint()
	{
		return constraintType == DataDictionary.CHECK_CONSTRAINT;
	}

	/**
	 * Does this element have a constraint on it.
	 *
	 * @return boolean	Whether or not this element has a constraint on it
	 */
    @Override
	boolean hasConstraint()
	{
		return true;
	}

	/**
     * Does this constraint require a backing index for its implementation?
	 *
     * @return boolean  {@code true} if this constraint requires a backing
     *                  index, i.e. if is a foreign key, primary key or
     *                  unique key constraint
	 */
	boolean requiresBackingIndex()
	{
		switch (constraintType)
		{
			case DataDictionary.FOREIGNKEY_CONSTRAINT:
			case DataDictionary.PRIMARYKEY_CONSTRAINT:
			case DataDictionary.UNIQUE_CONSTRAINT:
				return true;
			default:
				return false;
		}
	}	

	/**
     * Is this a primary key or unique constraint?
	 *
     * @return boolean  {@code true} if this is a primary key or
     *                  unique key constraint
	 */
	boolean requiresUniqueIndex()
	{
		switch (constraintType)
		{
			case DataDictionary.PRIMARYKEY_CONSTRAINT:
			case DataDictionary.UNIQUE_CONSTRAINT:
				return true;
			default:
				return false;
		}
	}

	/**
	 * Get the constraint type
	 *
	 * @return constraintType	The constraint type.
	 */
	int getConstraintType()
	{
		return constraintType;
	}

	/**
	 * Set the optional properties for the backing index to this constraint.
	 *
	 * @param properties	The optional Properties for this constraint.
	 */
    void setProperties(Properties properties)
	{
		this.properties = properties;
	}

	/** 
	 * Get the optional properties for the backing index to this constraint.
	 *
	 *
	 * @return The optional properties for the backing index to this constraint
	 */
    Properties getProperties()
	{
		return properties;
	}


	/** 
	 * Is this constraint referenced.
	 *
	 * @return true/false
	 */
    boolean isReferenced()
	{
		return false;
	}

	/** 
	 * Get the count of enabled fks
	 * that reference this constraint
	 *
	 * @return the number
	 */
    int getReferenceCount()
	{
		return 0;
	}
	/** 
	 * Is this constraint enabled.
	 *
	 * @return true/false
	 */
    boolean isEnabled()
	{
		return true;
	}

	/**
	 * Get the column list from this node.
	 *
	 * @return ResultColumnList The column list from this table constraint.
	 */
    ResultColumnList getColumnList()
	{
		return columnList;
	}

	/**
	 * Set the column list for this node.  This is useful for check constraints
	 * where the list of referenced columns is built at bind time.
	 *
	 * @param columnList	The new columnList.
	 */
    void setColumnList(ResultColumnList columnList)
	{
		this.columnList = columnList;
	}

	/**
	 * Get the check condition from this table constraint.
	 *
	 * @return The check condition from this node.
	 */
    ValueNode getCheckCondition()
	{
		return checkCondition;
	}

	/**
	 * Set the check condition for this table constraint.
	 *
	 * @param checkCondition	The check condition
	 */
    void setCheckCondition(ValueNode checkCondition)
	{
		this.checkCondition = checkCondition;
	}

	/**
	 * Get the text of the constraint. (Only meaningful for check constraints.)
	 *
	 * @return The constraint text.
	 */
    String getConstraintText()
	{
		return constraintText;
	}

	/**
     * Return the behavior of this constraint.
     * See {@link org.apache.derby.iapi.sql.StatementType#DROP_CASCADE} etc.
	 *
	 * @return the behavior
	 */
	int getDropBehavior()
	{
		return behavior;
	}

    /**
     * @return the expected type of the constraint, DataDictionary.DROP_CONSTRAINT if the constraint is
     *         to be dropped without checking its type.
     */
    int getVerifyType()
    {
        return verifyType;
    }

	///////////////////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	///////////////////////////////////////////////////////////////////////////
	/**
	  *	Get the UUID factory
	  *
	  *	@return	the UUID factory
	  *
	  */
	private	UUIDFactory	getUUIDFactory()
	{
		if ( uuidFactory == null )
		{
			uuidFactory = Monitor.getMonitor().getUUIDFactory();
		}
		return	uuidFactory;
	}
}
