/*

   Derby - Class org.apache.derby.impl.sql.compile.ConstraintDefinitionNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.depend.ProviderList;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.util.ReuseFactory;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.reference.SQLState;
import java.util.Properties;

/**
 * A ConstraintDefintionNode is a class for all nodes that can represent
 * constraint definitions.
 *
 * @author Jerry Brenner
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
	int[]			 checkColumnReferences;
	ResultColumnList columnList;
	String			 constraintText;
	ValueNode		 checkCondition;
	private int				 behavior;
    private int verifyType = DataDictionary.DROP_CONSTRAINT; // By default do not check the constraint type

	public void init(
					Object constraintName,
					Object constraintType,
					Object rcl,
					Object properties,
					Object checkCondition,
					Object constraintText,
					Object behavior)
	{
		this.constraintName = (TableName) constraintName;

		/* We need to pass null as name to TableElementNode's constructor 
		 * since constraintName may be null.
		 */
		super.init(null);
		if (this.constraintName != null)
		{
			this.name = this.constraintName.getTableName();
		}
		this.constraintType = ((Integer) constraintType).intValue();
		this.properties = (Properties) properties;
		this.columnList = (ResultColumnList) rcl;
		this.checkCondition = (ValueNode) checkCondition;
		this.constraintText = (String) constraintText;
		this.behavior = ((Integer) behavior).intValue();
	}

	public void init(
					Object constraintName,
					Object constraintType,
					Object rcl,
					Object properties,
					Object checkCondition,
					Object constraintText)
	{
		init(
					constraintName,
					constraintType,
					rcl,
					properties, 
					checkCondition,
					constraintText,
					ReuseFactory.getInteger(StatementType.DROP_DEFAULT)
					);
	}

	public void init(
					Object constraintName,
					Object constraintType,
					Object rcl,
					Object properties,
					Object checkCondition,
					Object constraintText,
					Object behavior,
                    Object verifyType)
	{
        init( constraintName, constraintType, rcl, properties, checkCondition, constraintText, behavior);
        this.verifyType = ((Integer) verifyType).intValue();
    }
    
	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

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
	protected void bind(DDLStatementNode ddlNode, DataDictionary dd)	throws StandardException
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
		To support dropping exisiting constraints that may have mismatched schema names
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
	 * Get the constraint name
	 *
	 * @return constraintName	The constraint name.
	 */
	public TableName getXXRConstraintName()
	{
		return constraintName;
	}

	/**
	 * Set the auxiliary provider list.
	 *
	 * @param apl	The new auxiliary provider list.
	 *
	 * @return Nothing.
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
	public ProviderList getAuxiliaryProviderList()
	{
		return apl;
	}

	/**
	 * Is this a primary key constraint.
	 *
	 * @return boolean	Whether or not this is a primary key constraint
	 */
	boolean hasPrimaryKeyConstraint()
	{
		return constraintType == DataDictionary.PRIMARYKEY_CONSTRAINT;
	}

	/**
	 * Is this a unique key constraint.
	 *
	 * @return boolean	Whether or not this is a unique key constraint
	 */
	boolean hasUniqueKeyConstraint()
	{
		return constraintType == DataDictionary.UNIQUE_CONSTRAINT;
	}

	/**
	 * Is this a foreign key constraint.
	 *
	 * @return boolean	Whether or not this is a unique key constraint
	 */
	boolean hasForeignKeyConstraint()
	{
		return constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT;
	}

	/**
	 * Does this element have a check constraint.
	 *
	 * @return boolean	Whether or not this element has a check constraint
	 */
	boolean hasCheckConstraint()
	{
		return constraintType == DataDictionary.CHECK_CONSTRAINT;
	}

	/**
	 * Does this element have a constraint on it.
	 *
	 * @return boolean	Whether or not this element has a constraint on it
	 */
	boolean hasConstraint()
	{
		return true;
	}

	/**
	 * Is this a foreign key constraint.
	 *
	 * @return boolean	Whether or not this is a unique key constraint
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
	 * Is this a foreign key constraint.
	 *
	 * @return boolean	Whether or not this is a unique key constraint
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
	 *
	 * @return Nothing.
	 */
	public void setProperties(Properties properties)
	{
		this.properties = properties;
	}

	/** 
	 * Get the optional properties for the backing index to this constraint.
	 *
	 *
	 * @return The optional properties for the backing index to this constraint
	 */
	public Properties getProperties()
	{
		return properties;
	}


	/** 
	 * Is this constraint referenced.
	 *
	 * @return true/false
	 */
	public boolean isReferenced()
	{
		return false;
	}

	/** 
	 * Get the count of enabled fks
	 * that reference this constraint
	 *
	 * @return the number
	 */
	public int getReferenceCount()
	{
		return 0;
	}
	/** 
	 * Is this constraint enabled.
	 *
	 * @return true/false
	 */
	public boolean isEnabled()
	{
		return true;
	}

	/**
	 * Get the column list from this node.
	 *
	 * @return ResultColumnList The column list from this table constraint.
	 */
	public ResultColumnList getColumnList()
	{
		return columnList;
	}

	/**
	 * Set the column list for this node.  This is useful for check constraints
	 * where the list of referenced columns is built at bind time.
	 *
	 * @param columnList	The new columnList.
	 *
	 * @return	Nothing.
	 */
	public void setColumnList(ResultColumnList columnList)
	{
		this.columnList = columnList;
	}

	/**
	 * Get the check condition from this table constraint.
	 *
	 * @return The check condition from this node.
	 */
	public ValueNode getCheckCondition()
	{
		return checkCondition;
	}

	/**
	 * Set the check condition for this table constraint.
	 *
	 * @param checkCondition	The check condition
	 *
	 * @return Nothing.
	 */
	public void setCheckCondition(ValueNode checkCondition)
	{
		this.checkCondition = checkCondition;
	}

	/**
	 * Get the text of the constraint. (Only meaningful for check constraints.)
	 *
	 * @return The constraint text.
	 */
	public String getConstraintText()
	{
		return constraintText;
	}

	/**
	 * Get the array of 1-based column references for a check constraint.
	 *
	 * @return	The array of 1-based column references for a check constraint.
	 */
	public int[] getCheckColumnReferences()
	{
		return checkColumnReferences;
	}

	/**
	 * Set the array of 1-based column references for a check constraint.
	 *
	 * @param checkColumnReferences	The array of 1-based column references
	 *								for the check constraint.
	 *
	 * @return Nothing.
	 */
	public void setCheckColumnReferences(int[] checkColumnReferences)
	{
		this.checkColumnReferences = checkColumnReferences;
	}

	/**
	 * Return the behavior of this constriant (DropStatementNode.xxx)	
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
