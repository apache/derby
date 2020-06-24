/*

   Derby - Class org.apache.derby.impl.sql.compile.ColumnDefinitionNode

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

import java.sql.Types;
import java.util.List;
import org.apache.derby.catalog.DefaultInfo;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.DefaultInfoImpl;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Limits;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.depend.ProviderList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.impl.sql.execute.ColumnInfo;

/**
 * A ColumnDefinitionNode represents a column definition in a DDL statement.
 * There will be a ColumnDefinitionNode for each column in a CREATE TABLE
 * statement, and for the column in an ALTER TABLE ADD COLUMN statement.
 *
 */

public class ColumnDefinitionNode extends TableElementNode
{
	boolean						isAutoincrement;
	
    /**
     * The data type of this column.
     */
    DataTypeDescriptor type;
    
	DataValueDescriptor			defaultValue;
	DefaultInfoImpl				defaultInfo;
	DefaultNode					defaultNode;
	boolean						keepCurrentDefault;
    GenerationClauseNode   generationClauseNode;
	long						autoincrementIncrement;
	long						autoincrementStart;
        boolean                                            autoincrementCycle;
	//This variable tells if the autoincrement column is participating 
	//in create or alter table. And if it is participating in alter
	//table, then it further knows if it is represting a change in 
	//increment value or a change in start value.
	//This information is later used to make sure that the autoincrement
	//column's increment value is not 0 at the time of create, or is not
	//getting set to 0 at the time of increment value modification.
//IC see: https://issues.apache.org/jira/browse/DERBY-783
	long						autoinc_create_or_modify_Start_Increment;
	boolean						autoincrementVerify;

	//autoinc_create_or_modify_Start_Increment will be set to one of the
	//following 3 values.
	//CREATE_AUTOINCREMENT - this autoincrement column definition is for create table
	public static final int CREATE_AUTOINCREMENT = 0;
	//MODIFY_AUTOINCREMENT_RESTART_VALUE - this column definition is for
	//alter table command to change the start value of the column
	public static final int MODIFY_AUTOINCREMENT_RESTART_VALUE = 1;
	//MODIFY_AUTOINCREMENT_INC_VALUE - this column definition is for
	//alter table command to change the increment value of the column
	public static final int MODIFY_AUTOINCREMENT_INC_VALUE = 2;
	//alter table command to change the ALWAYS vs DEFAULT nature of an autoinc column
	public static final int MODIFY_AUTOINCREMENT_ALWAYS_VS_DEFAULT = 3;
	
	public static final int MODIFY_AUTOINCREMENT_CYCLE_VALUE = 4;
	/**
     * Constructor for a ColumnDefinitionNode
	 *
	 * @param name			The name of the column
	 * @param defaultNode	The default value of the column
	 * @param dataTypeServices	A DataTypeServices telling the type
	 *				of the column
     * @param autoIncrementInfo Info for auto-increment columns
     * @param cm            The context manager
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ColumnDefinitionNode(
                    String name,
                    ValueNode defaultNode,
                    DataTypeDescriptor dataTypeServices,
                    long[] autoIncrementInfo,
                    ContextManager cm)
		throws StandardException
	{
        super(name, cm);
        this.type = dataTypeServices;

//IC see: https://issues.apache.org/jira/browse/DERBY-673
        if (defaultNode instanceof UntypedNullConstantNode)
		{
			/* No DTS yet for MODIFY DEFAULT */
			if (dataTypeServices != null)
			{
				defaultValue = 
					((UntypedNullConstantNode) defaultNode).
									convertDefaultNode(this.type);
			}
		}
		else if (defaultNode instanceof GenerationClauseNode)
		{
            generationClauseNode = (GenerationClauseNode) defaultNode;
		}
		else
		{
			if (SanityManager.DEBUG)
			{
				if (defaultNode != null &&
					! (defaultNode instanceof DefaultNode))
				{
					SanityManager.THROWASSERT(
						"defaultNode expected to be instanceof DefaultNode, not " +
						defaultNode.getClass().getName());
				}
			}
			this.defaultNode = (DefaultNode) defaultNode;
			if (autoIncrementInfo != null)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                long[] aii = autoIncrementInfo;
				autoincrementStart = aii[QueryTreeNode.AUTOINCREMENT_START_INDEX];
				autoincrementIncrement = aii[QueryTreeNode.AUTOINCREMENT_INC_INDEX];
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
				autoincrementCycle = aii[QueryTreeNode.AUTOINCREMENT_CYCLE] == 1 ? true : false;
				//Parser has passed the info about autoincrement column's status in the
				//following array element. It will tell if the autoinc column is part of 
				//a create table or if is a part of alter table. And if it is part of 
				//alter table, is it for changing the increment value or for changing 
				//the start value?
//IC see: https://issues.apache.org/jira/browse/DERBY-783
				autoinc_create_or_modify_Start_Increment = aii[QueryTreeNode.AUTOINCREMENT_CREATE_MODIFY];
				
				/*
				 * If using DB2 syntax to set increment value, will need to check if column
				 * is already created for autoincrement.
				 */
				autoincrementVerify = (aii[QueryTreeNode.AUTOINCREMENT_IS_AUTOINCREMENT_INDEX] > 0) ? false : true;
				isAutoincrement = true;
				// an autoincrement column cannot be null-- setting
				// non-nullability for this column is needed because 
				// you could create a column with ai default, add data, drop 
				// the default, and try to add it back again you'll get an
				// error because the column is marked nullable.
                if (dataTypeServices != null)
                    setNullability(false);
			}
		}
		// ColumnDefinitionNode instances can be subclassed by
		// ModifyColumnNode for use in ALTER TABLE .. ALTER COLUMN
		// statements, in which case the node represents the intended
		// changes to the column definition. For such a case, we
		// record whether or not the statement specified that the
		// column's default value should be changed. If we are to
		// keep the current default, ModifyColumnNode will re-read
		// the current default from the system catalogs prior to
		// performing the column alteration. See DERBY-4006
		// for more discussion of this behavior.
		this.keepCurrentDefault = (defaultNode == null);
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
			return "type: " + getType() + "\n" +
				"defaultValue: " + defaultValue + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Returns the unqualified name of the column being defined.
	 *
	 * @return	the name of the column
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getColumnName()
	{
		return this.name;
	}

	/**
	 * Returns the data type of the column being defined.
	 *
	 * @return	the data type of the column
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    final DataTypeDescriptor getType()
	{
		return type;
	}

    /** Set the type of this column */
    public void setType( DataTypeDescriptor dts ) { type = dts; }
    
    /**
     * Set the nullability of the column definition node.
      */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    final void setNullability(boolean nullable)
    {
        type = getType().getNullabilityType(nullable);
    }
    
    /**
     * Set the collation type, note derivation is always
     * implicit for any catalog item.
     */
    void setCollationType(int collationType)
    {
        type = getType().getCollatedType(collationType,
                StringDataValue.COLLATION_DERIVATION_IMPLICIT);
    }

	/**
	 * Return the DataValueDescriptor containing the default value for this
	 * column
	 *
	 * @return	The default value of the column
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    DataValueDescriptor getDefaultValue()
	{
		return this.defaultValue;
	}

	/**
	 * Return the DefaultInfo containing the default information for this
	 * column
	 *
	 * @return	The default info for the column
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    DefaultInfo getDefaultInfo()
	{
		return defaultInfo;
	}

	/**
	 * Set the generation clause (Default) bound to this column.
	 */
    public  void    setDefaultInfo( DefaultInfoImpl dii ) { defaultInfo = dii; }

	/**
	 * Return the DefaultNode, if any, associated with this node.
	 *
	 * @return The DefaultNode, if any, associated with this node.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    DefaultNode getDefaultNode()
	{
		return defaultNode;
	}

	/**
	 * Return true if this column has a generation clause.
	 */
	public boolean hasGenerationClause() { return ( generationClauseNode != null ); }

	/**
	 * Get the generation clause.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    GenerationClauseNode getGenerationClauseNode() {
        return generationClauseNode;
    }

	/**
	 * Is this an autoincrement column?
	 *
	 * @return Whether or not this is an autoincrement column.
	 */
    boolean isAutoincrementColumn()
	{
		if (SanityManager.DEBUG)
		{
			//increment value for autoincrement column can't be 0 if the autoinc column
			//is part of create table or it is part of alter table to change the 
			//increment value. 
//IC see: https://issues.apache.org/jira/browse/DERBY-783
			if (isAutoincrement && autoincrementIncrement == 0 && 
					(autoinc_create_or_modify_Start_Increment == ColumnDefinitionNode.CREATE_AUTOINCREMENT ||
							autoinc_create_or_modify_Start_Increment == ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE))
			{
				SanityManager.THROWASSERT(
					"autoincrementIncrement expected to be non-zero");
			}
			if ((! isAutoincrement) && 
				(autoincrementStart != 0 || autoincrementIncrement != 0))
			{
				SanityManager.THROWASSERT(
					"both autoincrementStart and autoincrementIncrement expected to be 0");
			}
		}
		return isAutoincrement;
	}

	/**
	 * Get the autoincrement start value
	 *
	 * @return Autoincrement start value.
	 */
	long getAutoincrementStart()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isAutoincrement,
				"isAutoincrement expected to be true");
		}
		return autoincrementStart;
	}

	/**
	 * Get the autoincrement increment value
	 *
	 * @return Autoincrement increment value.
	 */
	long getAutoincrementIncrement()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isAutoincrement,
				"isAutoincrement expected to be true");
		}
		return autoincrementIncrement;
	}

	/**
	 * Get the autoincrement cycle value
	 *
	 * @return Autoincrement cycle value.
	 */
	boolean getAutoincrementCycle()
	{
		if (SanityManager.DEBUG)
		{			SanityManager.ASSERT(isAutoincrement,
					"isAutoincrement expected to be true");
		}
		return autoincrementCycle;
	}


	/**
	 * Get the status of this autoincrement column 
	 *
	 * @return ColumnDefinitionNode.CREATE_AUTOINCREMENT - 
	 * 		if this definition is for autoincrement column creatoin
	 *   ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE -
	 * 		if this definition is for alter sutoincrement column to change the start value 
	 *   ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE 
	 * 		if this definition is for alter autoincrement column to change the increment value
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-783
	long getAutoinc_create_or_modify_Start_Increment()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isAutoincrement,
				"isAutoincrement expected to be true");
		}
		return autoinc_create_or_modify_Start_Increment;
	}
	
	/**
	 * Check the validity of a user type.  Checks whether this column
	 * definition describes a user type that either doesn't exist or is
	 * inaccessible, or that doesn't implement Serializable.
	 *
	 * @exception StandardException		Thrown on error
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void checkUserType(TableDescriptor td)
		throws StandardException
	{
		String			columnTypeName;

        // continue if this is a generated column and the datatype has been
        // omitted. we can't check generation clauses until later on
        if ( hasGenerationClause() && (getType() == null ) ) { return; }
//IC see: https://issues.apache.org/jira/browse/DERBY-3923

		/* Built-in types need no checking */
		if (!getType().getTypeId().userType())
			return;

        // bind the UDT if necessary
        setType( bindUserType( getType() ) );

		ClassInspector classInspector = getClassFactory().getClassInspector();

		columnTypeName =
			getType().getTypeId().getCorrespondingJavaTypeName();

		/* User type - We first check for the columnTypeName as a java class.
		 * If that fails, then we treat it as a class alias.
		 */

		boolean foundMatch = false;
		Throwable reason = null;
		try {
			foundMatch = classInspector.accessible(columnTypeName);
		} catch (ClassNotFoundException cnfe) {
			reason = cnfe;
		}

		if (!foundMatch)
		{
			throw StandardException.newException(SQLState.LANG_TYPE_DOESNT_EXIST, reason, columnTypeName,
																name);
		}

		if (! classInspector.assignableTo(columnTypeName,
											"java.io.Serializable")  &&
            // Before Java2, SQLData is not defined, assignableTo call returns false
            ! classInspector.assignableTo(columnTypeName,"java.sql.SQLData"))
        {
			getCompilerContext().addWarning(
				StandardException.newWarning(SQLState.LANG_TYPE_NOT_SERIALIZABLE, columnTypeName,
																 name));
		}
	}

	/**
	 * Get the UUID of the old column default.
	 *
	 * @return The UUID of the old column default.
	 */
	UUID getOldDefaultUUID()
	{
		return null;
	}

	/**
	 * Get the action associated with this node.
	 *
	 * @return The action associated with this node.
	 */
	int getAction()
	{
		return ColumnInfo.CREATE;
	}

	/**
	 * Check the validity of the default, if any, for this node.
	 *
	 * @param dd		The DataDictionary.
	 * @param td		The TableDescriptor.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void bindAndValidateDefault(DataDictionary dd, TableDescriptor td)
		throws StandardException
	{
		/* DB2 requires non-nullable columns to have a default in ALTER TABLE */
//IC see: https://issues.apache.org/jira/browse/DERBY-3923
		if (td != null && !hasGenerationClause() && !getType().isNullable() && defaultNode == null)
		{
			if (!isAutoincrement )
				throw StandardException.newException(SQLState.LANG_DB2_NOT_NULL_COLUMN_INVALID_DEFAULT, getColumnName());
		}
			
		// No work to do if no user specified default
		if (defaultNode == null)
		{
			return;
		}

		// No work to do if user specified NULL
		if (defaultValue != null)
		{
			return;
		}

		// Now validate the default
		validateDefault(dd, td);
	}


	/**
	 * Check the validity of the autoincrement values for this node.
	 * The following errors are thrown by this routine.
	 * 1. 42z21 Invalid Increment; i.e 0.
	 * 2. 42z22 Invalid Type; autoincrement created on a non-exact-numeric type
	 * 3. 42995 The requested function does not apply to global temporary tables
	 *
	 * @param 		dd		DataDictionary.
	 * @param		td		table descriptor.
	 * @param		tableType	base table or declared global temporary table.
	 *
	 * @exception 	StandardException if autoincrement default is incorrect; i.e
	 * 				if increment is 0 or if initial or increment values are out
	 * 				of range for the datatype.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void validateAutoincrement(DataDictionary dd,
                               TableDescriptor td,
                               int tableType) throws StandardException
	{
		if (isAutoincrement == false)
			return;

		if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_DECLARED_GLOBAL_TEMP_TABLE);

		//increment value for autoincrement column can't be 0 if the autoinc column
		//is part of create table or it is part of alter table to change the 
		//increment value. 
//IC see: https://issues.apache.org/jira/browse/DERBY-783
		if (autoincrementIncrement == 0 && 
				(autoinc_create_or_modify_Start_Increment == ColumnDefinitionNode.CREATE_AUTOINCREMENT ||
						autoinc_create_or_modify_Start_Increment == ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE))
			throw StandardException.newException(SQLState.LANG_AI_INVALID_INCREMENT, getColumnName());
		int jdbctype = getType().getTypeId().getJDBCTypeId();
		switch (jdbctype)
		{
		case Types.TINYINT:
			autoincrementCheckRange((long)Byte.MIN_VALUE, 
									(long)Byte.MAX_VALUE, 
									TypeId.TINYINT_NAME);
			break;
		case Types.SMALLINT:
			autoincrementCheckRange((long)Short.MIN_VALUE, 
									(long)Short.MAX_VALUE,
									TypeId.SMALLINT_NAME);
			break;
		case Types.INTEGER:
			autoincrementCheckRange((long)Integer.MIN_VALUE, 
									(long)Integer.MAX_VALUE,
									TypeId.INTEGER_NAME);
			break;
		case Types.BIGINT:
			autoincrementCheckRange(Long.MIN_VALUE, Long.MAX_VALUE,
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                                    TypeId.BIGINT_NAME);
			break;
		default:
			throw StandardException.newException(SQLState.LANG_AI_INVALID_TYPE,
												 getColumnName());
		}
	}

	/**
	 * checks to see if autoincrementIncrement and autoincrementInitial
	 * are within the bounds of the type whose min and max values are
	 * passed into this routine.
	 */
	private	void autoincrementCheckRange(long minValue, long maxValue,
									String typeName)
				throws StandardException					
	{
		if ((minValue > autoincrementIncrement) || 
			(maxValue < autoincrementIncrement))
		{
			throw StandardException.newException(
								 SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, typeName);
		}
		if ((minValue > autoincrementStart) || 
			(maxValue < autoincrementStart))
		{
			throw StandardException.newException(
								 SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, typeName);
		}			
	}
	/**
	 * Check the validity of the default for this node.
	 *
	 * @param td		The TableDescriptor.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void validateDefault(DataDictionary dd, TableDescriptor td)
		throws StandardException
	{
		if (defaultNode == null)
			return;

		//Examin whether default value is autoincrement.
//IC see: https://issues.apache.org/jira/browse/DERBY-167
		if (isAutoincrement){
			defaultInfo = createDefaultInfoOfAutoInc();
			return;
		}
		
		
		//Judged as default value is constant value.
		
		CompilerContext cc = getCompilerContext();

		ValueNode defaultTree = defaultNode.getDefaultTree();

		/* bind the default.
		 * Verify that it does not contain any ColumnReferences or subqueries
		 * and that it is type compatable with the column.
		 */
		final int previousReliability = cc.getReliability();
		try
		{
			/*
				Defaults cannot have dependencies as they
				should just be constants. Code used to exist
				to handle dependencies in defaults, now this
				is under sanity to ensure no dependencies exist.
			 */
			ProviderList apl = null;
			ProviderList prevAPL = null;

			if (SanityManager.DEBUG) {
				apl = new ProviderList();
				prevAPL = cc.getCurrentAuxiliaryProviderList();
				cc.setCurrentAuxiliaryProviderList(apl);
			}
			
			// Tell the compiler context to only allow deterministic nodes
			cc.setReliability( CompilerContext.DEFAULT_RESTRICTION );
			defaultTree = defaultTree.bindExpression(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                            new FromList(
                                getOptimizerFactory().doJoinOrderOptimization(),
								getContextManager()), 
							(SubqueryList) null,
							(List<AggregateNode>) null);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

			TypeId columnTypeId = getType().getTypeId();
			TypeId defaultTypeId = defaultTree.getTypeId();

			// Check for 'invalid default' errors (42894)
			// before checking for 'not storable' errors (42821).
			if (!defaultTypeIsValid(columnTypeId, getType(),
					defaultTypeId, defaultTree, defaultNode.getDefaultText()))
			{
					throw StandardException.newException(
						SQLState.LANG_DB2_INVALID_DEFAULT_VALUE,
						this.name);
			}

			// Now check 'not storable' errors.
			if (! getTypeCompiler(columnTypeId).
								storable(defaultTypeId, getClassFactory()))
			{
				throw StandardException.newException(SQLState.LANG_NOT_STORABLE, 
					columnTypeId.getSQLTypeName(),
					defaultTypeId.getSQLTypeName() );
			}

			// Save off the default text
			// RESOLVEDEFAULT - Convert to constant if possible
//IC see: https://issues.apache.org/jira/browse/DERBY-167
			defaultInfo = new DefaultInfoImpl(false,
							  defaultNode.getDefaultText(), 
							  defaultValue);

			if (SanityManager.DEBUG)
			{
				/* Save the APL off in the constraint node */
				if (apl.size() > 0)
				{

					SanityManager.THROWASSERT("DEFAULT clause has unexpected dependencies");
				}
				// Restore the previous AuxiliaryProviderList
				cc.setCurrentAuxiliaryProviderList(prevAPL);
			}

		}
		finally
		{
			cc.setReliability(previousReliability);
		}
	}


	protected static DefaultInfoImpl createDefaultInfoOfAutoInc(){
//IC see: https://issues.apache.org/jira/browse/DERBY-167
		return new DefaultInfoImpl(true,
					   null, 
					   null);
	}
	

	/**
	 * Check the validity of the default for this node
	 *
	 * @param columnType TypeId of the target column.
	 * @param columnDesc Description of the type of the
	 *		target column.
	 * @param defaultType TypeId of the default node.
	 * @param defaultNode Parsed ValueNode for the default value.
	 * @param defaultText Unparsed default value (as entered
	 * 		by user).
	 * @return True if the defaultNode abides by the restrictions
	 * 	imposed by DB2 on default constants; false otherwise.
	 *
	 */

    boolean defaultTypeIsValid(TypeId columnType,
		DataTypeDescriptor columnDesc, TypeId defaultType,
		ValueNode defaultNode, String defaultText)
//IC see: https://issues.apache.org/jira/browse/DERBY-582
	throws StandardException
	{


		/* We can use info about the way the parser works
		 * to guide this process a little (see the getNumericNode()
		 * method in sqlgrammar.jj):
		 *
		 * 1) Tinyint and Smallints are both parsed as "INT" types,
	 	 *	  while integers larger than a basic "INT" are parsed into
		 *	  "LONGINT" or, if needed, "DECIMAL".
		 * 2) Floats, doubles, and decimals with fractional parts
		 *	  are all parsed as "DECIMAL".
		 * 3) All strings are parsed as "CHAR" constants (no varchar
		 *	  or any others; see stringLiteral() method in
		 *	  sqlgrammar.jj).
		 */

		int colType = columnType.getTypeFormatId();
		int defType = (defaultType == null ? -1 : defaultType.getTypeFormatId());

		if (!defaultNode.isConstantExpression()) {
		// then we have a built-in function, such as "user"
		// or "current schema".  If the function is a datetime
		// value function, then we don't need any special
		// action; however, if it's a "user" or "current schema"
		// function, then the column must be a char type with
		// minimum lengths matching those of DB2 (note that
		// such limits are ONLY enforced on defaults, not at
		// normal insertion time).

			boolean charCol = ((colType == StoredFormatIds.CHAR_TYPE_ID) ||
				(colType == StoredFormatIds.VARCHAR_TYPE_ID) ||
				(colType == StoredFormatIds.LONGVARCHAR_TYPE_ID));

			if (defaultNode instanceof SpecialFunctionNode) {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                switch (((SpecialFunctionNode)defaultNode).kind) {
                    case SpecialFunctionNode.K_USER:
                    case SpecialFunctionNode.K_CURRENT_USER:
                    case SpecialFunctionNode.K_CURRENT_ROLE:
                    case SpecialFunctionNode.K_SESSION_USER:
                    case SpecialFunctionNode.K_SYSTEM_USER:
                        // DB2 enforces min length of 8.
                        // Note also: any size under 30 gives a warning in DB2.
                        return (charCol && (columnDesc.getMaximumWidth() >=
                                Limits.DB2_MIN_COL_LENGTH_FOR_CURRENT_USER));
//IC see: https://issues.apache.org/jira/browse/DERBY-104

                    case SpecialFunctionNode.K_CURRENT_SCHEMA:
                        // DB2 enforces min length of 128.
                        return (charCol && (columnDesc.getMaximumWidth() >=
                                Limits.DB2_MIN_COL_LENGTH_FOR_CURRENT_SCHEMA));
                    default:
                        // else, function not allowed.
                        return false;
                }
			}
		}

		switch (colType) {

//IC see: https://issues.apache.org/jira/browse/DERBY-4716
			case StoredFormatIds.BOOLEAN_TYPE_ID:
                return ( defaultNode instanceof BooleanConstantNode );
                
			case StoredFormatIds.INT_TYPE_ID:
			// DB2 doesn't allow floating point values to be used
			// as defaults for integer columns (they ARE allowed
			// as part of normal insertions, but not as defaults).
			// If the default is an integer that's too big, then
			// it won't have type INT_TYPE_ID (it'll be either
			// LONGINT or DECIMAL)--so we only allow the default
			// value if it's integer.
				return (defType == StoredFormatIds.INT_TYPE_ID);

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
            case StoredFormatIds.BIGINT_TYPE_ID:
			// This is a BIGINT column: we allow smallints, ints,
			// and big int constants.  Smallint and int literals
			// are both covered by INT_TYPE; big int literals are
			// covered by LONG_INT type.
				return ((defType == StoredFormatIds.INT_TYPE_ID)
                    || (defType == StoredFormatIds.BIGINT_TYPE_ID));
	
			case StoredFormatIds.DECIMAL_TYPE_ID:
				if (defType == StoredFormatIds.DECIMAL_TYPE_ID) {
				// only valid if scale and precision are within
				// those of the column.  Note that scale here should
				// exclude any trailing 0's after the decimal
					DataTypeDescriptor defDesc = defaultNode.getTypeServices();
					int len = defaultText.length();
					int precision = defDesc.getPrecision();
					int scale = defDesc.getScale();
					for (int i = 1; i <= scale; scale--, precision--) {
						if (defaultText.charAt(len - i) != '0')
							break;
					}
					return ((scale <= columnDesc.getScale()) &&
						((precision - scale) <=
						(columnDesc.getPrecision() - columnDesc.getScale())));
				}
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                else if ((defType == StoredFormatIds.BIGINT_TYPE_ID) ||
					(defType == StoredFormatIds.INT_TYPE_ID)) {
				// only valid if number of digits is within limits of
				// the decimal column.  We'll check this at insertion time;
				// see Beetle 5585 regarding the need to move that check to
				// here instead of waiting until insert time.  Until that's
				// done, just allow this and wait for insertion...
					return true;
				}
				else
				// no other types allowed.
					return false;

			case StoredFormatIds.CHAR_TYPE_ID:
			case StoredFormatIds.VARCHAR_TYPE_ID:
			case StoredFormatIds.LONGVARCHAR_TYPE_ID:
			// only valid if the default type is a character string.
			// That's not to say that all character defaults are
			// valid, but we only check for character string here;
			// further checking will be done at insertion time.  See
			// beetle 5585 regarding the need to move that check
			// to here instead of waiting until insert time.
				return (defType == StoredFormatIds.CHAR_TYPE_ID);

//IC see: https://issues.apache.org/jira/browse/DERBY-34
			case StoredFormatIds.BIT_TYPE_ID:
			case StoredFormatIds.VARBIT_TYPE_ID:
			case StoredFormatIds.LONGVARBIT_TYPE_ID:
			// only valid if the default type is a BIT string.
				return (defType == StoredFormatIds.BIT_TYPE_ID);

			case StoredFormatIds.USERDEFINED_TYPE_ID_V3:
			// default is only valid if it's the same type as the column.
				return (defType == colType);

			case StoredFormatIds.BLOB_TYPE_ID:
			case StoredFormatIds.CLOB_TYPE_ID:
			case StoredFormatIds.SMALLINT_TYPE_ID:
			case StoredFormatIds.REAL_TYPE_ID:
			case StoredFormatIds.DOUBLE_TYPE_ID:
			case StoredFormatIds.DATE_TYPE_ID:
			case StoredFormatIds.TIME_TYPE_ID:
			case StoredFormatIds.TIMESTAMP_TYPE_ID:
			// For these types, validity checks will be performed
			// by Derby at insertion time--see beetle 5585 regarding
			// the need to do such checks here instead of later.  For now,
			// just assume we're okay.
				return true;

			default:
			// All other default type checks either 
			// (TINYINT, etc), or 2) require a DB2 cast-
			// function (ex. blob(...), which Derby doesn't
			// support yet--see Beetle 5281), and so they are not
			// valid for Derby running in DB2 compatibility mode.
				return false;

		}

	}

    /**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void printSubNodes(int depth)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-4087
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (defaultNode != null) {
				printLabel(depth, "default: ");
				defaultNode.treePrint(depth + 1);
			}


			if (generationClauseNode != null) {
				printLabel(depth, "generationClause: ");
				generationClauseNode.treePrint(depth + 1);
			}
		}
	}
}
