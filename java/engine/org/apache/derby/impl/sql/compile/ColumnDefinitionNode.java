/*

   Derby - Class org.apache.derby.impl.sql.compile.ColumnDefinitionNode

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

import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.reference.DB2Limit;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.ProviderList;
import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.sql.execute.ColumnInfo;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.DefaultInfo;
import org.apache.derby.catalog.UUID;

import org.apache.derby.catalog.types.DefaultInfoImpl;

import java.util.Vector;
import java.sql.Types;

/**
 * A ColumnDefinitionNode represents a column definition in a DDL statement.
 * There will be a ColumnDefinitionNode for each column in a CREATE TABLE
 * statement, and for the column in an ALTER TABLE ADD COLUMN statement.
 *
 * @author Jeff Lichtman
 */

public class ColumnDefinitionNode extends TableElementNode
{
	boolean						isAutoincrement;
	DataTypeDescriptor			dataTypeServices;
	DataValueDescriptor			defaultValue;
	DefaultInfoImpl				defaultInfo;
	DefaultNode					defaultNode;
	long						autoincrementIncrement;
	long						autoincrementStart;
	boolean						autoincrementVerify;

	/**
	 * Initializer for a ColumnDefinitionNode
	 *
	 * @param name			The name of the column
	 * @param defaultNode	The default value of the column
	 * @param dataTypeServices	A DataTypeServices telling the type
	 *				of the column
	 * @param autoIncrementInfo	Info for autoincrement columns
	 *
	 */

	public void init(
					Object name,
					Object defaultNode,
					Object dataTypeServices,
					Object autoIncrementInfo)
		throws StandardException
	{
		super.init(name);
		this.dataTypeServices = (DataTypeDescriptor) dataTypeServices;
		if (defaultNode instanceof UntypedNullConstantNode)
		{
			/* No DTS yet for MODIFY DEFAULT */
			if (dataTypeServices != null)
			{
				defaultValue = 
					((UntypedNullConstantNode) defaultNode).
									convertDefaultNode(this.dataTypeServices);
			}
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
				long[] aii = (long[]) autoIncrementInfo;
				autoincrementStart = aii[QueryTreeNode.AUTOINCREMENT_START_INDEX];
				autoincrementIncrement = aii[QueryTreeNode.AUTOINCREMENT_INC_INDEX];

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
					(this.dataTypeServices).setNullability(false);
			}
		}
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
			return "dataTypeServices: " + dataTypeServices.toString() + "\n" +
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
	public String getColumnName()
	{
		return this.name;
	}

	/**
	 * Returns the data type services of the column being defined.
	 *
	 * @return	the data type services of the column
	 */
	public DataTypeDescriptor getDataTypeServices()
	{
		return this.dataTypeServices;
	}

	/**
	 * Return the DataValueDescriptor containing the default value for this
	 * column
	 *
	 * @return	The default value of the column
	 */

	public DataValueDescriptor getDefaultValue()
	{
		return this.defaultValue;
	}

	/**
	 * Return the DefaultInfo containing the default information for this
	 * column
	 *
	 * @return	The default info for the column
	 */

	public DefaultInfo getDefaultInfo()
	{
		return defaultInfo;
	}

	/**
	 * Return the DefaultNode, if any, associated with this node.
	 *
	 * @return The DefaultNode, if any, associated with this node.
	 */
	public DefaultNode getDefaultNode()
	{
		return defaultNode;
	}

	/**
	 * Is this an autoincrement column?
	 *
	 * @return Whether or not this is an autoincrement column.
	 */
	public boolean isAutoincrementColumn()
	{
		if (SanityManager.DEBUG)
		{
			if (isAutoincrement && autoincrementIncrement == 0)
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
	 * Check the validity of a user type.  Checks whether this column
	 * definition describes a user type that either doesn't exist or is
	 * inaccessible, or that doesn't implement Serializable.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void checkUserType(TableDescriptor td) 
		throws StandardException
	{
		String			columnTypeName;

		/* Built-in types need no checking */
		if (dataTypeServices.getTypeId().builtIn())
			return;

		ClassInspector classInspector = getClassFactory().getClassInspector();

		columnTypeName =
			dataTypeServices.getTypeId().getCorrespondingJavaTypeName();




		/* User type - We first check for the columnTypeName as a java class.
		 * If that fails, then we treat it as a class alias.
		 */

		boolean foundMatch = false;
		Throwable reason = null;
		try {
			foundMatch = classInspector.accessible(columnTypeName);
		} catch (ClassNotFoundException cnfe) {
			reason = cnfe;
		} catch (LinkageError le) {
			reason = le;
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
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void bindAndValidateDefault(DataDictionary dd, TableDescriptor td)
		throws StandardException
	{
		/* DB2 requires non-nullable columns to have a default in ALTER TABLE */
		if (td != null && !dataTypeServices.isNullable() && defaultNode == null)
		{
			if (!isAutoincrement)
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
	public void validateAutoincrement(DataDictionary dd, TableDescriptor td, int tableType)
	     throws StandardException
	{
		if (isAutoincrement == false)
			return;

		if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_DECLARED_GLOBAL_TEMP_TABLE);

		if (autoincrementIncrement == 0)
			throw StandardException.newException(SQLState.LANG_AI_INVALID_INCREMENT, getColumnName());
		int jdbctype = dataTypeServices.getTypeId().getJDBCTypeId();
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
									TypeId.LONGINT_NAME);
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
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void validateDefault(DataDictionary dd, TableDescriptor td)
		throws StandardException
	{
		CompilerContext cc;

		if (defaultNode == null)
			return;

		cc = getCompilerContext();

		ValueNode defaultTree = defaultNode.getDefaultTree();

		/* bind the default.
		 * Verify that it does not contain any ColumnReferences or subqueries
		 * and that it is type compatable with the column.
		 */
		final int previousReliability = cc.getReliability();
		try
		{
			/* Each default can have its own set of dependencies.
			 * These dependencies need to be shared with the prepared
			 * statement as well.  We create a new auxiliary provider list
			 * for the default, "push" it on the compiler context
			 * by swapping it with the current auxiliary provider list
			 * and the "pop" it when we're done by restoring the old 
			 * auxiliary provider list.
			 */
			ProviderList apl = new ProviderList();

			ProviderList prevAPL = cc.getCurrentAuxiliaryProviderList();
			cc.setCurrentAuxiliaryProviderList(apl);

			// Tell the compiler context to only allow deterministic nodes
			cc.setReliability( CompilerContext.DEFAULT_RESTRICTION );
			defaultTree = defaultTree.bindExpression(
							(FromList) getNodeFactory().getNode(
								C_NodeTypes.FROM_LIST,
								getNodeFactory().doJoinOrderOptimization(),
								getContextManager()), 
							(SubqueryList) null,
							(Vector) null);

			TypeId columnTypeId = (TypeId) dataTypeServices.getTypeId();
			TypeId defaultTypeId = defaultTree.getTypeId();

			// Check for 'invalid default' errors (42894)
			// before checking for 'not storable' errors (42821).
			if (!defaultTypeIsValid(columnTypeId, dataTypeServices,
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
			defaultInfo = new DefaultInfoImpl(defaultNode.getDefaultText(), defaultValue);

			/* Save the APL off in the constraint node */
			if (apl.size() > 0)
			{
				defaultNode.setAuxiliaryProviderList(apl);
				// Add info on any providers to DefaultInfo
				ProviderInfo[]	providerInfos = null;

				/* Get all the dependencies for the current statement and transfer
				 * them to this view.
				 */
				DependencyManager dm;
				dm = dd.getDependencyManager();
				providerInfos = dm.getPersistentProviderInfos(apl);
				defaultInfo.setProviderInfo(providerInfos);
			}

			// Restore the previous AuxiliaryProviderList
			cc.setCurrentAuxiliaryProviderList(prevAPL);
		}
		finally
		{
			cc.setReliability(previousReliability);
		}
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

	public boolean defaultTypeIsValid(TypeId columnType,
		DataTypeDescriptor columnDesc, TypeId defaultType,
		ValueNode defaultNode, String defaultText)
	{

		if (defaultText.length() > DB2Limit.DB2_CHAR_MAXWIDTH)
		// DB2 spec says this isn't allowed.
			return false;

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

			if (defaultNode instanceof CurrentUserNode) {

				defaultText = defaultText.toLowerCase(java.util.Locale.ENGLISH);
				if (defaultText.indexOf("user") != -1)
				// DB2 enforces min length of 8.
				// Note also: any size under 30 gives a warning in DB2.
					return (charCol && (columnDesc.getMaximumWidth() >=
						DB2Limit.MIN_COL_LENGTH_FOR_CURRENT_USER));

				if ((defaultText.indexOf("schema") != -1) ||
					(defaultText.indexOf("sqlid") != -1))
				// DB2 enforces min length of 128.
					return (charCol && (columnDesc.getMaximumWidth() >=
						DB2Limit.MIN_COL_LENGTH_FOR_CURRENT_SCHEMA));

				// else, function not allowed.
				return false;

			}

		}

		switch (colType) {

			case StoredFormatIds.INT_TYPE_ID:
			// DB2 doesn't allow floating point values to be used
			// as defaults for integer columns (they ARE allowed
			// as part of normal insertions, but not as defaults).
			// If the default is an integer that's too big, then
			// it won't have type INT_TYPE_ID (it'll be either
			// LONGINT or DECIMAL)--so we only allow the default
			// value if it's integer.
				return (defType == StoredFormatIds.INT_TYPE_ID);

			case StoredFormatIds.LONGINT_TYPE_ID:
			// This is a BIGINT column: we allow smallints, ints,
			// and big int constants.  Smallint and int literals
			// are both covered by INT_TYPE; big int literals are
			// covered by LONG_INT type.
				return ((defType == StoredFormatIds.INT_TYPE_ID)
					|| (defType == StoredFormatIds.LONGINT_TYPE_ID));
	
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
				else if ((defType == StoredFormatIds.LONGINT_TYPE_ID) ||
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
			// by Cloudscape at insertion time--see beetle 5585 regarding
			// the need to do such checks here instead of later.  For now,
			// just assume we're okay.
				return true;

			default:
			// All other default type checks either 
			// (TINYINT, NATIONAL_CHAR, etc), or 2) require a DB2 cast-
			// function (ex. blob(...), which Cloudscape doesn't
			// support yet--see Beetle 5281), and so they are not
			// valid for Cloudscape running in DB2 compatibility mode.
				return false;

		}

	}

}
