/*

   Derby - Class org.apache.derby.impl.sql.compile.NodeFactoryImpl

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

import java.util.Properties;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.sql.compile.Optimizer;

import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.iapi.services.loader.ClassInfo;
import org.apache.derby.iapi.util.StringUtil;

/**
 * This class is a factory for QueryTreeNode nodes.  It exists to provide
 * methods to generate new nodes without having to call new directly.
 * In the future, it may implement caching of nodes, as well, to avoid
 * memory management and garbage collection.
 *
 * @author Jeff Lichtman
 */

public class NodeFactoryImpl extends NodeFactory implements ModuleControl, ModuleSupportable
{
	//////////////////////////////////////////////////////////////////////
	//
	// CONSTANTS
	//
	//////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////
	//
	// STATE
	//
	//////////////////////////////////////////////////////////////////////

	/* Do join order optimization by default */
	private Boolean joinOrderOptimization = Boolean.TRUE;

	private final ClassInfo[]	nodeCi = new ClassInfo[200];

	//////////////////////////////////////////////////////////////////////
	//
	// ModuleControl interface
	//
	//////////////////////////////////////////////////////////////////////

	public boolean canSupport(Properties startParams)
	{
		return	Monitor.isDesiredType( startParams, org.apache.derby.iapi.reference.EngineType.NONE );
	}

	/**
		@see Monitor
		@exception StandardException Ooops
	  */

	public void boot(boolean create, Properties startParams)
		 throws StandardException
	{
		/*
		** This system property determines whether to optimize join order
		** by default.  It is used mainly for testing - there are many tests
		** that assume the join order is fixed.
		*/
		String opt =
			PropertyUtil.getSystemProperty(Optimizer.JOIN_ORDER_OPTIMIZATION);
		if (opt != null)
		{
			joinOrderOptimization = Boolean.valueOf(opt);
		}
	}

	/**
		@see Monitor
	  */

	public void stop() {
	}

	/**
	  Every Module needs a public niladic constructor. It just does.
	  */
    public	NodeFactoryImpl() {}

	/** @see NodeFactory#doJoinOrderOptimization */
	public Boolean doJoinOrderOptimization()
	{
		return joinOrderOptimization;
	}

	/**
	 * @see NodeFactory#getNode
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode getNode(int nodeType, ContextManager cm)
											throws StandardException
	{

		ClassInfo ci = nodeCi[nodeType];

		Class nodeClass = null;
		if (ci == null)
		{
			String nodeName = nodeName(nodeType);

			try
			{
				nodeClass = Class.forName(nodeName);
			}
			catch (ClassNotFoundException cnfe)
			{
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Unexpected ClassNotFoundException",
																			cnfe);
				}
			}

			ci = new ClassInfo(nodeClass);
			nodeCi[nodeType] = ci;
		}

		QueryTreeNode retval = null;

		try
		{
			retval = (QueryTreeNode) ci.getNewInstance();
			//retval = (QueryTreeNode) nodeClass.newInstance();
		}
		catch (Exception iae)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Unexpected Exception", iae);
			}
		}

		retval.setContextManager(cm);
		retval.setNodeType(nodeType);

		return retval;
	}

	/**
	 * Translate a node type from C_NodeTypes to a class name
	 *
	 * @param nodeType	A node type identifier from C_NodeTypes
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected String nodeName(int nodeType)
			throws StandardException
	{
		switch (nodeType)
		{
		  // WARNING: WHEN ADDING NODE TYPES HERE, YOU MUST ALSO ADD
		  // THEM TO $WS/tools/release/config/dbms/cloudscapenodes.properties
			// xxxRESOLVE: why not make this a giant array and simply index into
			// it? manish Thu Feb 22 14:49:41 PST 2001  
		  case C_NodeTypes.CURRENT_ROW_LOCATION_NODE:
		  	return C_NodeNames.CURRENT_ROW_LOCATION_NODE_NAME;

		  case C_NodeTypes.GROUP_BY_LIST:
		  	return C_NodeNames.GROUP_BY_LIST_NAME;

          case C_NodeTypes.CURRENT_ISOLATION_NODE:
            return C_NodeNames.CURRENT_ISOLATION_NAME;

		  case C_NodeTypes.ORDER_BY_LIST:
		  	return C_NodeNames.ORDER_BY_LIST_NAME;

		  case C_NodeTypes.PREDICATE_LIST:
		  	return C_NodeNames.PREDICATE_LIST_NAME;

		  case C_NodeTypes.RESULT_COLUMN_LIST:
		  	return C_NodeNames.RESULT_COLUMN_LIST_NAME;

		  case C_NodeTypes.SUBQUERY_LIST:
		  	return C_NodeNames.SUBQUERY_LIST_NAME;

		  case C_NodeTypes.TABLE_ELEMENT_LIST:
		  	return C_NodeNames.TABLE_ELEMENT_LIST_NAME;

		  case C_NodeTypes.UNTYPED_NULL_CONSTANT_NODE:
		  	return C_NodeNames.UNTYPED_NULL_CONSTANT_NODE_NAME;

		  case C_NodeTypes.TABLE_ELEMENT_NODE:
		  	return C_NodeNames.TABLE_ELEMENT_NODE_NAME;

		  case C_NodeTypes.VALUE_NODE_LIST:
		  	return C_NodeNames.VALUE_NODE_LIST_NAME;

		  case C_NodeTypes.ALL_RESULT_COLUMN:
		  	return C_NodeNames.ALL_RESULT_COLUMN_NAME;

		  case C_NodeTypes.GET_CURRENT_CONNECTION_NODE:
		  	return C_NodeNames.GET_CURRENT_CONNECTION_NODE_NAME;

		  case C_NodeTypes.NOP_STATEMENT_NODE:
		  	return C_NodeNames.NOP_STATEMENT_NODE_NAME;

		  case C_NodeTypes.SET_TRANSACTION_ISOLATION_NODE:
		  	return C_NodeNames.SET_TRANSACTION_ISOLATION_NODE_NAME;

		  case C_NodeTypes.CHAR_LENGTH_OPERATOR_NODE:
			return C_NodeNames.LENGTH_OPERATOR_NODE_NAME;

		  // ISNOTNULL compressed into ISNULL
		  case C_NodeTypes.IS_NOT_NULL_NODE:
		  case C_NodeTypes.IS_NULL_NODE:
		  	return C_NodeNames.IS_NULL_NODE_NAME;

		  case C_NodeTypes.NOT_NODE:
		  	return C_NodeNames.NOT_NODE_NAME;

		  case C_NodeTypes.SQL_TO_JAVA_VALUE_NODE:
		  	return C_NodeNames.SQL_TO_JAVA_VALUE_NODE_NAME;

		  case C_NodeTypes.TABLE_NAME:
		  	return C_NodeNames.TABLE_NAME_NAME;

		  case C_NodeTypes.GROUP_BY_COLUMN:
		  	return C_NodeNames.GROUP_BY_COLUMN_NAME;

		  case C_NodeTypes.JAVA_TO_SQL_VALUE_NODE:
		  	return C_NodeNames.JAVA_TO_SQL_VALUE_NODE_NAME;

		  case C_NodeTypes.FROM_LIST:
		  	return C_NodeNames.FROM_LIST_NAME;

		  case C_NodeTypes.BOOLEAN_CONSTANT_NODE:
		  	return C_NodeNames.BOOLEAN_CONSTANT_NODE_NAME;

		  case C_NodeTypes.AND_NODE:
		  	return C_NodeNames.AND_NODE_NAME;

		  case C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
		  case C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
		  case C_NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
		  case C_NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
		  case C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
		  case C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE:
			  return C_NodeNames.BINARY_RELATIONAL_OPERATOR_NODE_NAME;

		  case C_NodeTypes.BINARY_MINUS_OPERATOR_NODE:
		  case C_NodeTypes.BINARY_PLUS_OPERATOR_NODE:
		  case C_NodeTypes.BINARY_TIMES_OPERATOR_NODE:
		  case C_NodeTypes.BINARY_DIVIDE_OPERATOR_NODE:
		  case C_NodeTypes.MOD_OPERATOR_NODE:
		  	return C_NodeNames.BINARY_ARITHMETIC_OPERATOR_NODE_NAME;

		  case C_NodeTypes.COALESCE_FUNCTION_NODE:
		  	return C_NodeNames.COALESCE_FUNCTION_NODE_NAME;

		  case C_NodeTypes.CONCATENATION_OPERATOR_NODE:
		  	return C_NodeNames.CONCATENATION_OPERATOR_NODE_NAME;

		  case C_NodeTypes.LIKE_OPERATOR_NODE:
		  	return C_NodeNames.LIKE_OPERATOR_NODE_NAME;

		  case C_NodeTypes.OR_NODE:
		  	return C_NodeNames.OR_NODE_NAME;

		  case C_NodeTypes.BETWEEN_OPERATOR_NODE:
		  	return C_NodeNames.BETWEEN_OPERATOR_NODE_NAME;

		  case C_NodeTypes.CONDITIONAL_NODE:
		  	return C_NodeNames.CONDITIONAL_NODE_NAME;

		  case C_NodeTypes.IN_LIST_OPERATOR_NODE:
		  	return C_NodeNames.IN_LIST_OPERATOR_NODE_NAME;

		  case C_NodeTypes.BIT_CONSTANT_NODE:
		  	return C_NodeNames.BIT_CONSTANT_NODE_NAME;

		  case C_NodeTypes.LONGVARBIT_CONSTANT_NODE:
		  case C_NodeTypes.VARBIT_CONSTANT_NODE:
          case C_NodeTypes.BLOB_CONSTANT_NODE:
			return C_NodeNames.VARBIT_CONSTANT_NODE_NAME;

		  case C_NodeTypes.CAST_NODE:
		  	return C_NodeNames.CAST_NODE_NAME;

		  case C_NodeTypes.CHAR_CONSTANT_NODE:
		  case C_NodeTypes.LONGVARCHAR_CONSTANT_NODE:
		  case C_NodeTypes.VARCHAR_CONSTANT_NODE:
          case C_NodeTypes.CLOB_CONSTANT_NODE:
			return C_NodeNames.CHAR_CONSTANT_NODE_NAME;

		  case C_NodeTypes.COLUMN_REFERENCE:
		  	return C_NodeNames.COLUMN_REFERENCE_NAME;

		  case C_NodeTypes.DROP_INDEX_NODE:
		  	return C_NodeNames.DROP_INDEX_NODE_NAME;

		  case C_NodeTypes.DROP_SPS_NODE:
		  	return C_NodeNames.DROP_SPS_NODE_NAME;

		  case C_NodeTypes.DROP_TRIGGER_NODE:
		  	return C_NodeNames.DROP_TRIGGER_NODE_NAME;

		  case C_NodeTypes.READ_CURSOR_NODE:
		  	return C_NodeNames.READ_CURSOR_NODE_NAME;

		  case C_NodeTypes.TINYINT_CONSTANT_NODE:
		  case C_NodeTypes.SMALLINT_CONSTANT_NODE:
		  case C_NodeTypes.INT_CONSTANT_NODE:
		  case C_NodeTypes.LONGINT_CONSTANT_NODE:
		  case C_NodeTypes.DECIMAL_CONSTANT_NODE:
		  case C_NodeTypes.DOUBLE_CONSTANT_NODE:
		  case C_NodeTypes.FLOAT_CONSTANT_NODE:
			return C_NodeNames.NUMERIC_CONSTANT_NODE_NAME;

		  case C_NodeTypes.USERTYPE_CONSTANT_NODE:
		  	return C_NodeNames.USERTYPE_CONSTANT_NODE_NAME;

		  case C_NodeTypes.PREDICATE:
		  	return C_NodeNames.PREDICATE_NAME;

		  case C_NodeTypes.RESULT_COLUMN:
		  	return C_NodeNames.RESULT_COLUMN_NAME;

		  case C_NodeTypes.SET_SCHEMA_NODE:
		  	return C_NodeNames.SET_SCHEMA_NODE_NAME;

		  case C_NodeTypes.SIMPLE_STRING_OPERATOR_NODE:
		  	return C_NodeNames.SIMPLE_STRING_OPERATOR_NODE_NAME;

		  case C_NodeTypes.STATIC_CLASS_FIELD_REFERENCE_NODE:
		  	return C_NodeNames.STATIC_CLASS_FIELD_REFERENCE_NODE_NAME;

		  case C_NodeTypes.STATIC_METHOD_CALL_NODE:
		  	return C_NodeNames.STATIC_METHOD_CALL_NODE_NAME;

		  case C_NodeTypes.EXTRACT_OPERATOR_NODE:
		  	return C_NodeNames.EXTRACT_OPERATOR_NODE_NAME;

		  case C_NodeTypes.PARAMETER_NODE:
		  	return C_NodeNames.PARAMETER_NODE_NAME;

		  case C_NodeTypes.DROP_SCHEMA_NODE:
		  	return C_NodeNames.DROP_SCHEMA_NODE_NAME;

		  case C_NodeTypes.DROP_TABLE_NODE:
		  	return C_NodeNames.DROP_TABLE_NODE_NAME;

		  case C_NodeTypes.DROP_VIEW_NODE:
		  	return C_NodeNames.DROP_VIEW_NODE_NAME;

		  case C_NodeTypes.SUBQUERY_NODE:
		  	return C_NodeNames.SUBQUERY_NODE_NAME;

		  case C_NodeTypes.BASE_COLUMN_NODE:
		  	return C_NodeNames.BASE_COLUMN_NODE_NAME;

		  case C_NodeTypes.CALL_STATEMENT_NODE:
		  	return C_NodeNames.CALL_STATEMENT_NODE_NAME;

		  case C_NodeTypes.MODIFY_COLUMN_DEFAULT_NODE:
          case C_NodeTypes.MODIFY_COLUMN_TYPE_NODE:
		  case C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE:
		  case C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE:
			return C_NodeNames.MODIFY_COLUMN_NODE_NAME;

		  case C_NodeTypes.NON_STATIC_METHOD_CALL_NODE:
		  	return C_NodeNames.NON_STATIC_METHOD_CALL_NODE_NAME;

		  case C_NodeTypes.CURRENT_OF_NODE:
		  	return C_NodeNames.CURRENT_OF_NODE_NAME;

		  case C_NodeTypes.DEFAULT_NODE:
		  	return C_NodeNames.DEFAULT_NODE_NAME;

		  case C_NodeTypes.DELETE_NODE:
		  	return C_NodeNames.DELETE_NODE_NAME;

		  case C_NodeTypes.UPDATE_NODE:
		  	return C_NodeNames.UPDATE_NODE_NAME;

		  case C_NodeTypes.ORDER_BY_COLUMN:
		  	return C_NodeNames.ORDER_BY_COLUMN_NAME;

		  case C_NodeTypes.ROW_RESULT_SET_NODE:
		  	return C_NodeNames.ROW_RESULT_SET_NODE_NAME;

		  case C_NodeTypes.VIRTUAL_COLUMN_NODE:
		  	return C_NodeNames.VIRTUAL_COLUMN_NODE_NAME;

		  case C_NodeTypes.CURRENT_DATETIME_OPERATOR_NODE:
		  	return C_NodeNames.CURRENT_DATETIME_OPERATOR_NODE_NAME;

		  case C_NodeTypes.CURRENT_USER_NODE:
		  	return C_NodeNames.CURRENT_USER_NODE_NAME;

		  case C_NodeTypes.IS_NODE:
		  	return C_NodeNames.IS_NODE_NAME;

		  case C_NodeTypes.LOCK_TABLE_NODE:
		  	return C_NodeNames.LOCK_TABLE_NODE_NAME;

		  case C_NodeTypes.ALTER_TABLE_NODE:
		  	return C_NodeNames.ALTER_TABLE_NODE_NAME;

		  case C_NodeTypes.AGGREGATE_NODE:
		  	return C_NodeNames.AGGREGATE_NODE_NAME;

		  case C_NodeTypes.COLUMN_DEFINITION_NODE:
		  	return C_NodeNames.COLUMN_DEFINITION_NODE_NAME;

		  case C_NodeTypes.EXEC_SPS_NODE:
		  	return C_NodeNames.EXEC_SPS_NODE_NAME;

		  case C_NodeTypes.FK_CONSTRAINT_DEFINITION_NODE:
		  	return C_NodeNames.FK_CONSTRAINT_DEFINITION_NODE_NAME;

		  case C_NodeTypes.FROM_VTI:
		  	return C_NodeNames.FROM_VTI_NAME;

		  case C_NodeTypes.MATERIALIZE_RESULT_SET_NODE:
		  	return C_NodeNames.MATERIALIZE_RESULT_SET_NODE_NAME;

		  case C_NodeTypes.NORMALIZE_RESULT_SET_NODE:
		  	return C_NodeNames.NORMALIZE_RESULT_SET_NODE_NAME;

		  case C_NodeTypes.SCROLL_INSENSITIVE_RESULT_SET_NODE:
		  	return C_NodeNames.SCROLL_INSENSITIVE_RESULT_SET_NODE_NAME;

		  case C_NodeTypes.ORDER_BY_NODE:
		  	return C_NodeNames.ORDER_BY_NODE_NAME;

		  case C_NodeTypes.DISTINCT_NODE:
		  	return C_NodeNames.DISTINCT_NODE_NAME;

          case C_NodeTypes.LOCATE_FUNCTION_NODE:
		  case C_NodeTypes.SUBSTRING_OPERATOR_NODE:
		  case C_NodeTypes.TRIM_OPERATOR_NODE:
		  	return C_NodeNames.TERNARY_OPERATOR_NODE_NAME;

		  case C_NodeTypes.SELECT_NODE:
		  	return C_NodeNames.SELECT_NODE_NAME;

		  case C_NodeTypes.CREATE_VIEW_NODE:
		  	return C_NodeNames.CREATE_VIEW_NODE_NAME;

		  case C_NodeTypes.CONSTRAINT_DEFINITION_NODE:
		  	return C_NodeNames.CONSTRAINT_DEFINITION_NODE_NAME;

		  case C_NodeTypes.ALTER_SPS_NODE:
		  	return C_NodeNames.ALTER_SPS_NODE_NAME;

		  case C_NodeTypes.NEW_INVOCATION_NODE:
		  	return C_NodeNames.NEW_INVOCATION_NODE_NAME;

		  case C_NodeTypes.CREATE_SCHEMA_NODE:
		  	return C_NodeNames.CREATE_SCHEMA_NODE_NAME;

		  case C_NodeTypes.FROM_BASE_TABLE:
		  	return C_NodeNames.FROM_BASE_TABLE_NAME;

		  case C_NodeTypes.FROM_SUBQUERY:
		  	return C_NodeNames.FROM_SUBQUERY_NAME;

		  case C_NodeTypes.GROUP_BY_NODE:
		  	return C_NodeNames.GROUP_BY_NODE_NAME;

		  case C_NodeTypes.INSERT_NODE:
		  	return C_NodeNames.INSERT_NODE_NAME;

		  case C_NodeTypes.JOIN_NODE:
		  	return C_NodeNames.JOIN_NODE_NAME;

		  case C_NodeTypes.CREATE_TABLE_NODE:
		  	return C_NodeNames.CREATE_TABLE_NODE_NAME;

		  case C_NodeTypes.RENAME_NODE:
		  	return C_NodeNames.RENAME_NODE_NAME;

		  case C_NodeTypes.UNION_NODE:
		  	return C_NodeNames.UNION_NODE_NAME;

		  case C_NodeTypes.CREATE_TRIGGER_NODE:
		  	return C_NodeNames.CREATE_TRIGGER_NODE_NAME;

		  case C_NodeTypes.HALF_OUTER_JOIN_NODE:
		  	return C_NodeNames.HALF_OUTER_JOIN_NODE_NAME;

		  case C_NodeTypes.CREATE_INDEX_NODE:
		  	return C_NodeNames.CREATE_INDEX_NODE_NAME;

		  case C_NodeTypes.CURSOR_NODE:
		  	return C_NodeNames.CURSOR_NODE_NAME;

		  case C_NodeTypes.HASH_TABLE_NODE:
		  	return C_NodeNames.HASH_TABLE_NODE_NAME;

		  case C_NodeTypes.INDEX_TO_BASE_ROW_NODE:
		  	return C_NodeNames.INDEX_TO_BASE_ROW_NODE_NAME;

		  case C_NodeTypes.CREATE_ALIAS_NODE:
		  	return C_NodeNames.CREATE_ALIAS_NODE_NAME;

		  case C_NodeTypes.PROJECT_RESTRICT_NODE:
		  	return C_NodeNames.PROJECT_RESTRICT_NODE_NAME;

		  case C_NodeTypes.SQL_BOOLEAN_CONSTANT_NODE:
		  	return C_NodeNames.SQL_BOOLEAN_CONSTANT_NODE_NAME;

		  case C_NodeTypes.DROP_ALIAS_NODE:
		  	return C_NodeNames.DROP_ALIAS_NODE_NAME;

		  case C_NodeTypes.TEST_CONSTRAINT_NODE:
		  	return C_NodeNames.TEST_CONSTRAINT_NODE_NAME;

          case C_NodeTypes.ABSOLUTE_OPERATOR_NODE:
          case C_NodeTypes.SQRT_OPERATOR_NODE:
		  case C_NodeTypes.UNARY_PLUS_OPERATOR_NODE:
		  case C_NodeTypes.UNARY_MINUS_OPERATOR_NODE:
            return C_NodeNames.UNARY_ARITHMETIC_OPERATOR_NODE_NAME;

		  case C_NodeTypes.SAVEPOINT_NODE:
		  	return C_NodeNames.SAVEPOINT_NODE_NAME;

		  case C_NodeTypes.TIMESTAMP_OPERATOR_NODE:
            return C_NodeNames.TIMESTAMP_OPERATOR_NODE_NAME;

		  case C_NodeTypes.DB2_LENGTH_OPERATOR_NODE:
            return C_NodeNames.DB2_LENGTH_OPERATOR_NODE_NAME;

		  // WARNING: WHEN ADDING NODE TYPES HERE, YOU MUST ALSO ADD
		  // THEM TO $WS/tools/release/config/dbms/cloudscapenodes.properties

		  default:
			throw StandardException.newException(SQLState.NOT_IMPLEMENTED);
		}
	}


	/**
	 * Get one of the several types of create alias nodes. Carved out of parser
	 * so this could be used by ALTER PUBLICATION.
	 *
	 * @param aliasName	The name of the alias
	 * @param sourceJarFile			(Plugin only) the jar file that the source method lives in
	 * @param fullStaticMethodName	The full path/method name
	 * @param aliasSpecificInfo	The full path of the target method name,
	 *								if any
	 * @param aliasType	The type of alias to create
	 * @param delimitedIdentifier	Whether or not to treat the class name
	 *								as a delimited identifier if trying to
	 *								resolve it as a class alias.
	 * @param cm			A ContextManager
	 *
	 * @return	A CreateAliasNode matching the given parameters
	 *
	 * @exception StandardException		Thrown on error
	 */
	public	QueryTreeNode
	getCreateAliasNode(
		Object aliasName,
		String fullStaticMethodName,
		Object aliasSpecificInfo,
		char aliasType,
		Boolean delimitedIdentifier,
		ContextManager cm)
		throws StandardException
	{
		int nodeType;
		String methodName = null;
		String javaClassName = fullStaticMethodName;
		String targetMethodName = null;
		String targetClassName = null;

		nodeType = C_NodeTypes.CREATE_ALIAS_NODE;

		{
			/*
			** Parse the class name, splitting out the class name from
			** the method name.
			*/
			int lastPeriod = fullStaticMethodName.lastIndexOf(".");
			if ((lastPeriod == -1) ||
				fullStaticMethodName.length() == lastPeriod + 1)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_FULL_STATIC_METHOD_NAME, 
														fullStaticMethodName);
			}
			else
			{
				methodName = fullStaticMethodName.substring(lastPeriod + 1);
				javaClassName = fullStaticMethodName.substring(0, lastPeriod);
			}
		}

		return getNode(
			nodeType,
			aliasName,
			javaClassName,
			methodName,
			aliasSpecificInfo,
			new Character(aliasType),
			delimitedIdentifier,
			cm );
	}


}
