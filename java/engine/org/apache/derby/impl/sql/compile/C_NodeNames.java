/*

   Derby - Class org.apache.derby.impl.sql.compile.C_NodeNames

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

/**
 * This is the set of constants used to identify the classes
 * that are used in NodeFactoryImpl.
 *
 * This class is not shipped. The names are used in
 * NodeFactoryImpl, mapped from int NodeTypes and used in
 * Class.forName calls.
 *
 * WARNING: WHEN ADDING NODE TYPES HERE, YOU MUST ALSO ADD
 * THEM TO $WS/tools/release/config/dbms/cloudscapenodes.properties
 *
 * @author ames
 */

public interface C_NodeNames
{

	// The names are in alphabetic order.
	//
    // WARNING: WHEN ADDING NODE TYPES HERE, YOU MUST ALSO ADD
    // THEM TO $WS/tools/release/config/dbms/cloudscapenodes.properties

	static final String AGGREGATE_NODE_NAME = "org.apache.derby.impl.sql.compile.AggregateNode";

	static final String ALL_RESULT_COLUMN_NAME = "org.apache.derby.impl.sql.compile.AllResultColumn";

	static final String ALTER_SPS_NODE_NAME = "org.apache.derby.impl.sql.compile.AlterSPSNode";

	static final String ALTER_TABLE_NODE_NAME = "org.apache.derby.impl.sql.compile.AlterTableNode";

	static final String AND_NODE_NAME = "org.apache.derby.impl.sql.compile.AndNode";

	static final String BASE_COLUMN_NODE_NAME = "org.apache.derby.impl.sql.compile.BaseColumnNode";

	static final String BETWEEN_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.BetweenOperatorNode";

	static final String BINARY_ARITHMETIC_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.BinaryArithmeticOperatorNode";

	static final String BINARY_RELATIONAL_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode";

	static final String BIT_CONSTANT_NODE_NAME = "org.apache.derby.impl.sql.compile.BitConstantNode";

	static final String BOOLEAN_CONSTANT_NODE_NAME = "org.apache.derby.impl.sql.compile.BooleanConstantNode";

	static final String CALL_STATEMENT_NODE_NAME = "org.apache.derby.impl.sql.compile.CallStatementNode";

	static final String CAST_NODE_NAME = "org.apache.derby.impl.sql.compile.CastNode";

	static final String CHAR_CONSTANT_NODE_NAME = "org.apache.derby.impl.sql.compile.CharConstantNode";

	static final String COALESCE_FUNCTION_NODE_NAME = "org.apache.derby.impl.sql.compile.CoalesceFunctionNode";

	static final String COLUMN_DEFINITION_NODE_NAME = "org.apache.derby.impl.sql.compile.ColumnDefinitionNode";

	static final String COLUMN_REFERENCE_NAME = "org.apache.derby.impl.sql.compile.ColumnReference";

	static final String CONCATENATION_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.ConcatenationOperatorNode";

	static final String CONDITIONAL_NODE_NAME = "org.apache.derby.impl.sql.compile.ConditionalNode";

	static final String CONSTRAINT_DEFINITION_NODE_NAME = "org.apache.derby.impl.sql.compile.ConstraintDefinitionNode";

	static final String CREATE_ALIAS_NODE_NAME = "org.apache.derby.impl.sql.compile.CreateAliasNode";

	static final String CREATE_INDEX_NODE_NAME = "org.apache.derby.impl.sql.compile.CreateIndexNode";

	static final String CREATE_SCHEMA_NODE_NAME = "org.apache.derby.impl.sql.compile.CreateSchemaNode";

	static final String CREATE_TABLE_NODE_NAME = "org.apache.derby.impl.sql.compile.CreateTableNode";

	static final String CREATE_TRIGGER_NODE_NAME = "org.apache.derby.impl.sql.compile.CreateTriggerNode";

	static final String CREATE_VIEW_NODE_NAME = "org.apache.derby.impl.sql.compile.CreateViewNode";

	static final String CURRENT_DATETIME_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.CurrentDatetimeOperatorNode";

	static final String CURRENT_ISOLATION_NAME = "org.apache.derby.impl.sql.compile.CurrentIsolationNode";

	static final String CURRENT_OF_NODE_NAME = "org.apache.derby.impl.sql.compile.CurrentOfNode";

	static final String CURRENT_ROW_LOCATION_NODE_NAME = "org.apache.derby.impl.sql.compile.CurrentRowLocationNode";

	static final String CURRENT_USER_NODE_NAME = "org.apache.derby.impl.sql.compile.CurrentUserNode";

	static final String CURSOR_NODE_NAME = "org.apache.derby.impl.sql.compile.CursorNode";

	static final String DB2_LENGTH_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.DB2LengthOperatorNode";

	static final String DML_MOD_STATEMENT_NODE_NAME = "org.apache.derby.impl.sql.compile.DMLModStatementNode";

	static final String DEFAULT_NODE_NAME = "org.apache.derby.impl.sql.compile.DefaultNode";

	static final String DELETE_NODE_NAME = "org.apache.derby.impl.sql.compile.DeleteNode";

	static final String DISTINCT_NODE_NAME = "org.apache.derby.impl.sql.compile.DistinctNode";

	static final String DROP_ALIAS_NODE_NAME = "org.apache.derby.impl.sql.compile.DropAliasNode";

	static final String DROP_INDEX_NODE_NAME = "org.apache.derby.impl.sql.compile.DropIndexNode";

	static final String DROP_SPS_NODE_NAME = "org.apache.derby.impl.sql.compile.DropSPSNode";

	static final String DROP_SCHEMA_NODE_NAME = "org.apache.derby.impl.sql.compile.DropSchemaNode";

	static final String DROP_TABLE_NODE_NAME = "org.apache.derby.impl.sql.compile.DropTableNode";

	static final String DROP_TRIGGER_NODE_NAME = "org.apache.derby.impl.sql.compile.DropTriggerNode";

	static final String DROP_VIEW_NODE_NAME = "org.apache.derby.impl.sql.compile.DropViewNode";

	static final String EXEC_SPS_NODE_NAME = "org.apache.derby.impl.sql.compile.ExecSPSNode";

	static final String EXTRACT_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.ExtractOperatorNode";

	static final String FK_CONSTRAINT_DEFINITION_NODE_NAME = "org.apache.derby.impl.sql.compile.FKConstraintDefinitionNode";

	static final String FROM_BASE_TABLE_NAME = "org.apache.derby.impl.sql.compile.FromBaseTable";

	static final String FROM_LIST_NAME = "org.apache.derby.impl.sql.compile.FromList";

	static final String FROM_SUBQUERY_NAME = "org.apache.derby.impl.sql.compile.FromSubquery";

	static final String FROM_VTI_NAME = "org.apache.derby.impl.sql.compile.FromVTI";

	static final String GET_CURRENT_CONNECTION_NODE_NAME = "org.apache.derby.impl.sql.compile.GetCurrentConnectionNode";

	static final String GROUP_BY_COLUMN_NAME = "org.apache.derby.impl.sql.compile.GroupByColumn";

	static final String GROUP_BY_LIST_NAME = "org.apache.derby.impl.sql.compile.GroupByList";

	static final String GROUP_BY_NODE_NAME = "org.apache.derby.impl.sql.compile.GroupByNode";

	static final String HALF_OUTER_JOIN_NODE_NAME = "org.apache.derby.impl.sql.compile.HalfOuterJoinNode";

	static final String HASH_TABLE_NODE_NAME = "org.apache.derby.impl.sql.compile.HashTableNode";

	static final String IN_LIST_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.InListOperatorNode";

	static final String INDEX_TO_BASE_ROW_NODE_NAME = "org.apache.derby.impl.sql.compile.IndexToBaseRowNode";

	static final String INSERT_NODE_NAME = "org.apache.derby.impl.sql.compile.InsertNode";

	static final String IS_NODE_NAME = "org.apache.derby.impl.sql.compile.IsNode";

	static final String IS_NULL_NODE_NAME = "org.apache.derby.impl.sql.compile.IsNullNode";

	static final String JAVA_TO_SQL_VALUE_NODE_NAME = "org.apache.derby.impl.sql.compile.JavaToSQLValueNode";

	static final String JOIN_NODE_NAME = "org.apache.derby.impl.sql.compile.JoinNode";

	static final String LENGTH_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.LengthOperatorNode";

	static final String LIKE_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.LikeEscapeOperatorNode";

	static final String LOCK_TABLE_NODE_NAME = "org.apache.derby.impl.sql.compile.LockTableNode";

	static final String MATERIALIZE_RESULT_SET_NODE_NAME = "org.apache.derby.impl.sql.compile.MaterializeResultSetNode";

	static final String MODIFY_COLUMN_NODE_NAME = "org.apache.derby.impl.sql.compile.ModifyColumnNode";

	static final String NOP_STATEMENT_NODE_NAME = "org.apache.derby.impl.sql.compile.NOPStatementNode";

	static final String NEW_INVOCATION_NODE_NAME = "org.apache.derby.impl.sql.compile.NewInvocationNode";

	static final String NON_STATIC_METHOD_CALL_NODE_NAME = "org.apache.derby.impl.sql.compile.NonStaticMethodCallNode";

	static final String NORMALIZE_RESULT_SET_NODE_NAME = "org.apache.derby.impl.sql.compile.NormalizeResultSetNode";

	static final String NOT_NODE_NAME = "org.apache.derby.impl.sql.compile.NotNode";

	static final String NUMERIC_CONSTANT_NODE_NAME = "org.apache.derby.impl.sql.compile.NumericConstantNode";

	static final String OR_NODE_NAME = "org.apache.derby.impl.sql.compile.OrNode";

	static final String ORDER_BY_COLUMN_NAME = "org.apache.derby.impl.sql.compile.OrderByColumn";

	static final String ORDER_BY_LIST_NAME = "org.apache.derby.impl.sql.compile.OrderByList";

	static final String ORDER_BY_NODE_NAME = "org.apache.derby.impl.sql.compile.OrderByNode";

	static final String PARAMETER_NODE_NAME = "org.apache.derby.impl.sql.compile.ParameterNode";

	static final String PREDICATE_NAME = "org.apache.derby.impl.sql.compile.Predicate";

	static final String PREDICATE_LIST_NAME = "org.apache.derby.impl.sql.compile.PredicateList";

	static final String PROJECT_RESTRICT_NODE_NAME = "org.apache.derby.impl.sql.compile.ProjectRestrictNode";

	static final String READ_CURSOR_NODE_NAME = "org.apache.derby.impl.sql.compile.ReadCursorNode";

	static final String RENAME_NODE_NAME = "org.apache.derby.impl.sql.compile.RenameNode";

	static final String RESULT_COLUMN_NAME = "org.apache.derby.impl.sql.compile.ResultColumn";

	static final String RESULT_COLUMN_LIST_NAME = "org.apache.derby.impl.sql.compile.ResultColumnList";

	static final String ROW_RESULT_SET_NODE_NAME = "org.apache.derby.impl.sql.compile.RowResultSetNode";

	static final String SQL_BOOLEAN_CONSTANT_NODE_NAME = "org.apache.derby.impl.sql.compile.SQLBooleanConstantNode";

	static final String SQL_TO_JAVA_VALUE_NODE_NAME = "org.apache.derby.impl.sql.compile.SQLToJavaValueNode";

	static final String SCROLL_INSENSITIVE_RESULT_SET_NODE_NAME = "org.apache.derby.impl.sql.compile.ScrollInsensitiveResultSetNode";

	static final String SELECT_NODE_NAME = "org.apache.derby.impl.sql.compile.SelectNode";

	static final String SET_SCHEMA_NODE_NAME = "org.apache.derby.impl.sql.compile.SetSchemaNode";

	static final String SET_TRANSACTION_ISOLATION_NODE_NAME = "org.apache.derby.impl.sql.compile.SetTransactionIsolationNode";

	static final String SET_TRIGGERS_NODE_NAME = "org.apache.derby.impl.sql.compile.SetTriggersNode";

	static final String SIMPLE_STRING_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.SimpleStringOperatorNode";

	static final String STATIC_CLASS_FIELD_REFERENCE_NODE_NAME = "org.apache.derby.impl.sql.compile.StaticClassFieldReferenceNode";

	static final String STATIC_METHOD_CALL_NODE_NAME = "org.apache.derby.impl.sql.compile.StaticMethodCallNode";

	static final String SUBQUERY_LIST_NAME = "org.apache.derby.impl.sql.compile.SubqueryList";

	static final String SUBQUERY_NODE_NAME = "org.apache.derby.impl.sql.compile.SubqueryNode";

	static final String TABLE_ELEMENT_LIST_NAME = "org.apache.derby.impl.sql.compile.TableElementList";

	static final String TABLE_ELEMENT_NODE_NAME = "org.apache.derby.impl.sql.compile.TableElementNode";

	static final String TABLE_NAME_NAME = "org.apache.derby.impl.sql.compile.TableName";

	static final String TERNARY_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.TernaryOperatorNode";

	static final String TEST_CONSTRAINT_NODE_NAME = "org.apache.derby.impl.sql.compile.TestConstraintNode";

	static final String TIMESTAMP_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.TimestampOperatorNode";

	static final String UNARY_ARITHMETIC_OPERATOR_NODE_NAME = "org.apache.derby.impl.sql.compile.UnaryArithmeticOperatorNode";

	static final String UNION_NODE_NAME = "org.apache.derby.impl.sql.compile.UnionNode";

	static final String UNTYPED_NULL_CONSTANT_NODE_NAME = "org.apache.derby.impl.sql.compile.UntypedNullConstantNode";

	static final String UPDATE_NODE_NAME = "org.apache.derby.impl.sql.compile.UpdateNode";

	static final String USERTYPE_CONSTANT_NODE_NAME = "org.apache.derby.impl.sql.compile.UserTypeConstantNode";

	static final String VALUE_NODE_LIST_NAME = "org.apache.derby.impl.sql.compile.ValueNodeList";

	static final String VARBIT_CONSTANT_NODE_NAME = "org.apache.derby.impl.sql.compile.VarbitConstantNode";

	static final String VIRTUAL_COLUMN_NODE_NAME = "org.apache.derby.impl.sql.compile.VirtualColumnNode";

	static final String SAVEPOINT_NODE_NAME = "org.apache.derby.impl.sql.compile.SavepointNode";

	// The names are in alphabetic order.
	//
    // WARNING: WHEN ADDING NODE TYPES HERE, YOU MUST ALSO ADD
    // THEM TO $WS/tools/release/config/dbms/cloudscapenodes.properties

}
