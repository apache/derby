/*

   Derby - Class org.apache.derby.iapi.sql.compile.C_NodeTypes

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

package org.apache.derby.iapi.sql.compile;

/**
 * The purpose of this interface is to hold the constant definitions
 * of the different node type identifiers, for use with NodeFactory.
 * The reason this class exists is that it is not shipped with the
 * product, so it saves footprint to have all these constant definitions
 * here instead of in NodeFactory.
 */
public interface C_NodeTypes
{
	/** Node types, for use with getNode methods */
	static final int TEST_CONSTRAINT_NODE = 1;
	static final int CURRENT_ROW_LOCATION_NODE = 2;
	static final int GROUP_BY_LIST = 3;
    static final int CURRENT_ISOLATION_NODE = 4;
	static final int IDENTITY_VAL_NODE = 5;
	static final int CURRENT_SCHEMA_NODE = 6;
	static final int ORDER_BY_LIST = 7;
	static final int PREDICATE_LIST = 8;
	static final int RESULT_COLUMN_LIST = 9;
	// 10 available
	static final int SUBQUERY_LIST = 11;
	static final int TABLE_ELEMENT_LIST = 12;
	static final int UNTYPED_NULL_CONSTANT_NODE = 13;
	static final int TABLE_ELEMENT_NODE = 14;
	static final int VALUE_NODE_LIST = 15;
	static final int ALL_RESULT_COLUMN = 16;
	// 17 is available
	static final int GET_CURRENT_CONNECTION_NODE = 18;
	static final int NOP_STATEMENT_NODE = 19;
	static final int DB2_LENGTH_OPERATOR_NODE = 20;
	static final int SET_TRANSACTION_ISOLATION_NODE = 21;
	// 22 is available
	static final int CHAR_LENGTH_OPERATOR_NODE = 23;
	static final int IS_NOT_NULL_NODE = 24;
	static final int IS_NULL_NODE = 25;
	static final int NOT_NODE = 26;
	// 27 is available
	static final int SQL_TO_JAVA_VALUE_NODE = 28;
	static final int UNARY_MINUS_OPERATOR_NODE = 29;
	static final int UNARY_PLUS_OPERATOR_NODE = 30;
   // 31 is available
	static final int UNARY_DATE_TIMESTAMP_OPERATOR_NODE = 32;
	static final int TIMESTAMP_OPERATOR_NODE = 33;
	static final int TABLE_NAME = 34;
	static final int GROUP_BY_COLUMN = 35;
	static final int JAVA_TO_SQL_VALUE_NODE = 36;
	static final int FROM_LIST = 37;
	static final int BOOLEAN_CONSTANT_NODE = 38;
	static final int AND_NODE = 39;
	static final int BINARY_DIVIDE_OPERATOR_NODE = 40;
	static final int BINARY_EQUALS_OPERATOR_NODE = 41;
	static final int BINARY_GREATER_EQUALS_OPERATOR_NODE = 42;
	static final int BINARY_GREATER_THAN_OPERATOR_NODE = 43;
	static final int BINARY_LESS_EQUALS_OPERATOR_NODE = 44;
	static final int BINARY_LESS_THAN_OPERATOR_NODE = 45;
	static final int BINARY_MINUS_OPERATOR_NODE = 46;
	static final int BINARY_NOT_EQUALS_OPERATOR_NODE = 47;
	static final int BINARY_PLUS_OPERATOR_NODE = 48;
	static final int BINARY_TIMES_OPERATOR_NODE = 49;
	static final int CONCATENATION_OPERATOR_NODE = 50;
    static final int LIKE_ESCAPE_OPERATOR_NODE = 51;
	static final int OR_NODE = 52;
	static final int BETWEEN_OPERATOR_NODE = 53;
	static final int CONDITIONAL_NODE = 54;
	static final int IN_LIST_OPERATOR_NODE = 55;
	static final int NOT_BETWEEN_OPERATOR_NODE = 56;
	static final int NOT_IN_LIST_OPERATOR_NODE = 57;
	static final int BIT_CONSTANT_NODE = 58;
	static final int VARBIT_CONSTANT_NODE = 59;
	static final int CAST_NODE = 60;
	static final int CHAR_CONSTANT_NODE = 61;
	static final int COLUMN_REFERENCE = 62;
	static final int DROP_INDEX_NODE = 63;
	// 64 available;
	static final int DROP_TRIGGER_NODE = 65;
	// 66 available;
	static final int DECIMAL_CONSTANT_NODE = 67;
	static final int DOUBLE_CONSTANT_NODE = 68;
    static final int REAL_CONSTANT_NODE = 69;
	static final int INT_CONSTANT_NODE = 70;
    static final int BIGINT_CONSTANT_NODE = 71;
	static final int LONGVARBIT_CONSTANT_NODE = 72;
	static final int LONGVARCHAR_CONSTANT_NODE = 73;
	static final int SMALLINT_CONSTANT_NODE = 74;
	static final int TINYINT_CONSTANT_NODE = 75;
	static final int USERTYPE_CONSTANT_NODE = 76;
	static final int VARCHAR_CONSTANT_NODE = 77;
	static final int PREDICATE = 78;
	// 79 available
	static final int RESULT_COLUMN = 80;
	static final int SET_SCHEMA_NODE = 81;
	static final int UPDATE_COLUMN = 82;
	static final int SIMPLE_STRING_OPERATOR_NODE = 83;
	static final int STATIC_CLASS_FIELD_REFERENCE_NODE = 84;
	static final int STATIC_METHOD_CALL_NODE = 85;
	static final int REVOKE_NODE = 86;
	static final int EXTRACT_OPERATOR_NODE = 87;
	static final int PARAMETER_NODE = 88;
	static final int GRANT_NODE = 89;
	static final int DROP_SCHEMA_NODE = 90;
	static final int DROP_TABLE_NODE = 91;
	static final int DROP_VIEW_NODE = 92;
	static final int SUBQUERY_NODE = 93;
	static final int BASE_COLUMN_NODE = 94;
	static final int CALL_STATEMENT_NODE = 95;
	static final int MODIFY_COLUMN_DEFAULT_NODE = 97;
	static final int NON_STATIC_METHOD_CALL_NODE = 98;
	static final int CURRENT_OF_NODE = 99;
	static final int DEFAULT_NODE = 100;
	static final int DELETE_NODE = 101;
	static final int UPDATE_NODE = 102;
	static final int PRIVILEGE_NODE = 103;
	static final int ORDER_BY_COLUMN = 104;
	static final int ROW_RESULT_SET_NODE = 105;
	static final int TABLE_PRIVILEGES_NODE = 106;
	static final int VIRTUAL_COLUMN_NODE = 107;
	static final int CURRENT_DATETIME_OPERATOR_NODE = 108;
	static final int CURRENT_USER_NODE = 109; // special function CURRENT_USER
	static final int USER_NODE = 110; // // special function USER
	static final int IS_NODE = 111;
	static final int LOCK_TABLE_NODE = 112;
	static final int DROP_COLUMN_NODE = 113;
	static final int ALTER_TABLE_NODE = 114;
	static final int AGGREGATE_NODE = 115;
	static final int COLUMN_DEFINITION_NODE = 116;
	// 117 is available
	static final int EXEC_SPS_NODE = 118;
	static final int FK_CONSTRAINT_DEFINITION_NODE = 119;
	static final int FROM_VTI = 120;
	static final int MATERIALIZE_RESULT_SET_NODE = 121;
	static final int NORMALIZE_RESULT_SET_NODE = 122;
	static final int SCROLL_INSENSITIVE_RESULT_SET_NODE = 123;
	static final int DISTINCT_NODE = 124;
	static final int SESSION_USER_NODE = 125; // // special function SESSION_USER
	static final int SYSTEM_USER_NODE = 126; // // special function SYSTEM_USER
	static final int TRIM_OPERATOR_NODE = 127;
	// 128 is available
	static final int SELECT_NODE = 129;
	static final int CREATE_VIEW_NODE = 130;
	static final int CONSTRAINT_DEFINITION_NODE = 131;
	// 132 available;
	static final int NEW_INVOCATION_NODE = 133;
	static final int CREATE_SCHEMA_NODE = 134;
	static final int FROM_BASE_TABLE = 135;
	static final int FROM_SUBQUERY = 136;
	static final int GROUP_BY_NODE = 137;
	static final int INSERT_NODE = 138;
	static final int JOIN_NODE = 139;
	static final int ORDER_BY_NODE = 140;
	static final int CREATE_TABLE_NODE = 141;
	static final int UNION_NODE = 142;
	static final int CREATE_TRIGGER_NODE = 143;
	static final int HALF_OUTER_JOIN_NODE = 144;
// UNUSED	static final int CREATE_SPS_NODE = 145;
	static final int CREATE_INDEX_NODE = 146;
	static final int CURSOR_NODE = 147;
	static final int HASH_TABLE_NODE = 148;
	static final int INDEX_TO_BASE_ROW_NODE = 149;
	static final int CREATE_ALIAS_NODE = 150;
	static final int PROJECT_RESTRICT_NODE = 151;
	// UNUSED static final int BOOLEAN_TRUE_NODE = 152;
	// UNUSED static final int BOOLEAN_FALSE_NODE = 153;
	static final int SUBSTRING_OPERATOR_NODE = 154;
	// UNUSED static final int BOOLEAN_NODE = 155;
	static final int DROP_ALIAS_NODE = 156;
    static final int INTERSECT_OR_EXCEPT_NODE = 157;
	// 158 - 183 available
    static final int TIMESTAMP_ADD_FN_NODE = 184;
    static final int TIMESTAMP_DIFF_FN_NODE = 185;
	static final int MODIFY_COLUMN_TYPE_NODE = 186;
	static final int MODIFY_COLUMN_CONSTRAINT_NODE = 187;
    static final int ABSOLUTE_OPERATOR_NODE = 188;
    static final int SQRT_OPERATOR_NODE = 189;
    static final int LOCATE_FUNCTION_NODE = 190;
  //for rename table/column/index
	static final int RENAME_NODE = 191;

	static final int COALESCE_FUNCTION_NODE = 192;

	static final int MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE = 193;

	static final int MOD_OPERATOR_NODE = 194;
    // LOB
    static final int BLOB_CONSTANT_NODE = 195;
    static final int CLOB_CONSTANT_NODE = 196;
    //static final int NCLOB_CONSTANT_NODE = 197;
    // for SAVEPOINT sql
    static final int SAVEPOINT_NODE = 198;

    // XML
    static final int XML_CONSTANT_NODE = 199;
    static final int XML_PARSE_OPERATOR_NODE = 200;
    static final int XML_SERIALIZE_OPERATOR_NODE = 201;
    static final int XML_EXISTS_OPERATOR_NODE = 202;
    static final int XML_QUERY_OPERATOR_NODE = 203;

    // Roles
    static final int CURRENT_ROLE_NODE = 210;
    static final int CREATE_ROLE_NODE = 211;
    static final int SET_ROLE_NODE = 212;
    static final int SET_ROLE_DYNAMIC = 213;
    static final int DROP_ROLE_NODE = 214;
    static final int GRANT_ROLE_NODE = 215;
    static final int REVOKE_ROLE_NODE = 216;

    // generated columns
    static final int GENERATION_CLAUSE_NODE = 222;

	// OFFSET, FETCH FIRST node
	static final int ROW_COUNT_NODE = 223;

    // sequences
    static final int CREATE_SEQUENCE_NODE = 224;
    static final int DROP_SEQUENCE_NODE = 225;
    static final int NEXT_SEQUENCE_NODE = 231;

	// Windowing
	static final int AGGREGATE_WINDOW_FUNCTION_NODE = 226;
	static final int ROW_NUMBER_FUNCTION_NODE = 227;
	static final int WINDOW_DEFINITION_NODE = 228;
	static final int WINDOW_REFERENCE_NODE = 229;
	static final int WINDOW_RESULTSET_NODE = 230;

    // Final value in set, keep up to date!
    static final int FINAL_VALUE = NEXT_SEQUENCE_NODE;

    /**
     * Extensions to this interface can use nodetypes &gt; MAX_NODE_TYPE with out fear of collision
     * with C_NodeTypes
     */
    static final int MAX_NODE_TYPE = 999;
}
