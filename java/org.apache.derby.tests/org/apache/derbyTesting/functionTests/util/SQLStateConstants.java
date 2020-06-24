/*
    Derby - Class org.apache.derbyTesting.functionTests.util.SQLState

    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at
     
    http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
   
*/
package org.apache.derbyTesting.functionTests.util;
    
/**
 * This contains constants for all the standard SQL states as well as
 * for those that are specific to Derby that our tests compare against
 * to make sure the right error is thrown.
 *
 * It is important to use these constants rather than those in
 * org.apache.derby.shared.common.reference.SQLState.java because
 * (a) that class is not part of the public API and (b) that class contains
 * message ids, not SQL states.
*/
public class SQLStateConstants
{
    // ==== STANDARD SQL STATES =====
    // These are derived from the ISO SQL2003 specification
    // INCITS-ISO-IEC-9075-2-2003
    //
    public static final String AMBIGUOUS_CURSOR_NAME_NO_SUBCLASS 
        = "3C000";
    public static final String ATTEMPT_TO_ASSIGN_TO_NON_UPDATABLE_COLUMN_NO_SUBCLASS 
        = "0U000";
    public static final String ATTEMPT_TO_ASSIGN_TO_ORDERING_COLUMN_NO_SUBCLASS 
        = "0V000";
    public static final String CARDINALITY_VIOLATION_NO_SUBCLASS 
        = "21000";
    public static final String CLI_SPECIFIC_CONDITION_NO_SUBCLASS 
        = "HY000";
    public static final String CONNECTION_EXCEPTION_NO_SUBCLASS 
        = "08000";
    public static final String CONNECTION_EXCEPTION_CONNECTION_DOES_NOT_EXIST 
        = "08003";
    public static final String CONNECTION_EXCEPTION_CONNECTION_FAILURE 
        = "08006";
    public static final String CONNECTION_EXCEPTION_CONNECTION_NAME_IN_USE 
        = "08002";
    public static final String CONNECTION_EXCEPTION_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION 
        = "08001";
    public static final String CONNECTION_EXCEPTION_SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION 
        = "08004";
    public static final String CONNECTION_EXCEPTION_TRANSACTION_RESOLUTION_UNKNOWN 
        = "08007";
    public static final String DATA_EXCEPTION_NO_SUBCLASS 
        = "22000";
    public static final String DATA_EXCEPTION_ARRAY_ELEMENT_ERROR
        = "2202E";
    public static final String DATA_EXCEPTION_CHARACTER_NOT_IN_REPERTOIRE 
        = "22021";
    public static final String DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW 
        = "22008";
    public static final String DATA_EXCEPTION_DIVISION_BY_ZERO 
        = "22012";
    public static final String DATA_EXCEPTION_ERROR_IN_ASSIGNMENT 
        = "22005";
    public static final String DATA_EXCEPTION_ESCAPE_CHARACTER_CONFLICT 
        = "2200B";
    public static final String DATA_EXCEPTION_INDICATOR_OVERFLOW 
        = "22022";
    public static final String DATA_EXCEPTION_INTERVAL_FIELD_OVERFLOW 
        = "22015";
    public static final String DATA_EXCEPTION_INTERVAL_VALUE_OUT_OF_RANGE 
        = "2200P";
    public static final String DATA_EXCEPTION_INVALID_ARGUMENT_FOR_NATURAL_LOGARITHM 
        = "2201E";
    public static final String DATA_EXCEPTION_INVALID_ARGUMENT_FOR_POWER_FUNCTION 
        = "2201F";
    public static final String DATA_EXCEPTION_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION 
        = "2201G";
    public static final String DATA_EXCEPTION_INVALID_CHARACTER_VALUE_FOR_CAST 
        = "22018";
    public static final String DATA_EXCEPTION_INVALID_DATETIME_FORMAT 
        = "22007";
    public static final String DATA_EXCEPTION_INVALID_ESCAPE_CHARACTER 
        = "22019";
    public static final String DATA_EXCEPTION_INVALID_ESCAPE_OCTET
        = "2200D";
    public static final String DATA_EXCEPTION_INVALID_ESCAPE_SEQUENCE 
        = "22025";
    public static final String DATA_EXCEPTION_INVALID_INDICATOR_PARAMETER_VALUE 
        = "22010";
    public static final String DATA_EXCEPTION_INVALID_INTERVAL_FORMAT 
        = "22006";
    public static final String DATA_EXCEPTION_INVALID_PARAMETER_VALUE 
        = "22023";


    // Derby uses 22013 for something else, cf. SQLState.LANG_SQRT_OF_NEG_NUMBER
    // public static final String DATA_EXCEPTION_INVALID_PRECEDING_OR_FOLLOWING_SIZE_IN_WINDOW_FUNCTION
    //    = "22013";

    public static final String DATA_EXCEPTION_INVALID_REGULAR_EXPRESSION 
        = "2201B";
    public static final String DATA_EXCEPTION_INVALID_REPEAT_ARGUMENT_IN_A_SAMPLE_CLAUSE 
        = "2202G";
    public static final String DATA_EXCEPTION_INVALID_SAMPLE_SIZE 
        = "2202H";
    public static final String DATA_EXCEPTION_INVALID_TIME_ZONE_DISPLACEMENT_VALUE 
        = "22009";
    public static final String DATA_EXCEPTION_INVALID_USE_OF_ESCAPE_CHARACTER 
        = "2200C";
    public static final String DATA_EXCEPTION_NULL_VALUE_NO_INDICATOR_PARAMETER
        = "2200G";
    public static final String DATA_EXCEPTION_MOST_SPECIFIC_TYPE_MISMATCH 
        = "22002";
    public static final String DATA_EXCEPTION_MULTISET_VALUE_OVERFLOW 
        = "2200Q";
    public static final String DATA_EXCEPTION_NONCHARACTER_IN_UCS_STRING 
        = "22029";
    public static final String DATA_EXCEPTION_NULL_VALUE_NOT_ALLOWED 
        = "22004";
    public static final String DATA_EXCEPTION_NULL_VALUE_SUBSTITUTED_FOR_MUTATOR_SUBJECT_PARAMETER 
        = "2202D";
    public static final String DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE 
        = "22003";
    public static final String DATA_EXCEPTION_SEQUENCE_GENERATOR_LIMIT_EXCEEDED 
        = "2200H";
    public static final String DATA_EXCEPTION_STRING_DATA_LENGTH_MISMATCH 
        = "22026";
    public static final String DATA_EXCEPTION_STRING_DATA_RIGHT_TRUNCATION 
        = "22001";
    public static final String DATA_EXCEPTION_SUBSTRING_ERROR 
        = "22011";
    public static final String DATA_EXCEPTION_TRIM_ERROR 
        = "22027";
    public static final String DATA_EXCEPTION_UNTERMINATED_C_STRING 
        = "22024";
    public static final String DATA_EXCEPTION_ZERO_LENGTH_CHARACTER_STRING 
        = "2200F";
    public static final String DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST_NO_SUBCLASS 
        = "2B000";
    public static final String DIAGNOSTICS_EXCEPTION_NO_SUBCLASS 
        = "0Z000";
    public static final String DIAGNOSTICS_EXCEPTION_MAXIMUM_NUMBER_OF_DIAGNOSTICS_AREAS_EXCEEDED 
        = "0Z001";
    public static final String DYNAMIC_SQL_ERROR_NO_SUBCLASS 
        = "07000";
    public static final String DYNAMIC_SQL_ERROR_CURSOR_SPECIFICATION_CANNOT_BE_EXECUTED 
        = "07003";
    public static final String DYNAMIC_SQL_ERROR_INVALID_DATETIME_INTERVAL_CODE 
        = "0700F";
    public static final String DYNAMIC_SQL_ERROR_INVALID_DESCRIPTOR_COUNT 
        = "07008";
    public static final String DYNAMIC_SQL_ERROR_INVALID_DESCRIPTOR_INDEX 
        = "07009";
    public static final String DYNAMIC_SQL_ERROR_PREPARED_STATEMENT_NOT_A_CURSOR_SPECIFICATION 
        = "07005";
    public static final String DYNAMIC_SQL_ERROR_RESTRICTED_DATA_TYPE_ATTRIBUTE_VIOLATION 
        = "07006";
    public static final String DYNAMIC_SQL_ERROR_DATA_TYPE_TRANSFORM_FUNCTION_VIOLATION 
        = "0700B";
    public static final String DYNAMIC_SQL_ERROR_INVALID_DATA_TARGET 
        = "0700D";
    public static final String DYNAMIC_SQL_ERROR_INVALID_LEVEL_VALUE 
        = "0700E";
    public static final String DYNAMIC_SQL_ERROR_UNDEFINED_DATA_VALUE 
        = "0700C";
    public static final String DYNAMIC_SQL_ERROR_USING_CLAUSE_DOES_NOT_MATCH_DYNAMIC_PARAMETER_SPEC 
        = "07001";
    public static final String DYNAMIC_SQL_ERROR_USING_CLAUSE_DOES_NOT_MATCH_TARGET_SPEC 
        = "07002";
    public static final String DYNAMIC_SQL_ERROR_USING_CLAUSE_REQUIRED_FOR_DYNAMIC_PARAMETERS 
        = "07004";
    public static final String DYNAMIC_SQL_ERROR_USING_CLAUSE_REQUIRED_FOR_RESULT_FIELDS 
        = "07007";
    public static final String EXTERNAL_ROUTINE_EXCEPTION_NO_SUBCLASS 
        = "38000";
    public static final String EXTERNAL_ROUTINE_EXCEPTION_CONTAINING_SQL_NOT_PERMITTED 
        = "38001";
    public static final String EXTERNAL_ROUTINE_EXCEPTION_MODIFYING_SQL_DATA_NOT_PERMITTED 
        = "38002";
    public static final String EXTERNAL_ROUTINE_EXCEPTION_PROHIBITED_SQL_STATEMENT_ATTEMPTED 
        = "38003";
    public static final String EXTERNAL_ROUTINE_EXCEPTION_READING_SQL_DATA_NOT_PERMITTED 
        = "38004";
    public static final String EXTERNAL_ROUTINE_INVOCATION_EXCEPTION_NO_SUBCLASS 
        = "39000";
    public static final String EXTERNAL_ROUTINE_INVOCATION_EXCEPTION_NULL_VALUE_NOT_ALLOWED 
        = "39004";
    public static final String FEATURE_NOT_SUPPORTED_NO_SUBCLASS 
        = "0A000";
    public static final String FEATURE_NOT_SUPPORTED_MULTIPLE_ENVIRONMENT_TRANSACTIONS 
        = "0A001";
    public static final String INTEGRITY_CONSTRAINT_VIOLATION_NO_SUBCLASS 
        = "23000";
    public static final String INTEGRITY_CONSTRAINT_VIOLATION_RESTRICT_VIOLATION 
        = "23001";
    public static final String INVALID_AUTHORIZATION_SPECIFICATION_NO_SUBCLASS 
        = "28000";
    public static final String INVALID_CATALOG_NAME_NO_SUBCLASS 
        = "3D000";
    public static final String INVALID_CHARACTER_SET_NAME_NO_SUBCLASS 
        = "2C000";
    public static final String INVALID_COLLATION_NAME_NO_SUBCLASS 
        = "2H000";
    public static final String INVALID_CONDITION_NUMBER_NO_SUBCLASS 
        = "35000";
    public static final String INVALID_CONNECTION_NAME_NO_SUBCLASS 
        = "2E000";
    public static final String INVALID_CURSOR_NAME_NO_SUBCLASS 
        = "34000";
    public static final String INVALID_CURSOR_STATE_NO_SUBCLASS 
        = "24000";
    public static final String INVALID_GRANTOR_STATE_NO_SUBCLASS 
        = "0L000";
    public static final String INVALID_ROLE_SPECIFICATION 
        = "0P000";
    public static final String INVALID_SCHEMA_NAME_NO_SUBCLASS 
        = "3F000";
    public static final String INVALID_SCHEMA_NAME_LIST_SPECIFICATION_NO_SUBCLASS 
        = "0E000";
    public static final String INVALID_SQL_DESCRIPTOR_NAME_NO_SUBCLASS 
        = "33000";
    public static final String INVALID_SQL_INVOKED_PROCEDURE_REFERENCE_NO_SUBCLASS 
        = "0M000";
    public static final String INVALID_SQL_STATEMENT 
        = "30000";
    public static final String INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS 
        = "30000";
    public static final String INVALID_SQL_STATEMENT_NAME_NO_SUBCLASS 
        = "26000";
    public static final String INVALID_TRANSFORM_GROUP_NAME_SPECIFICATION_NO_SUBCLASS 
        = "0S000";
    public static final String INVALID_TRANSACTION_STATE_ACTIVE_SQL_TRANSACTION 
        = "25001";
    public static final String INVALID_TRANSACTION_STATE_BRANCH_TRANSACTION_ALREADY_ACTIVE 
        = "25002";
    public static final String INVALID_TRANSACTION_STATE_HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL 
        = "25008";
    public static final String INVALID_TRANSACTION_STATE_INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION 
        = "25003";
    public static final String INVALID_TRANSACTION_STATE_INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION 
        = "25004";
    public static final String INVALID_TRANSACTION_STATE_NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION 
        = "25005";
    public static final String INVALID_TRANSACTION_STATE_READ_ONLY_SQL_TRANSACTION 
        = "25006";
    public static final String INVALID_TRANSACTION_STATE_SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED 
        = "25007";
    public static final String INVALID_TRANSACTION_INITIATION_NO_SUBCLASS 
        = "0B000";
    public static final String INVALID_TRANSACTION_TERMINATION_NO_SUBCLASS 
        = "2D000";
    public static final String LOCATOR_EXCEPTION_INVALID_SPECIFICATION 
        = "0F001";
    public static final String LOCATOR_EXCEPTION_NO_SUBCLASS 
        = "0F000";
    public static final String NO_DATA_NO_SUBCLASS 
        = "02000";
    public static final String NO_DATA_NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED 
        = "02001";
    public static final String REMOTE_DATABASE_ACCESS_NO_SUBCLASS 
        = "HZ000";
    public static final String SAVEPOINT_EXCEPTION_INVALID_SPECIFICATION 
        = "3B001";
    public static final String SAVEPOINT_EXCEPTION_NO_SUBCLASS 
        = "3B000";
    public static final String SAVEPOINT_EXCEPTION_TOO_MANY 
        = "3B002";
    public static final String SQL_ROUTINE_EXCEPTION_NO_SUBCLASS 
        = "2F000";
    public static final String SQL_ROUTINE_EXCEPTION_FUNCTION_EXECUTED_NO_RETURN_STATEMENT 
        = "2F005";
    public static final String SQL_ROUTINE_EXCEPTION_MODIFYING_SQL_DATA_NOT_PERMITTED 
        = "2F002";
    public static final String SQL_ROUTINE_EXCEPTION_PROHIBITED_SQL_STATEMENT_ATTEMPTED 
        = "2F003";
    public static final String SQL_ROUTINE_EXCEPTION_READING_SQL_DATA_NOT_PERMITTED 
        = "2F004";
    public static final String SUCCESSFUL_COMPLETION_NO_SUBCLASS 
        = "00000";
    public static final String SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_NO_SUBCLASS 
        = "42000";
    public static final String SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_IN_DIRECT_STATEMENT_NO_SUBCLASS 
        = "2A000";
    public static final String SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_IN_DYNAMIC_STATEMENT_NO_SUBCLASS 
        = "37000";
    public static final String TARGET_TABLE_DISAGREES_WITH_CURSOR_SPECIFICATION_NO_SUBCLASS 
        = "0T000";
    public static final String TRANSACTION_ROLLBACK_NO_SUBCLASS 
        = "40000";
    public static final String TRANSACTION_ROLLBACK_INTEGRITY_CONSTRAINT_VIOLATION 
        = "40002";
    public static final String TRANSACTION_ROLLBACK_SERIALIZATION_FAILURE 
        = "40001";
    public static final String TRANSACTION_ROLLBACK_STATEMENT_COMPLETION_UNKNOWN 
        = "40003";
    public static final String TRIGGERED_DATA_CHANGE_VIOLATION_NO_SUBCLASS 
        = "27000";
    public static final String WARNING_NO_SUBCLASS 
        = "01000";
    public static final String WARNING_ADDITIONAL_RESULT_SETS_RETURNED 
        = "0100D";
    public static final String WARNING_ARRAY_DATA_RIGHT_TRUNCATION 
        = "0102F";
    public static final String WARNING_ATTEMPT_TO_RETURN_TOO_MANY_RESULT_SETS 
        = "0100E";
    public static final String WARNING_CURSOR_OPERATION_CONFLICT 
        = "01001";
    public static final String WARNING_DEFAULT_VALUE_TOO_LONG_FOR_INFORMATION_SCHEMA 
        = "0100B";
    public static final String WARNING_DISCONNECT_ERROR 
        = "01002";
    public static final String WARNING_DYNAMIC_RESULT_SETS_RETURNED 
        = "0100C";
    public static final String WARNING_INSUFFICIENT_ITEM_DESCRIPTOR_AREAS 
        = "01005";
    public static final String WARNING_NULL_VALUE_ELIMINATED_IN_SET_FUNCTION 
        = "01003";
    public static final String WARNING_PRIVILEGE_NOT_GRANTED 
        = "01007";
    public static final String WARNING_PRIVILEGE_NOT_REVOKED 
        = "01006";
    public static final String WARNING_QUERY_EXPRESSION_TOO_LONG_FOR_INFORMATION_SCHEMA 
        = "0100A";
    public static final String WARNING_SEARCH_CONDITION_TOO_LONG_FOR_INFORMATION_SCHEMA 
        = "01009";
    public static final String WARNING_STATEMENT_TOO_LONG_FOR_INFORMATION_SCHEMA 
        = "0100F";
    public static final String WARNING_STRING_DATA_RIGHT_TRUNCATION_WARNING 
        = "01004";
    public static final String WITH_CHECK_OPTION_VIOLATION_NO_SUBCLASS 
        = "44000";
    // The SQLState when calling next on a result set which is closed.
    public static final String RESULT_SET_IS_CLOSED = "XCL16";
    //The SQLState of the SQLExcepion thrown when a class for which 
    //isWrapperFor returns false is passed as a parameter to the 
    //unwrap method.
    public static final String UNABLE_TO_UNWRAP = "XJ128";
    
    public static final String LANG_GRANT_REVOKE_WITH_LEGACY_ACCESS = "42Z60";
    public static final String SHUTDOWN_DATABASE = "08006";
    public static final String PROPERTY_UNSUPPORTED_CHANGE = "XCY02";
}
