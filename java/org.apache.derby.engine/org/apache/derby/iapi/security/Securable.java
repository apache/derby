/*

   Derby - Class org.apache.derby.iapi.security.Securable

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

package org.apache.derby.iapi.security;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

/**
 * Operations which can be secured. SQL authorization is one way to control
 * who can access these operations.
 */
public enum Securable
{
        SET_DATABASE_PROPERTY
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_SET_DATABASE_PROPERTY",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        GET_DATABASE_PROPERTY
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_GET_DATABASE_PROPERTY",
             AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR
             ),
            
        FREEZE_DATABASE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_FREEZE_DATABASE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        UNFREEZE_DATABASE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_UNFREEZE_DATABASE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        CHECKPOINT_DATABASE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_CHECKPOINT_DATABASE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        BACKUP_DATABASE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_BACKUP_DATABASE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        BACKUP_DATABASE_NOWAIT
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_BACKUP_DATABASE_NOWAIT",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        DISABLE_LOG_ARCHIVE_MODE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_DISABLE_LOG_ARCHIVE_MODE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        CHECK_TABLE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_CHECK_TABLE",
             AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR
             ),
            
        INSTALL_JAR
            (
             SchemaDescriptor.SQLJ_SCHEMA_UUID,
             "INSTALL_JAR",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        REPLACE_JAR
            (
             SchemaDescriptor.SQLJ_SCHEMA_UUID,
             "REPLACE_JAR",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        REMOVE_JAR
            (
             SchemaDescriptor.SQLJ_SCHEMA_UUID,
             "REMOVE_JAR",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        EXPORT_TABLE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_EXPORT_TABLE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        IMPORT_TABLE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_IMPORT_TABLE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        IMPORT_TABLE_LOBS_FROM_EXTFILE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        IMPORT_DATA
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_IMPORT_DATA",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        IMPORT_DATA_LOBS_FROM_EXTFILE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        BULK_INSERT
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_BULK_INSERT",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        RELOAD_SECURITY_POLICY
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_RELOAD_SECURITY_POLICY",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        SET_USER_ACCESS
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_SET_USER_ACCESS",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        GET_USER_ACCESS
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_GET_USER_ACCESS",
             AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR
             ),
            
        INVALIDATE_STORED_STATEMENTS
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_INVALIDATE_STORED_STATEMENTS",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        EMPTY_STATEMENT_CACHE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_EMPTY_STATEMENT_CACHE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        SET_XPLAIN_MODE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_SET_XPLAIN_MODE",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        GET_XPLAIN_MODE
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_GET_XPLAIN_MODE",
             AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR
             ),
            
        SET_XPLAIN_SCHEMA
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_SET_XPLAIN_SCHEMA",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        GET_XPLAIN_SCHEMA
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_GET_XPLAIN_SCHEMA",
             AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR
             ),
            
        CREATE_USER
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_CREATE_USER",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        RESET_PASSWORD
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_RESET_PASSWORD",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
        DROP_USER
            (
             SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
             "SYSCS_DROP_USER",
             AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR
             ),
            
            ;

        /** UUID string of schema holding the system routine associated with the operation */
        public  final   String  routineSchemaID;

        /** Name of the associated system routine */
        public  final   String  routineName;
        
        /** Type of routine (function or procedure) */
        public  final   char    routineType;

        /** Construct a Securable from its attributes */
        private Securable
            (
             String routineSchemaID,
             String routineName,
             char   routineType
             )
        {
            this.routineSchemaID = routineSchemaID;
            this.routineName = routineName;
            this.routineType = routineType;
        }

}
