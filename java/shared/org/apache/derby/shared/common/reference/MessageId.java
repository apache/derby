/*

   Derby - Class org.apache.derby.shared.common.reference.MessageId

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

package org.apache.derby.shared.common.reference;

/**
	This class contains message identifiers for
	strings that are not converted to SQL exceptions.

	A* - Authentication
	C* - Class Manager
	D* - Raw Store Data
	I* - Interface in com.ibm.db2j.core.*
	J* - Connectivity (JDBC)
	L* - Raw Store Log
	M* - Message service
*/

public interface MessageId {

	/*
	** Raw Store log
	*/

	String LOG_BEGIN_ERROR					= "L001";
	String LOG_END_ERROR					= "L002";
	String LOG_BEGIN_CORRUPT_STACK			= "L003";
	String LOG_END_CORRUPT_STACK			= "L004";
	String LOG_BEGIN_ERROR_STACK			= "L005";
	String LOG_END_ERROR_STACK				= "L006";
	String LOG_LOG_NOT_FOUND				= "L007";
	String LOG_DELETE_INCOMPATIBLE_FILE		= "L008";
	String LOG_DELETE_OLD_FILE				= "L009";
	String LOG_INCOMPLETE_LOG_RECORD		= "L010";
	String LOG_CHECKPOINT_EXCEPTION			= "L011";
    String LOG_RECORD_NOT_FIRST             = "L012";
    String LOG_RECORD_FIRST                 = "L013";
    String LOG_BAD_START_INSTANT            = "L014";
    String LOG_NEW_LOGFILE_EXIST            = "L015";
    String LOG_CANNOT_CREATE_NEW            = "L016";
    String LOG_CANNOT_CREATE_NEW_DUETO      = "L017";
    String LOG_MAYBE_INCONSISTENT           = "L018";
    String LOG_WAS_IN_DURABILITY_TESTMODE_NO_SYNC = "L020"; // database was running in
                                                            // derby.system.durability set to test 
    String LOG_DURABILITY_TESTMODE_NO_SYNC_ERR = "L021"; // hint that error could be because 
                                                         // derby.system.durability was set to test

    String LOG_CHANGED_DB_TO_READ_ONLY        = "L022"; // a permissions error on log caused us
    													// to change db to read only.
    
    /*
     * SQLBinary.java internal translatable strings
     */
    String BINARY_DATA_HIDDEN               = "BIN01";

    /*
     * Raw Store data
     */

    String STORE_BOOT_MSG                   = "D001";
    String STORE_SHUTDOWN_MSG               = "D002";
    String STORE_BACKUP_STARTED             = "D004";
    String STORE_MOVED_BACKUP               = "D005";
    String STORE_DATA_SEG_BACKUP_COMPLETED  = "D006";
    String STORE_EDITED_SERVICEPROPS        = "D007";
    String STORE_ERROR_EDIT_SERVICEPROPS    = "D008";
    String STORE_COPIED_LOG                 = "D009";
    String STORE_BACKUP_ABORTED             = "D010";
    String STORE_REMOVED_BACKUP             = "D011";
    String STORE_BACKUP_COMPLETED           = "D012";
    String STORE_DURABILITY_TESTMODE_NO_SYNC = "D013"; // for derby.system.durability is 
                                                       // set to test
    String STORE_BOOT_READONLY_MSG          = "D014";
    /**
     * When the specified overflow page isn't found while streaming from a
     * page overflow chain (large data values, typically BLOB or CLOB).
     */
    String STORE_STREAM_OVERFLOW_PAGE_NOT_FOUND = "D015";

	/*
	** ClassManager
	*/
	String CM_WROTE_CLASS_FILE				= "C000";
	String CM_UNKNOWN_CERTIFICATE			= "C001";
	String CM_SECURITY_EXCEPTION			= "C002";
	String CM_LOAD_JAR_EXCEPTION			= "C003";
	String CM_STALE_LOADER					= "C004";
	String CM_CLASS_LOADER_START			= "C005";
	String CM_CLASS_LOAD					= "C006";
	String CM_CLASS_LOAD_EXCEPTION			= "C007";


	/*
	** Connectivity
	*/
	String CONN_SHUT_DOWN_ENGINE		    = "J003"; // shutting down Derby Engine
	String CONN_DATABASE_IDENTITY			= "J004"; // database identity
	String CONN_SHUT_DOWN_CLOUDSCAPE		= "J005"; // shut down Derby
    String CONN_DEREGISTER_AUTOLOADEDDRIVER = "J006"; // remove registered AutoloadedDriver
	String CONN_CREATE_DATABASE				= "J007"; // create database
	String CONN_NO_DETAILS					= "J008"; // no details
    String CONN_DATA_ENCRYPTION             = "J010"; // encrypt database on disk
    String CONN_UPGRADE_DATABASE            = "J013"; // upgrade database 
    String CONN_CRYPTO_PROVIDER             = "J016"; // cryptographic service provider
    String CONN_CRYPTO_ALGORITHM            = "J017"; // cryptographic algorithm
    String CONN_CRYPTO_KEY_LENGTH           = "J018"; // cryptographic key length
	String CONN_CRYPTO_EXTERNAL_KEY         = "J019"; // external cryptographic key
	String CONN_BOOT_PASSWORD               = "J020"; // secret cryptographic key
	String CONN_LOCALE                      = "J021"; // locale for the database
	String CONN_COLLATION                   = "J031"; // collation info for the character datatypes
	String CONN_USERNAME_ATTR               = "J022"; // user name
	String CONN_PASSWORD_ATTR               = "J023"; // user password
	String CONN_LOG_DEVICE                  = "J025"; // log directory path
	String CONN_ROLL_FORWARD_RECOVERY_FROM  = "J028"; //backup path for roll-forward recovery 
	String CONN_CREATE_FROM                 = "J029"; //backup path for creating database from backup
	String CONN_RESTORE_FROM                = "J030"; //backup path for restoring database from backup
    String CONN_NETWORK_SERVER_CLASS_FIND   = "J100"; // Cannot find the network server starterclass
    String CONN_NETWORK_SERVER_CLASS_LOAD   = "J101"; // Cannot load the network server constructor
    String CONN_NETWORK_SERVER_START_EXCEPTION = "J102";
    String CONN_NETWORK_SERVER_SHUTDOWN_EXCEPTION = "J103";
    String CONN_ALREADY_CLOSED                              = "J104";
    String CONN_PRECISION_TOO_LARGE                         = "J105";   
    //  following only used in text we print out - see client.am.SqlException:
    String BATCH_POSITION_ID                                = "J107";
    // following two only used in client.am.Version
    String SECURITY_MANAGER_NO_ACCESS_ID                    = "J108";
    String UNKNOWN_HOST_ID                                  = "J109";
    String CONN_SECMECH_NOT_SUPPORTED                       = "J110";
    String CONN_PASSWORD_MISSING                            = "J111";
    String CONN_USERID_MISSING                              = "J112";
    String CONN_USERID_OR_PASSWORD_INVALID                  = "J113";
    String CONN_USERID_REVOKED                              = "J114";
    String CONN_NEW_PASSWORD_INVALID                        = "J115";
    String CONN_SECSVC_NONRETRYABLE_ERR                     = "J116";
    String CONN_SECTKN_MISSING_OR_INVALID                   = "J117";
    String CONN_PASSWORD_EXPIRED                            = "J118";
    String CONN_NOT_SPECIFIED                               = "J120";
    String CONN_USER_NOT_AUTHORIZED_TO_DB                   = "J121";
    String CONN_DRDA_RDBNACRM                               = "J122";
    String CONN_DRDA_CMDCHKRM                               = "J123";
    String CONN_DRDA_RDBACCRM                               = "J124";
    String CONN_DRDA_DTARMCHRM                              = "J125";
    String CONN_DRDA_PRCCNVRM                               = "J126";
    String CONN_PARSE_SQLDIAGGRP_NOT_IMPLEMENTED            = "J127";
    String CONN_CURSOR_NOT_OPEN                             = "J128";
    String CONN_DRDA_QRYOPEN                                = "J129";
    String CONN_DRDA_INVALIDFDOCA                           = "J130";
    String CONN_DRDA_DATASTREAM_SYNTAX_ERROR                = "J131";
    String CONN_USERNAME_DESCRIPTION                        = "J132";
    String CONN_PASSWORD_DESCRIPTION                        = "J133";
    /**
     * Error message for negative values for the maxStatements property
     * of data sources supporting statement pooling.
     */
    String CONN_NEGATIVE_MAXSTATEMENTS                      = "J134";
    String CONN_XA_TRANSACTION_TIMED_OUT                    = "J135";
    String CONN_CLOSE_XA_TRANSACTION_ROLLED_BACK            = "J136";

	/*
	** Authentication
	*/
	String AUTH_NO_SERVICE_FOR_SYSTEM	= "A001"; // FATAL: There is no Authentication Service for the system
	String AUTH_NO_SERVICE_FOR_DB		= "A002"; // FATAL: There is no Authentication Service for the database
	String AUTH_NO_LDAP_HOST_MENTIONED	= "A011"; // No LDAP Server/Host name mentioned ...
	String AUTH_INVALID					= "A020"; // authentication failed due to invalid password or whatever

	/*
	** Derby interface in org.apache.derby.iapi.*
	** These messages are thrown in both the server and the client side.
	*/
	String CORE_DATABASE_NOT_AVAILABLE	= "I024"; // Database not available
	String CORE_DRIVER_NOT_AVAILABLE	= "I025"; // JDBC Driver not available
	String JDBC_DRIVER_REGISTER_ERROR 	= "I026"; // Error while registering driver
    /**
     * At the time InputStream.reset was invoked, either no mark was set or the
     * read ahead limit of the mark was exceeded.
     */
    String STREAM_MARK_UNSET_OR_EXCEEDED                    = "I027";
    /**
     * Error message when the client encounters a read error when reading a
     * user stream, which it is in the process of sending to the server.
     */
    String STREAM_DRDA_CLIENTSIDE_EXTDTA_READ_ERROR         = "I028";
    /** The stream ended before it was supposed to. */
    String STREAM_PREMATURE_EOF                             = "I029";

    /*
     * Monitor
     */
    String SERVICE_PROPERTIES_DONT_EDIT = "M001"; // Tell user not to edit service.properties

    /*
     * Replication
     */
    String REPLICATION_ERROR_BEGIN                       = "R001";
    String REPLICATION_ERROR_END                         = "R002";
    String REPLICATION_SLAVE_STARTED                     = "R003";
    String REPLICATION_SLAVE_STOPPED                     = "R004";
    String REPLICATION_FATAL_ERROR                       = "R005";
    String REPLICATION_SLAVE_LOST_CONN                   = "R006";
    String REPLICATION_MASTER_STARTED                    = "R007";
    String REPLICATION_MASTER_STOPPED                    = "R008";
    String REPLICATION_LOGSHIPPER_EXCEPTION              = "R009";
    String REPLICATION_MASTER_RECONN                     = "R010";
    String REPLICATION_SLAVE_NETWORK_LISTEN              = "R011";    
    String REPLICATION_INVALID_CONNECTION_HANDLE         = "R012";
    String REPLICATION_ONELINE_MSG_HEADER                = "R013";
    String REPLICATION_FAILOVER_SUCCESSFUL               = "R020";

}
