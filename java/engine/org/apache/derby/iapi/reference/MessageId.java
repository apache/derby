/*

   Derby - Class org.apache.derby.iapi.reference.MessageId

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

package org.apache.derby.iapi.reference;

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
    String LOG_TRANS_LOG_SCAN               = "L019";


    /*
     * Raw Store data
     */

    String STORE_BOOT_MSG                   = "D001";
    String STORE_SHUTDOWN_MSG               = "D002";
    String STORE_BACKUP_STARTED             = "D004";
    String STORE_MOVED_BACKUP               = "D005";
    String STORE_COPIED_DB_DIR              = "D006";
    String STORE_EDITED_SERVICEPROPS        = "D007";
    String STORE_ERROR_EDIT_SERVICEPROPS    = "D008";
    String STORE_COPIED_LOG                 = "D009";
    String STORE_BACKUP_ABORTED             = "D010";
    String STORE_REMOVED_BACKUP             = "D011";
    String STORE_BACKUP_COMPLETED           = "D012";


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
	String CONN_DATABASE_IDENTITY			= "J004"; // database identity
	String CONN_SHUT_DOWN_CLOUDSCAPE		= "J005"; // shut down Cloudscape
	String CONN_CREATE_DATABASE				= "J007"; // create database
	String CONN_NO_DETAILS					= "J008"; // no details
	String CONN_SPS_INSTALLER				= "J009"; // unexepected exception while loading JDBC ...
    String CONN_DATA_ENCRYPTION             = "J010"; // encrypt database on disk
    String CONN_UPGRADE_DATABASE            = "J013"; // upgrade database 
	String CONN_USE_UNICODE                 = "J014"; // use unicode
    String CONN_CRYPTO_PROVIDER             = "J016"; // cryptographic service provider
    String CONN_CRYPTO_ALGORITHM            = "J017"; // cryptographic algorithm
    String CONN_CRYPTO_KEY_LENGTH           = "J018"; // cryptographic key length
	String CONN_CRYPTO_EXTERNAL_KEY         = "J019"; // external cryptographic key
	String CONN_BOOT_PASSWORD               = "J020"; // secret cryptographic key
	String CONN_LOCALE                      = "J021"; // locale for the database
	String CONN_USERNAME_ATTR               = "J022"; // user name
	String CONN_PASSWORD_ATTR               = "J023"; // user password
	String CONN_LOG_DEVICE                  = "J025"; // log directory path
	String CONN_XA_GLOBAL_XID               = "J026"; // global Xid to start XA transaction
	String CONN_STAGE_PAGE_SIZE             = "J027"; // page size in the heap  for replication
	String CONN_ROLL_FORWARD_RECOVERY_FROM  = "J028"; //backup path for roll-forward recovery 
	String CONN_CREATE_FROM                 = "J029"; //backup path for creating database from backup
	String CONN_RESTORE_FROM                = "J030"; //backup path for restoring database from backup
    String CONN_NETWORK_SERVER_CLASS_FIND   = "J100"; // Cannot find the network server starterclass
    String CONN_NETWORK_SERVER_CLASS_LOAD   = "J101"; // Cannot load the network server constructor
    String CONN_NETWORK_SERVER_START_EXCEPTION = "J102";
    String CONN_NETWORK_SERVER_SHUTDOWN_EXCEPTION = "J103";

	/*
	** Authentication
	*/
	String AUTH_NO_SERVICE_FOR_SYSTEM	= "A001"; // FATAL: There is no Authentication Service for the system
	String AUTH_NO_SERVICE_FOR_DB		= "A002"; // FATAL: There is no Authentication Service for the database
	String AUTH_NO_LDAP_HOST_MENTIONED	= "A011"; // No LDAP Server/Host name mentioned ...
	String AUTH_INVALID					= "A020"; // authentication failed due to invalid password or whatever

	/*
	** cloudscape Interface in com.ibm.db2j.core.*
	** These messages are thrown in both the server and the client side.
	*/
	String CORE_JDBC_DRIVER_UNREGISTERED= "I015"; // JDBCDriver is not registered with the JDBC driver manager
	String CORE_DATABASE_NOT_AVAILABLE	= "I024"; // Database not available
	String CORE_DRIVER_NOT_AVAILABLE	= "I025"; // JDBC Driver not available

    /*
     * Monitor
     */
    String SERVICE_PROPERTIES_DONT_EDIT = "M001"; // Tell user not to edit service.properties
}
