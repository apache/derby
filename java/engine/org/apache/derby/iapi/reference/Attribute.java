/*

   Derby - Class org.apache.derby.iapi.reference.Attribute

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

package org.apache.derby.iapi.reference;


/**
	List of all connection (JDBC) attributes by the system.


	<P>
	This class exists for two reasons
	<Ol>
	<LI> To act as the internal documentation for the attributes. 
	<LI> To remove the need to declare a java static field for the attributes
	name in the protocol/implementation class. This reduces the footprint as
	the string is final and thus can be included simply as a String constant pool entry.
	</OL>
	<P>
	This class should not be shipped with the product.

	<P>
	This class has no methods, all it contains are String's which by
	are public, static and final since they are declared in an interface.
    
    <P>
    At some point this class should be replaced by
    org.apache.derby.shared.common.reference.Attribute.
    The issue is that this class is used by ij to check attributes,
    ij uses reflection on this class to get the list of valid attributes.
    The expanded class in shared has the client attributes as well.
    Ideally ij would work of an explicit list of attributes and not
    infer the set from reflection. See DERBY-1151
*/

public interface Attribute {

	/**
		Not an attribute but the root for the JDBC URL that Derby supports.
	*/
	String PROTOCOL = "jdbc:derby:";
		
	/**
	 * The SQLJ protocol for getting the default connection
	 * for server side jdbc
	 */
	String SQLJ_NESTED = "jdbc:default:connection";

	
	// Network Protocols.  These need to be rejected by the embedded driver.
	
	/**
	 * The protocol for Derby Network Client 
	 */ 
	String DNC_PROTOCOL = "jdbc:derby://";
	
	/** 
	 * The protocol for the IBM Universal JDBC Driver 
	 * 
	 */
	String JCC_PROTOCOL = "jdbc:derby:net:";
	
    /**
     * Attribute name for decrypting an encrypted database.
     */
    String DECRYPT_DATABASE = "decryptDatabase";

	/**
		Attribute name to encrypt the database on disk.
		If set to true, all user data is stored encrypted on disk.
	 */
	String DATA_ENCRYPTION = "dataEncryption";

	/**
		If dataEncryption is true, use this attribute to pass in the 
		secret key.  The secret key must be at least 8 characters long.
		This key must not be stored persistently in cleartext anywhere. 
	 */

	String BOOT_PASSWORD = "bootPassword";

    /**
	    The attribute that is used to chage the secret key of an encrypted
        database. The secret key must be at least 8 characters long.
		This key must not be stored persistently in cleartext anywhere. 
	 */

	String NEW_BOOT_PASSWORD = "newBootPassword";

    /**
     * Attribute name to start replication master mode for a database.
     * If used, REPLICATION_SLAVE_HOST is a required attribute.
     */
    String REPLICATION_START_MASTER = "startMaster";

    /**
     * Attribute name to stop replication master mode for a database.
     */
    String REPLICATION_STOP_MASTER = "stopMaster";

    /**
     * Attribute name to start replication slave mode for a database.
     */
    String REPLICATION_START_SLAVE = "startSlave";

    /**
     * Attribute name to stop replication slave mode for a database.
     */
    String REPLICATION_STOP_SLAVE = "stopSlave";

    /**
     * Attribute name to stop replication slave mode for a database.
     * Internal use only
     */
    String REPLICATION_INTERNAL_SHUTDOWN_SLAVE = "internal_stopslave";

    /**
     * If startMaster is true, this attribute is used to specify the
     * host name the master should connect to. This is a required
     * attribute.
     */
    String REPLICATION_SLAVE_HOST = "slaveHost";
    
    /**
     * Attribute name to start failover for a given database..
     */
    String REPLICATION_FAILOVER = "failover";

    /**
     * If startMaster is true, this attribute is used to specify the
     * port the master should connect to. This is an optional
     * attribute
     */
    String REPLICATION_SLAVE_PORT = "slavePort";

	/**
		The attribute that is used for the database name, from
		the JDBC notion of jdbc:<subprotocol>:<subname>
	*/
	String DBNAME_ATTR = "databaseName";

	/**
		The attribute that is used to request a shutdown.
	*/
	String SHUTDOWN_ATTR = "shutdown";

    /**
     * The attribute that is to keep autoloading idiom for driver
     */
    String DEREGISTER_ATTR = "deregister";

	/**
		The attribute that is used to request a database create.
	*/
	String CREATE_ATTR = "create";

    /**
        The attribute that is used to request a drop database.
    */
    String DROP_ATTR = "drop";

	/**
		The attribute that is used to set the user name.
	*/
	String USERNAME_ATTR = "user";


	/**
		The attribute that is used to set the user password.
	*/
	String PASSWORD_ATTR = "password";


	/**
		The attribute that is used to set the connection's DRDA ID.
	*/
	String DRDAID_ATTR = "drdaID";

	/**
		The attribute that is used to allow upgrade.
	*/
	String UPGRADE_ATTR = "upgrade";

	/**
		Put the log on a different device.
	 */
	String LOG_DEVICE = "logDevice";

	/**
		Set the territory for the database.
	*/
	String TERRITORY = "territory";

    /**
        Attribute for encrypting a database.
        Specifies the cryptographic services provider.
    */
    String CRYPTO_PROVIDER = "encryptionProvider";

    /**
        Attribute for encrypting a database.
        Specifies the cryptographic algorithm.
    */
    String CRYPTO_ALGORITHM = "encryptionAlgorithm";

    /**
        Attribute for encrypting a database.
        Specifies the key length in bytes for the specified cryptographic algorithm.
    */
	String CRYPTO_KEY_LENGTH = "encryptionKeyLength";

	/**
        Attribute for encrypting a database.
        Specifies the actual key. When this is specified
		all the supplied crypto information is stored
		external to the database, ie by the application.
	*/
	String CRYPTO_EXTERNAL_KEY = "encryptionKey";


    /**
	    The attribute that is used to chage the encryption 
        key of an encrypted database. When this is specified
        all the supplied crypto information is stored
        external to the database, ie by the application.
	*/
	String NEW_CRYPTO_EXTERNAL_KEY = "newEncryptionKey";


	/**
	   One can encrypt the database with an encryption key at create time.
	   For security reasons, this key is not made persistent in the database.

	   But it is necessary to verify the encryption key whenever booting the database  
	   before we access the data/logs to avoid the risk of corrupting the database because
	   of a wrong encryption key.

	   This attribute refers to the name of the file where encrypted data is stored for
	   verification of encryption key.
	 */
	String CRYPTO_EXTERNAL_KEY_VERIFY_FILE = "verifyKey.dat";

	
	/**
	 *	This attribute is used to request to  create a database from backup.
	 *  This will throw error if a database with same already exists at the 
	 *  location where we tring to create.
	 */
	String CREATE_FROM = "createFrom";

	/**
	 *	This attribute is used to request a database restore from backup.
	 *  It must be used only when the active database is corrupted,
	 *	because it will cleanup the existing database and replace 
	 *	it from the backup.
	 */
	String RESTORE_FROM = "restoreFrom";

	/**
		The attribute that is used to request a roll-forward recovery of the database.
	*/
	String ROLL_FORWARD_RECOVERY_FROM = "rollForwardRecoveryFrom";

    /**
     * securityMechanism sets the DRDA mechanism in-use for the client.
     * Internal only.
     */
    String CLIENT_SECURITY_MECHANISM = "securityMechanism";

    /**
     * Internal attributes. Mainly used by DRDA and Derby BUILTIN
     * authentication provider in some security mechanism context
     * (SECMEC_USRSSBPWD).
     *
     * DRDA_SECTKN_IN is the random client seed (RDs)
     * DRDA_SECTKN_OUT is the random server seed (RDr)
     */
    String DRDA_SECTKN_IN = "drdaSecTokenIn";
    String DRDA_SECTKN_OUT = "drdaSecTokenOut";
    /**
     * Internal attribute which holds the value of the securityMechanism
     * attribute specified by the client. Used for passing information about
     * which security mechanism to use from the network server to the embedded
     * driver. Use another name than "securityMechanism" in order to prevent
     * confusion if an attempt is made to establish an embedded connection with
     * securityMechanism specified (see DERBY-3025).
     */
    String DRDA_SECMEC = "drdaSecMec";

	/**
	 * Internal attribute. Used to always allow soft upgrade for
	 * authentication purposes in a two phase hard upgrade (to check
	 * database owner power before proceeding.  The purpose is to
	 * avoid failing soft upgrade due to a feature being set but not
	 * supported until after hard upgrade has taken place (e.g. during
	 * hard upgrade from 10.1 -> 10.3 or higher if
	 * derby.database.sqlAuthorization is set,
	 * cf. DD_Version#checkVersion).
	 */
	 String SOFT_UPGRADE_NO_FEATURE_CHECK = "softUpgradeNoFeatureCheck";

	/**
		Optional JDBC url attribute (at the database create time only) It can 
		be set to one of the following 2 values
		1) UCS_BASIC (This means codepoint based collation. This will also be 
		the default collation used by Derby if no collation attribute is 
		specified on the JDBC url at the database create time. This collation 
		is what Derby 10.2 and prior have supported)
		2)TERRITORY_BASED (the collation will be based on language 
		region specified by the exisiting Derby attribute called territory. 
		If the territory attribute is not specified at the database create 
		time, Derby will use java.util.Locale.getDefault to determine the 
		territory for the newly created database. 
	*/
	String COLLATION = "collation";
}
