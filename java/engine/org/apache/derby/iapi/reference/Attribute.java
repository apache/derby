/*

   Derby - Class org.apache.derby.iapi.reference.Attribute

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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
*/

public interface Attribute {

	/**
		Not an attribute but the root for the JDBC URL that Cloudscape supports.
	*/
	String PROTOCOL = "jdbc:derby:";
		
	/**
	 * The SQLJ protocol for getting the default connection
	 * for server side jdbc
	 */
	String SQLJ_NESTED = "jdbc:default:connection";


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
		The attribute that is used for the database name, from
		the JDBC notion of jdbc:<subprotocol>:<subname>
	*/
	String DBNAME_ATTR = "databaseName";

	/**
		The attribute that is used to request a shutdown.
	*/
	String SHUTDOWN_ATTR = "shutdown";

	/**
		The attribute that is used to request a database create.
	*/
	String CREATE_ATTR = "create";

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
		Set the collation sequence of the database, currently on IDENTITY
        will be supported (strings will sort according to binary comparison).
	*/
	String COLLATE = "collate";

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


}


