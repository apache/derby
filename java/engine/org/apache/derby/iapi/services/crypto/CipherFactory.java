/*

   Derby - Class org.apache.derby.iapi.services.crypto.CipherFactory

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.crypto;

import org.apache.derby.iapi.error.StandardException;
import java.security.SecureRandom;
import java.util.Properties;
import org.apache.derby.io.StorageFactory;

/**
	A CipherFactory can create new CipherProvider, which is a wrapper for a
	javax.crypto.Cipher

	This service is only available when run on JDK1.2 or beyond.
	To use this service, either the SunJCE or an alternative clean room
    implementation of the JCE must be installed.

	To use a CipherProvider to encrypt or decrypt, it needs 3 things:
	1) A CipherProvider that is initialized to ENCRYPT or DECRYPT
	2) A secret Key for the encryption/decryption
	3) An Initialization Vector (IvParameterSpec) that is used to create some
		randomness in the encryption

    See $WS/docs/funcspec/mulan/configurableEncryption.html

	See http://java.sun.com/products/JDK/1.1/docs/guide/security/CryptoSpec.html
	See http://java.sun.com/products/JDK/1.2/docs/guide/security/CryptoSpec.html
	See http://java.sun.com/products/jdk/1.2/jce/index.html
 */

public interface CipherFactory
{

    /** Minimum bootPassword length */
    public static final int MIN_BOOTPASS_LENGTH = 8;

	/**
		Get a CipherProvider that either Encrypts or Decrypts.
	 */
	public static final int ENCRYPT = 1;
	public static final int DECRYPT = 2;


	SecureRandom getSecureRandom();

	/**
		Returns a CipherProvider which is the encryption or decryption engine.
		@param mode is either ENCRYPT or DECRYPT.  The CipherProvider can only
				do encryption or decryption but not both.

		@exception StandardException Standard Cloudscape Error Policy
	 */
	CipherProvider createNewCipher(int mode)
		 throws StandardException;

	public String changeBootPassword(String changeString, Properties properties, CipherProvider verify)
		throws StandardException;

	/**
		Verify the external encryption key
		@param	create	 true means database is being created, whereas false
					implies that the database has already been created
		@param	storageFactory storageFactory is used to access any stored data
					that might be needed for verification process of the encryption key
		@param	properties	properties at time of database connection as well as those in service.properties

		@return	throws exception if unable to verify that the encryption key is the same as that
				used during database creation or if there are any problems when trying to do the
				verification process
	 */
	public void verifyKey(boolean create, StorageFactory storageFactory,Properties properties)
		throws StandardException;

}


