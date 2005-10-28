/*

   Derby - Class org.apache.derby.iapi.services.crypto.CipherProvider

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

import java.security.Key;

import org.apache.derby.iapi.error.StandardException;

/**
	A CipherProvider is a wrapper for a Cipher class in JCE.

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

public interface CipherProvider
{

	/**
		Encrypt data - use only with Cipher that has been initialized with
		CipherFactory.ENCRYPT.

		@return The number of bytes stored in ciphertext.

		@param cleartext the byte array containing the cleartext
		@param offset encrypt from this byte offset in the cleartext
		@param length encrypt this many bytes starting from offset
		@param ciphertext the byte array to store the ciphertext
		@param outputOffset the offset into the ciphertext array the output
				should go

		If cleartext and ciphertext are the same array, caller must be careful
		to not overwrite the cleartext before it is scrambled.

		@exception StandardException Standard Cloudscape Error Policy
	 */
	int encrypt(byte[] cleartext, int offset, int length,
				byte[] ciphertext, int outputOffset)
		 throws StandardException;

	/**
		Decrypt data - use only with Cipher that has been initialized with
		CipherFactory.DECRYPT.

		@return The number of bytes stored in cleartext.

		@param ciphertext the byte array containing the ciphertext
		@param offset decrypt from this byte offset in the ciphertext
		@param length decrypt this many bytes starting from offset
		@param cleartext the byte array to store the cleartext
		@param outputOffset the offset into the cleartext array the output
				should go

		If cleartext and ciphertext are the same array, caller must be careful
		to not overwrite the ciphertext before it is un-scrambled.

		@exception StandardException Standard Cloudscape Error Policy
	 */
	int decrypt(byte[] ciphertext, int offset, int length,
				byte[] cleartext, int outputOffset)
		 throws StandardException;


	/**
	 	Returns the encryption block size used during creation of the encrypted database
	 */
	public int getEncryptionBlockSize();
}
