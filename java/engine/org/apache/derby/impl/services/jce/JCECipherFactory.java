/*

   Derby - Class org.apache.derby.impl.services.jce.JCECipherFactory

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

package org.apache.derby.impl.services.jce;

import org.apache.derby.iapi.services.crypto.CipherFactory;
import org.apache.derby.iapi.services.crypto.CipherProvider;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.iapi.util.StringUtil;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.util.StringUtil;

import java.util.Properties;

import java.security.Key;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.MessageDigest;
import java.security.spec.KeySpec;
import java.security.spec.InvalidKeySpecException;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;
/**
	This CipherFactory creates new JCECipherProvider.

	@see CipherFactory
 */
public final class JCECipherFactory implements CipherFactory, ModuleControl, java.security.PrivilegedExceptionAction
{
    private final static String MESSAGE_DIGEST = "MD5";

	private final static String DEFAULT_PROVIDER = "com.sun.crypto.provider.SunJCE";
	private final static String DEFAULT_ALGORITHM = "DES/CBC/NoPadding";
	private final static String DES = "DES";
	private final static String DESede = "DESede";
    private final static String TripleDES = "TripleDES";
    private final static String AES = "AES";

    // minimum boot password length in bytes
    private final static int BLOCK_LENGTH = 8;

    /**
	AES encryption takes in an default Initialization vector length (IV) length of 16 bytes
	This is needed to generate an IV to use for encryption and decryption process 
	@see CipherProvider
     */
    private final static int AES_IV_LENGTH = 16;

    // key length in bytes
	private int keyLengthBits;
    private int encodedKeyLength;
    private String cryptoAlgorithm;
    private String cryptoAlgorithmShort;
    private String cryptoProvider;
    private String cryptoProviderShort;
	private MessageDigest messageDigest;

	private SecretKey mainSecretKey;
	private byte[] mainIV;

	/**
	    Amount of data that is used for verification of external encryption key
	    This does not include the MD5 checksum bytes
	 */
	private final static int VERIFYKEY_DATALEN = 4096;
	private StorageFile activeFile;
	private int action;
	private String activePerms;

	static String providerErrorName(String cps) {

		return cps == null ? "default" : cps;
	}


	private byte[] generateUniqueBytes() throws StandardException
	{
		try {

			String provider = cryptoProviderShort;

			KeyGenerator keyGen;
			if (provider == null)
			{
				keyGen = KeyGenerator.getInstance(cryptoAlgorithmShort);
			}
			else
			{
				if( provider.equals("BouncyCastleProvider"))
					provider = "BC";
				keyGen = KeyGenerator.getInstance(cryptoAlgorithmShort, provider);
			}

			keyGen.init(keyLengthBits);

			SecretKey key = keyGen.generateKey();

			return key.getEncoded();

		} catch (java.security.NoSuchAlgorithmException nsae) {
    		throw StandardException.newException(SQLState.ENCRYPTION_NOSUCH_ALGORITHM, cryptoAlgorithm,
				JCECipherFactory.providerErrorName(cryptoProviderShort));
		} catch (java.security.NoSuchProviderException nspe) {
			throw StandardException.newException(SQLState.ENCRYPTION_BAD_PROVIDER,
				JCECipherFactory.providerErrorName(cryptoProviderShort));
		}
	}

	/**
		Encrypt the secretKey with the boot password.
		This includes the following steps, 
		getting muck from the boot password and then using this to generate a key,
		generating an appropriate IV using the muck
		using the key and IV thus generated to create the appropriate cipher provider
		and encrypting the secretKey 
		@return hexadecimal string of the encrypted secretKey

		@exception StandardException Standard Cloudscape error policy
	 */
	private String encryptKey(byte[] secretKey, byte[] bootPassword)
		 throws StandardException
	{
		// In case of AES, care needs to be taken to allow for 16 bytes muck as well
		// as to have the secretKey that needs encryption to be a aligned appropriately
		// AES supports 16 bytes block size

		int muckLength = secretKey.length;
		if(cryptoAlgorithmShort.equals(AES))
			muckLength = AES_IV_LENGTH;		

		byte[] muck = getMuckFromBootPassword(bootPassword, muckLength);
		SecretKey key = generateKey(muck);
		byte[] IV = generateIV(muck);
                CipherProvider tmpCipherProvider = createNewCipher(ENCRYPT,key,IV);
		
		// store the actual secretKey.length before any possible padding  
		encodedKeyLength = secretKey.length;

		// for the secretKey to be encrypted, first ensure that it is aligned to the block size of the 
		// encryption algorithm by padding bytes appropriately if needed
                secretKey = padKey(secretKey,tmpCipherProvider.getEncryptionBlockSize());

                byte[] result = new byte[secretKey.length];

		// encrypt the secretKey using the key generated of muck from  boot password and the generated IV  
		tmpCipherProvider.encrypt(secretKey, 0, secretKey.length, result, 0);

		return org.apache.derby.iapi.util.StringUtil.toHexString(result, 0, result.length);

	}
	
	/**
            For block ciphers, and  algorithms using the NoPadding scheme, the data that has 
            to be encrypted needs to be a multiple of the expected block size for the cipher 
	    Pad the key with appropriate padding to make it blockSize align
	    @param     secretKey	the data that needs blocksize alignment
	    @param     blockSizeAlign   secretKey needs to be blocksize aligned		
	    @return    a byte array with the contents of secretKey along with padded bytes in the end
		       to make it blockSize aligned
         */
	private byte[] padKey(byte[] secretKey,int blockSizeAlign)
	{
	    byte [] result = secretKey;
	    if(secretKey.length % blockSizeAlign != 0 )
	    {
		int encryptedLength = secretKey.length + blockSizeAlign - (secretKey.length % blockSizeAlign);
		result = new byte[encryptedLength];
		System.arraycopy(secretKey,0,result,0,secretKey.length);
	    }
	    return result;
	}

	/**
	    Decrypt the secretKey with the user key .
	    This includes the following steps, 
	    retrieve the encryptedKey, generate the muck from the boot password and generate an appropriate IV using
	    the muck,and using the key and IV decrypt the encryptedKey 
	    @return decrypted key  
		@exception StandardException Standard Cloudscape error policy
	 */
	private byte[] decryptKey(String encryptedKey, int encodedKeyCharLength, byte[] bootPassword)
		 throws StandardException
	{
		byte[] secretKey = org.apache.derby.iapi.util.StringUtil.fromHexString(encryptedKey, 0, encodedKeyCharLength);
		// In case of AES, care needs to be taken to allow for 16 bytes muck as well
		// as to have the secretKey that needs encryption to be a aligned appropriately
		// AES supports 16 bytes block size
		int muckLength;
		if(cryptoAlgorithmShort.equals(AES))
		    muckLength = AES_IV_LENGTH;
		else
	            muckLength = secretKey.length;	

		byte[] muck = getMuckFromBootPassword(bootPassword, muckLength);


		// decrypt the encryptedKey with the mucked up boot password to recover
		// the secretKey
		SecretKey key = generateKey(muck);
		byte[] IV = generateIV(muck);


		createNewCipher(DECRYPT, key, IV).
			decrypt(secretKey, 0, secretKey.length, secretKey, 0);

		return secretKey;
	}

	private byte[] getMuckFromBootPassword(byte[] bootPassword, int encodedKeyByteLength) {
		int ulength = bootPassword.length;

		byte[] muck = new byte[encodedKeyByteLength];
		

		int rotation = 0;
		for (int i = 0; i < bootPassword.length; i++)
			rotation += bootPassword[i];

		for (int i = 0; i < encodedKeyByteLength; i++)
			muck[i] = (byte)(bootPassword[(i+rotation)%ulength] ^
                (bootPassword[i%ulength] << 4));

		return muck;
	}

	/**
		Generate a Key object using the input secretKey that can be used by
		JCECipherProvider to encrypt or decrypt.

		@exception StandardException Standard Cloudscape Error Policy
	 */
	private SecretKey generateKey(byte[] secretKey) throws StandardException
	{
		int length = secretKey.length;

		if (length < CipherFactory.MIN_BOOTPASS_LENGTH)
			throw StandardException.newException(SQLState.ILLEGAL_BP_LENGTH, new Integer(MIN_BOOTPASS_LENGTH));

		try
		{
            if (cryptoAlgorithmShort.equals(DES))
            {   // single DES
			    if (DESKeySpec.isWeak(secretKey, 0))
			    {
				    // OK, it is weak, spice it up
				    byte[] spice = StringUtil.getAsciiBytes("louDScap");
				    for (int i = 0; i < 7; i++)
					    secretKey[i] = (byte)((spice[i] << 3) ^ secretKey[i]);
			    }
            }
			return new SecretKeySpec(secretKey, cryptoAlgorithmShort);
		}
		catch (InvalidKeyException ike)
		{
			throw StandardException.newException(SQLState.CRYPTO_EXCEPTION, ike);
		}

	}

	/**
		Generate an IV using the input secretKey that can be used by
		JCECipherProvider to encrypt or decrypt.
	 */
	private byte[] generateIV(byte[] secretKey)
	{

		// do a little simple minded muddling to make the IV not
		// strictly alphanumeric and the number of total possible keys a little
		// bigger.
		int IVlen = BLOCK_LENGTH;
		
		byte[] iv = null;
		if(cryptoAlgorithmShort.equals(AES))
		{
			IVlen = AES_IV_LENGTH;
			iv = new byte[IVlen];
			iv[0] = (byte)(((secretKey[secretKey.length-1] << 2) | 0xF) ^ secretKey[0]);
			for (int i = 1; i < BLOCK_LENGTH; i++)
				iv[i] = (byte)(((secretKey[i-1] << (i%5)) | 0xF) ^ secretKey[i]);
			
			for(int i = BLOCK_LENGTH ; i < AES_IV_LENGTH ; i++)
			{
				iv[i]=iv[i-BLOCK_LENGTH];
			}
			
		}	
		else
		{
			iv = new byte[BLOCK_LENGTH];
			iv[0] = (byte)(((secretKey[secretKey.length-1] << 2) | 0xF) ^ secretKey[0]);
			for (int i = 1; i < BLOCK_LENGTH; i++)
				iv[i] = (byte)(((secretKey[i-1] << (i%5)) | 0xF) ^ secretKey[i]);	
		}

		return iv;
	}

	private int digest(byte[] input)
	{
		messageDigest.reset();
		byte[] digest = messageDigest.digest(input);
		byte[] condenseDigest = new byte[2];

		// no matter how long the digest is, condense it into an short.
		for (int i = 0; i < digest.length; i++)
			condenseDigest[i%2] ^= digest[i];

		int retval = (condenseDigest[0] & 0xFF) | ((condenseDigest[1] << 8) & 0xFF00);

		return retval;
	}

	public SecureRandom getSecureRandom() {
		return new SecureRandom(mainIV);
	}

	public CipherProvider createNewCipher(int mode)
										  throws StandardException {
		return createNewCipher(mode, mainSecretKey, mainIV);
	}


	private CipherProvider createNewCipher(int mode, SecretKey secretKey,
										  byte[] iv)
		 throws StandardException
	{
		return new JCECipherProvider(mode, secretKey, iv, cryptoAlgorithm, cryptoProviderShort);
	}

	/*
	 * module control methods
	 */

	public void	boot(boolean create, Properties properties)
		throws StandardException
	{

		if (SanityManager.DEBUG) {
			if (JVMInfo.JDK_ID < 2)
				SanityManager.THROWASSERT("expected JDK ID to be 2 - is " + JVMInfo.JDK_ID);
		}

        boolean provider_or_algo_specified = false;
		boolean storeProperties = create;

		String externalKey = properties.getProperty(Attribute.CRYPTO_EXTERNAL_KEY);
		if (externalKey != null) {
			storeProperties = false;
		}

        cryptoProvider = properties.getProperty(Attribute.CRYPTO_PROVIDER);

		if (cryptoProvider == null)
		{
			// JDK 1.3 does not create providers by itself.
			if (JVMInfo.JDK_ID == 2) {

				String vendor;
				try {
					vendor = System.getProperty("java.vendor", "");
				} catch (SecurityException se) {
					vendor = "";
				}

				vendor = StringUtil.SQLToUpperCase(vendor);

				if (vendor.startsWith("IBM "))
					cryptoProvider = "com.ibm.crypto.provider.IBMJCE";
				else if (vendor.startsWith("SUN "))
					cryptoProvider = "com.sun.crypto.provider.SunJCE";

			}
		}
		else
		{
            provider_or_algo_specified = true;

			// explictly putting the properties back into the properties
			// saves then in service.properties at create time.
		//	if (storeProperties)
		//		properties.put(Attribute.CRYPTO_PROVIDER, cryptoProvider);

			int dotPos = cryptoProvider.lastIndexOf('.');
			if (dotPos == -1)
				cryptoProviderShort = cryptoProvider;
			else
				cryptoProviderShort = cryptoProvider.substring(dotPos+1);

		}

        cryptoAlgorithm = properties.getProperty(Attribute.CRYPTO_ALGORITHM);
        if (cryptoAlgorithm == null)
            cryptoAlgorithm = DEFAULT_ALGORITHM;
        else {
            provider_or_algo_specified = true;

		}

		// explictly putting the properties back into the properties
		// saves then in service.properties at create time.
        if (storeProperties)
			properties.put(Attribute.CRYPTO_ALGORITHM, cryptoAlgorithm);

        int firstSlashPos = cryptoAlgorithm.indexOf('/');
        int lastSlashPos = cryptoAlgorithm.lastIndexOf('/');
        if (firstSlashPos < 0 || lastSlashPos < 0 || firstSlashPos == lastSlashPos)
    		throw StandardException.newException(SQLState.ENCRYPTION_BAD_ALG_FORMAT, cryptoAlgorithm);

        cryptoAlgorithmShort = cryptoAlgorithm.substring(0,firstSlashPos);

        if (provider_or_algo_specified)
        {
            // Track 3715 - disable use of provider/aglo specification if
            // jce environment is not 1.2.1.  The ExemptionMechanism class
            // exists in jce1.2.1 and not in jce1.2, so try and load the
            // class and if you can't find it don't allow the encryption.
            // This is a requirement from the government to give cloudscape
            // export clearance for 3.6.  Note that the check is not needed
            // if no provider/algo is specified, in that case we default to
            // a DES weak encryption algorithm which also is allowed for
            // export (this is how 3.5 got it's clearance).
            try
            {
                Class c = Class.forName("javax.crypto.ExemptionMechanism");
            }
            catch (Throwable t)
            {
                throw StandardException.newException(
                            SQLState.ENCRYPTION_BAD_JCE);
            }
        }

		// If connecting to an existing database and Attribute.CRYPTO_KEY_LENGTH is set
		// then obtain the encoded key length values without padding bytes and retrieve
		// the keylength in bits if boot password mechanism is used 
		// note: Attribute.CRYPTO_KEY_LENGTH is set during creation time to a supported
		// key length in the connection url. Internally , two values are stored in this property
		// if encryptionKey is used, this property will have only the encoded key length
		// if boot password mechanism is used, this property will have the following 
		// keylengthBits-EncodedKeyLength 
                 
		if(!create)
		{
		    // if available, parse the keylengths stored in Attribute.CRYPTO_KEY_LENGTH 
		    if(properties.getProperty(Attribute.CRYPTO_KEY_LENGTH) != null)
		    {
			String keyLengths = properties.getProperty(Attribute.CRYPTO_KEY_LENGTH);
		 	int pos = keyLengths.lastIndexOf('-');
			encodedKeyLength = Integer.parseInt(keyLengths.substring(pos+1)); 
			if(pos != -1)
			   keyLengthBits = Integer.parseInt(keyLengths.substring(0,pos));
		    }
		}
			

		// case 1 - if 'encryptionKey' is not set and 'encryptionKeyLength' is set, then use
		// the 'encryptionKeyLength' property value  as the keyLength in bits.
		// case 2 - 'encryptionKey' property is not set and 'encryptionKeyLength' is not set, then
		// use the defaults keylength:  56bits for DES, 168 for DESede and 128 for any other encryption
		// algorithm

		if (externalKey == null && create) {
			if(properties.getProperty(Attribute.CRYPTO_KEY_LENGTH) != null)
			{
				keyLengthBits = Integer.parseInt(properties.getProperty(Attribute.CRYPTO_KEY_LENGTH));
			}
			else if (cryptoAlgorithmShort.equals(DES)) {
				keyLengthBits = 56;
			} else if (cryptoAlgorithmShort.equals(DESede) || cryptoAlgorithmShort.equals(TripleDES)) {
				keyLengthBits = 168;

			} else {
				keyLengthBits = 128;
			}
		}

        // check the feedback mode
        String feedbackMode = cryptoAlgorithm.substring(firstSlashPos+1,lastSlashPos);

        if (!feedbackMode.equals("CBC") && !feedbackMode.equals("CFB") &&
            !feedbackMode.equals("ECB") && !feedbackMode.equals("OFB"))
    		throw StandardException.newException(SQLState.ENCRYPTION_BAD_FEEDBACKMODE, feedbackMode);

        // check the NoPadding mode is used
        String padding = cryptoAlgorithm.substring(lastSlashPos+1,cryptoAlgorithm.length());
        if (!padding.equals("NoPadding"))
    		throw StandardException.newException(SQLState.ENCRYPTION_BAD_PADDING, padding);

		Throwable t;
		try
		{
			if (cryptoProvider != null) {
				// provider package should be set by property
				if (Security.getProvider(cryptoProviderShort) == null)
				{
					action = 1;
					// add provider through privileged block.
					java.security.AccessController.doPrivileged(this);
				}
			}

			// need this to check the boot password
			messageDigest = MessageDigest.getInstance(MESSAGE_DIGEST);

			byte[] generatedKey;
			if (externalKey != null) {

				// incorrect to specify external key and boot password
				if (properties.getProperty(Attribute.BOOT_PASSWORD) != null)
					throw StandardException.newException(SQLState.SERVICE_WRONG_BOOT_PASSWORD);

				generatedKey = org.apache.derby.iapi.util.StringUtil.fromHexString(externalKey, 0, externalKey.length());

			} else {

				generatedKey = handleBootPassword(create, properties);
				if(create)
				   properties.put(Attribute.CRYPTO_KEY_LENGTH,keyLengthBits+"-"+generatedKey.length);
			}

			// Make a key and IV object out of the generated key
			mainSecretKey = generateKey(generatedKey);
			mainIV = generateIV(generatedKey);

			if (create)
			{
				properties.put(Attribute.DATA_ENCRYPTION, "true");

				// Set two new properties to allow for future changes to the log and data encryption
				// schemes. This property is introduced in version 10 , value starts at 1.
				properties.put(RawStoreFactory.DATA_ENCRYPT_ALGORITHM_VERSION,String.valueOf(1));
				properties.put(RawStoreFactory.LOG_ENCRYPT_ALGORITHM_VERSION,String.valueOf(1));
			}

			return;
		}
		catch (java.security.PrivilegedActionException  pae)
		{
			t = pae.getException();
		}
		catch (NoSuchAlgorithmException nsae)
		{
			t = nsae;
		}
		catch (SecurityException se)
		{
			t = se;
		} catch (LinkageError le) {
			t = le;
		} catch (ClassCastException cce) {
			t = cce;
		}

		throw StandardException.newException(SQLState.MISSING_ENCRYPTION_PROVIDER, t);
	}

	private byte[] handleBootPassword(boolean create, Properties properties)
		throws StandardException {

		String inputKey = properties.getProperty(Attribute.BOOT_PASSWORD);
		if (inputKey == null)
		{
			throw StandardException.newException(SQLState.SERVICE_WRONG_BOOT_PASSWORD);
		}

		byte[] bootPassword = StringUtil.getAsciiBytes(inputKey);

		if (bootPassword.length < CipherFactory.MIN_BOOTPASS_LENGTH)
		{
			String messageId = create ? SQLState.SERVICE_BOOT_PASSWORD_TOO_SHORT :
										SQLState.SERVICE_WRONG_BOOT_PASSWORD;

			throw StandardException.newException(messageId);
		}

		// Each database has its own unique encryption key that is
		// not known even to the user.  However, this key is masked
		// with the user input key and stored in the
		// services.properties file so that, with the user key, the
		// encryption key can easily be recovered.
		// To change the user encryption key to a database, simply
		// recover the unique real encryption key and masked it
		// with the new user key.

		byte[] generatedKey;

		if (create)
		{
			//
			generatedKey = generateUniqueBytes();

			properties.put(RawStoreFactory.ENCRYPTED_KEY, saveSecretKey(generatedKey, bootPassword));

		}
		else
		{
			generatedKey = getDatabaseSecretKey(properties, bootPassword, SQLState.SERVICE_WRONG_BOOT_PASSWORD);
		}

		return generatedKey;
	}

	public void stop()
	{

	}


	/**
		get the secretkey used for encryption and decryption when boot password mechanism is used for encryption
		Steps include 
		retrieve the stored key, decrypt the stored key and verify if the correct boot password was passed 
		There is a possibility that the decrypted key includes the original key and padded bytes in order to have
		been block size aligned during encryption phase. Hence extract the original key 
		
		@param	properties	properties to retrieve the encrypted key  
		@param	bootPassword	boot password used to connect to the encrypted database
		@param	errorState	errorstate to account for any errors during retrieval /creation of the secretKey
		@return the original unencrypted key bytes to use for encryption and decrytion   
		
         */
	private byte[] getDatabaseSecretKey(Properties properties, byte[] bootPassword, String errorState) throws StandardException {

		// recover the generated secret encryption key from the
		// services.properties file and the user key.
		String keyString = properties.getProperty(RawStoreFactory.ENCRYPTED_KEY);
		if (keyString == null)
			throw StandardException.newException(errorState);

		int encodedKeyCharLength = keyString.indexOf('-');

		if (encodedKeyCharLength == -1) // bad form
			throw StandardException.newException(errorState);

		int verifyKey = Integer.parseInt(keyString.substring(encodedKeyCharLength+1));
		byte[] generatedKey = decryptKey(keyString, encodedKeyCharLength, bootPassword);

		int checkKey = digest(generatedKey);

		if (checkKey != verifyKey)
			throw StandardException.newException(errorState);

		// if encodedKeyLength is not defined, then either it is an old version with no support for different
		// key sizes and padding except for defaults
	        byte[] result;	
		if(encodedKeyLength != 0)
		{
			result = new byte[encodedKeyLength];

			// extract the generated key without the padding bytes
			System.arraycopy(generatedKey,0,result,0,encodedKeyLength);
			return result;
		}

		return generatedKey;
	}

	private String saveSecretKey(byte[] secretKey, byte[] bootPassword) throws StandardException {
		String encryptedKey = encryptKey(secretKey, bootPassword);

		// make a verification key out of the message digest of
		// the generated key
		int verifyKey = digest(secretKey);

		return encryptedKey.concat("-" + verifyKey);

	}

	public String changeBootPassword(String changeString, Properties properties, CipherProvider verify)
		throws StandardException {

		// the new bootPassword is expected to be of the form
		// oldkey , newkey.
		int seperator = changeString.indexOf(',');
		if (seperator == -1)
			throw StandardException.newException(SQLState.WRONG_PASSWORD_CHANGE_FORMAT);

		String oldBP = changeString.substring(0, seperator).trim();
		byte[] oldBPAscii = StringUtil.getAsciiBytes(oldBP);
		if (oldBPAscii == null || oldBPAscii.length < CipherFactory.MIN_BOOTPASS_LENGTH)
			throw StandardException.newException(SQLState.WRONG_BOOT_PASSWORD);;

		String newBP = changeString.substring(seperator+1).trim();
		byte[] newBPAscii = StringUtil.getAsciiBytes(newBP);
		if (newBPAscii == null || newBPAscii.length < CipherFactory.MIN_BOOTPASS_LENGTH)
			throw StandardException.newException(SQLState.ILLEGAL_BP_LENGTH,
                new Integer(CipherFactory.MIN_BOOTPASS_LENGTH));

		// verify old key

		byte[] generatedKey = getDatabaseSecretKey(properties, oldBPAscii, SQLState.WRONG_BOOT_PASSWORD);

		// make sure the oldKey is correct
		byte[] IV = generateIV(generatedKey);

		if (!((JCECipherProvider) verify).verifyIV(IV))
			throw StandardException.newException(SQLState.WRONG_BOOT_PASSWORD);


		// Make the new key.  The generated key is unchanged, only the
		// encrypted key is changed.
		String newkey = saveSecretKey(generatedKey, newBPAscii);
		
		properties.put(Attribute.CRYPTO_KEY_LENGTH,keyLengthBits+"-"+encodedKeyLength);
		

		return saveSecretKey(generatedKey, newBPAscii);
	}

	/**
	 	perform actions with privileges enabled.
	 */
	public final Object run() throws StandardException, InstantiationException, IllegalAccessException {

		try {

			switch(action)
			{
				case 1:
					Security.addProvider(
					(Provider)(Class.forName(cryptoProvider).newInstance()));
					break;
				case 2:
					// SECURITY PERMISSION - MP1 and/or OP4
					// depends on the value of activePerms
					return activeFile.getRandomAccessFile(activePerms);

			}

		} catch (ClassNotFoundException cnfe) {
			throw StandardException.newException(SQLState.ENCRYPTION_NO_PROVIDER_CLASS,cryptoProvider);
		}
		catch(FileNotFoundException fnfe) {
			throw StandardException.newException(SQLState.ENCRYPTION_UNABLE_KEY_VERIFICATION,cryptoProvider);
		}
		return null;
	}



	/**
	    The database can be encrypted with an encryption key given in connection url.
	    For security reasons, this key is not made persistent in the database.

	    But it is necessary to verify the encryption key when booting the database if it is similar
	    to the one used when creating the database
	    This needs to happen before we access the data/logs to avoid the risk of corrupting the 
	    database because of a wrong encryption key.

	    This method performs the steps necessary to verify the encryption key if an external
	    encryption key is given.

	    At database creation, 4k of random data is generated using SecureRandom and MD5 is used
	    to compute the checksum for the random data thus generated.  This 4k page of random data
	    is then encrypted using the encryption key. The checksum of unencrypted data and
	    encrypted data is made persistent in the database in file by name given by
	    Attribute.CRYPTO_EXTERNAL_KEY_VERIFYFILE (verifyKey.dat). This file exists directly under the
	    database root directory.

	    When trying to boot an existing encrypted database, the given encryption key is used to decrypt
	    the data in the verifyKey.dat and the checksum is calculated and compared against the original
	    stored checksum. If these checksums dont match an exception is thrown.

	    Please note, this process of verifying the key  does not provide any added security but only is 
	    intended to allow to fail gracefully if a wrong encryption key is used

	    @return StandardException is thrown if there are any problems during the process of verification
	    		of the encryption key or if there is any mismatch of the encryption key.

	 */

	public void verifyKey(boolean create, StorageFactory sf, Properties properties)
		throws StandardException
	{

		if(properties.getProperty(Attribute.CRYPTO_EXTERNAL_KEY) == null)
			return;

		// if firstTime ( ie during creation of database, initial key used )
		// In order to allow for verifying the external key for future database boot,
		// generate random 4k of data and store the encrypted random data and the checksum
		// using MD5 of the unencrypted data. That way, on next database boot a check is performed
		// to verify if the key is the same as used when the database was created

		StorageRandomAccessFile verifyKeyFile = null;
		byte[] data = new byte[VERIFYKEY_DATALEN];
		try
		{
			if(create)
			{
				getSecureRandom().nextBytes(data);
				// get the checksum
				byte[] checksum = getMD5Checksum(data);

				CipherProvider tmpCipherProvider = createNewCipher(ENCRYPT,mainSecretKey,mainIV);
				tmpCipherProvider.encrypt(data, 0, data.length, data, 0);
				// openFileForWrite
				verifyKeyFile = privAccessFile(sf,Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE,"rw");
				// write the checksum length as int, and then the checksum and then the encrypted data
				verifyKeyFile.writeInt(checksum.length);
				verifyKeyFile.write(checksum);
				verifyKeyFile.write(data);
				verifyKeyFile.sync(true);
			}
			else
			{
				// open file for reading only
				verifyKeyFile = privAccessFile(sf,Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE,"r");
				// then read the checksum length 
				int checksumLen = verifyKeyFile.readInt();

				byte[] originalChecksum = new byte[checksumLen];
				verifyKeyFile.readFully(originalChecksum);

				verifyKeyFile.readFully(data);

				// decrypt data with key
				CipherProvider tmpCipherProvider = createNewCipher(DECRYPT,mainSecretKey,mainIV);
				tmpCipherProvider.decrypt(data, 0, data.length, data, 0);

				byte[] verifyChecksum = getMD5Checksum(data);

				if(!MessageDigest.isEqual(originalChecksum,verifyChecksum))
				{
					throw StandardException.newException(SQLState.ENCRYPTION_BAD_EXTERNAL_KEY);
				}

			}
		}
		catch(IOException ioe)
		{
			throw StandardException.newException(SQLState.ENCRYPTION_UNABLE_KEY_VERIFICATION,ioe);
		}
		finally
		{
			try
			{
				if(verifyKeyFile != null)
					verifyKeyFile.close();
			}
			catch(IOException ioee)
			{
				throw StandardException.newException(SQLState.ENCRYPTION_UNABLE_KEY_VERIFICATION,ioee);
			}
		}
		return ;
	}


	/**
		Use MD5 MessageDigest algorithm to generate checksum
		@param data	data to be used to compute the hash value
		@return returns the hash value computed using the data

	 */
	private byte[] getMD5Checksum(byte[] data)
		throws StandardException
	{
		try
		{
			// get the checksum
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			return md5.digest(data);
		}
		catch(NoSuchAlgorithmException nsae)
		{
			throw StandardException.newException(SQLState.ENCRYPTION_BAD_ALG_FORMAT,MESSAGE_DIGEST);
		}

	}


	/**
	 	access a file for either read/write
	 	@param storageFactory	factory used for io access
	 	@param	fileName		name of the file to create and open for write
 							The file will be created directly under the database root directory
		@param	filePerms		file permissions, if "rw" open file with read and write permissions
							    if "r" , open file with read permissions
	 	@return	StorageRandomAccessFile returns file with fileName for writing
		@exception IOException Any exception during accessing the file for read/write
	 */
	private StorageRandomAccessFile privAccessFile(StorageFactory storageFactory,String fileName,String filePerms)
		throws java.io.IOException
	{
		StorageFile verifyKeyFile = storageFactory.newStorageFile("",fileName);
		activeFile  = verifyKeyFile;
		this.action = 2;
		activePerms = filePerms;
	    try
        {
			return (StorageRandomAccessFile)java.security.AccessController.doPrivileged(this);
		}
		catch( java.security.PrivilegedActionException pae)
		{
			throw (java.io.IOException)pae.getException();
		}
	}


}
