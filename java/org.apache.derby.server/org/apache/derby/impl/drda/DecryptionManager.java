/*

   Derby - Class org.apache.derby.impl.drda.DecryptionManager

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

package org.apache.derby.impl.drda;

import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.spec.AlgorithmParameterSpec;
import java.sql.SQLException;
import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.interfaces.DHPublicKey;
import javax.crypto.spec.DHParameterSpec;
import javax.crypto.spec.DHPublicKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * This class is used to decrypt password and/or userid.
 * It uses Diffie_Hellman algorithm to get the publick key and secret key, and then
 * DES encryption is done using certain token (based on security mechanism) and 
 * this side's own public key. Basically, this class is called when using a security
 * mechanism that encrypts user ID and password (eusridpwd). This class uses IBM JCE
 * to do Diffie_Hellman algorithm and DES encryption.
 */

class DecryptionManager
{
  // DRDA's Diffie-Hellman agreed public value: prime.
  private static final byte modulusBytes__[] = {
    (byte)0xC6, (byte)0x21, (byte)0x12, (byte)0xD7,
    (byte)0x3E, (byte)0xE6, (byte)0x13, (byte)0xF0,
    (byte)0x94, (byte)0x7A, (byte)0xB3, (byte)0x1F,
    (byte)0x0F, (byte)0x68, (byte)0x46, (byte)0xA1,
    (byte)0xBF, (byte)0xF5, (byte)0xB3, (byte)0xA4,
    (byte)0xCA, (byte)0x0D, (byte)0x60, (byte)0xBC,
    (byte)0x1E, (byte)0x4C, (byte)0x7A, (byte)0x0D,
    (byte)0x8C, (byte)0x16, (byte)0xB3, (byte)0xE3
  };

  //the prime value in BigInteger form. It has to be in BigInteger form because this
  //is the form used in JCE library.
  private static final BigInteger modulus__
  = new BigInteger (1, modulusBytes__);

  //  DRDA's Diffie-Hellman agreed public value: base.
  private static final byte baseBytes__[] = {
    (byte)0x46, (byte)0x90, (byte)0xFA, (byte)0x1F,
    (byte)0x7B, (byte)0x9E, (byte)0x1D, (byte)0x44,
    (byte)0x42, (byte)0xC8, (byte)0x6C, (byte)0x91,
    (byte)0x14, (byte)0x60, (byte)0x3F, (byte)0xDE,
    (byte)0xCF, (byte)0x07, (byte)0x1E, (byte)0xDC,
    (byte)0xEC, (byte)0x5F, (byte)0x62, (byte)0x6E,
    (byte)0x21, (byte)0xE2, (byte)0x56, (byte)0xAE,
    (byte)0xD9, (byte)0xEA, (byte)0x34, (byte)0xE4
  };

  // The base value in BigInteger form. It has to be in BigInteger form because
  //this is the form used in IBM JCE library.
  private static final BigInteger base__ =
    new BigInteger (1, baseBytes__);

  //DRDA's Diffie-Hellman agreed exponential length
  private static final int exponential_length__ = 255;

  private KeyPairGenerator keyPairGenerator_;
  private KeyPair keyPair_;
  private KeyAgreement keyAgreement_;
  private DHParameterSpec paramSpec_;

  // Random Number Generator (PRNG) Algorithm
  private final static String SHA_1_PRNG_ALGORITHM = "SHA1PRNG";
  private final static int SECMEC_USRSSBPWD_SEED_LEN = 8;  // Seed length

  /**
   * EncryptionManager constructor. In this constructor,DHParameterSpec,
   * KeyPairGenerator, KeyPair, and KeyAgreement  are initialized.
   *
   * @throws SQLException that wraps any error
   */
  DecryptionManager () throws SQLException
  {
    try {
      if (java.security.Security.getProvider ("IBMJCE") == null) // IBMJCE is not installed, install it.
      {
          Class<?> clazz = Class.forName("IBMJCE");
          java.security.Security.addProvider ((java.security.Provider) clazz.getConstructor().newInstance());
      }
      paramSpec_ = new DHParameterSpec (modulus__, base__, exponential_length__);
      keyPairGenerator_ = KeyPairGenerator.getInstance ("DH", "IBMJCE");
      keyPairGenerator_.initialize ((AlgorithmParameterSpec)paramSpec_);
      keyPair_ = keyPairGenerator_.generateKeyPair();
      keyAgreement_ = KeyAgreement.getInstance ("DH", "IBMJCE");
      keyAgreement_.init (keyPair_.getPrivate());
    }
    catch (java.lang.ClassNotFoundException e) {
      throw new SQLException ("java.lang.ClassNotFoundException is caught" +
                              " when initializing EncryptionManager '" + e.getMessage() + "'");
    }
    catch (java.lang.IllegalAccessException e) {
      throw new SQLException ("java.lang.IllegalAccessException is caught" +
                              " when initializing EncryptionManager '" + e.getMessage() + "'");
    }
    catch (java.lang.InstantiationException e) {
      throw new SQLException ("java.lang.InstantiationException is caught" +
                              " when initializing EncryptionManager '" + e.getMessage() + "'");
    }
    catch (java.lang.NoSuchMethodException e) {
      throw new SQLException ("java.lang.NoSuchMethodException is caught" +
                              " when initializing EncryptionManager '" + e.getMessage() + "'");
    }
    catch (java.lang.reflect.InvocationTargetException e) {
      throw new SQLException ("java.lang.reflect.InvocationTargetException is caught" +
                              " when initializing EncryptionManager '" + e.getMessage() + "'");
    }
    catch (java.security.NoSuchProviderException e) {
      throw new SQLException ("java.security.NoSuchProviderException is caught" +
                              " when initializing EncryptionManager '" + e.getMessage() + "'");
    }
    catch (java.security.NoSuchAlgorithmException e) {
      throw new SQLException ("java.security.NoSuchAlgorithmException is caught" +
                  " when initializing EncryptionManager '" + e.getMessage() + "'");
    }
    catch (java.security.InvalidAlgorithmParameterException e) {
      throw new SQLException ("java.security.InvalidAlgorithmParameterException is caught" +
                  " when initializing EncryptionManager '" + e.getMessage() + "'");
    }

    catch (java.security.InvalidKeyException e) {
      throw new SQLException ("java.security.InvalidKeyException is caught" +
                  " when initializing EncryptionManager '" + e.getMessage() + "'");
    }
  }

  /**
   * This method generates the public key and returns it. This
   * shared public key is the application server's connection key and will
   * be exchanged with the application requester's connection key. This connection
   * key will be put in the sectkn in ACCSECRD command and send to the application
   * requester.
   *
   * @return  a byte array that is the application server's public key
   */
  public byte[] obtainPublicKey ()
  {
    //The encoded public key
    byte[] publicEnc =   keyPair_.getPublic().getEncoded();

    //we need to get the plain form public key because DRDA accepts plain form
    //public key only.
    BigInteger aPub = ((DHPublicKey) keyPair_.getPublic()).getY();
    byte[] aPubKey = aPub.toByteArray();

    //the following lines of code is to adjust the length of the key. PublicKey
    //in JCE is in the form of BigInteger and it's a signed value. When tranformed
    //to a Byte array form, normally this array is 32 bytes. However, if the
    //value happens to take up all 32 X 8 bits and it is positive, an extra
    //bit is needed and then a 33 byte array will be returned. Since DRDA can't
    //recogize the 33 byte key, we check the length here, if the length is 33,
    //we will just trim off the first byte (0) and get the rest of 32 bytes.
    if (aPubKey.length == 33 && aPubKey[0]==0) {
      byte[] newKey = new byte[32];
      for (int i=0; i < newKey.length; i++)
        newKey[i] = aPubKey[i+1];
      return newKey;
    }

    //the following lines of code is to adjust the length of the key. Occasionally,
    //the length of the public key is less than 32, the reason of this is that the 0 byte
    //in the beginning is somehow not returned. So we check the length here, if the length
    //is less than 32, we will pad 0 in the beginning to make the public key 32 bytes
    if (aPubKey.length < 32) {
      byte[] newKey = new byte[32];
      int i;
      for (i=0; i < 32-aPubKey.length; i++) {
        newKey[i] = 0;
      }
      for (int j=i; j<newKey.length; j++)
        newKey[j] = aPubKey[j-i];
      return newKey;
    }
    return aPubKey;
  }

  /**
   * This method is used to calculate the decryption token. DES encrypts the
   * data using a token and the generated shared private key. The token used
   * depends on the type of security mechanism being used:
   * USRENCPWD - The userid is used as the token. The USRID is zero-padded to
   * 8 bytes if less than 8 bytes or truncated to 8 bytes if greater than 8 bytes.
   * EUSRIDPWD - The middle 8 bytes of the server's connection key is used as
   * the token.  Decryption needs to use exactly the same token as encryption.
   *
   * @param  securityMechanism security mechanism
   * @param  initVector  userid or server(this side)'s connection key
   * @return byte[]  the decryption token
   */
  private byte[] calculateDecryptionToken (int securityMechanism, byte[] initVector)
  {
    byte[] token = new byte[8];

    //USRENCPWD, the userid is used as token
    if (securityMechanism == 7) {
      if (initVector.length < 8) { //shorter than 8 bytes, zero padded to 8 bytes
        System.arraycopy(initVector, 0, token, 0, initVector.length);
        for (int i=initVector.length; i<8; i++)
         token[i] = 0;
      }
      else {  //longer than 8 bytes, truncated to 8 bytes
        System.arraycopy(initVector, 0, token, 0, 8);
      }
    }
    //EUSRIDPWD - The middle 8 bytes of the server's connection key is used as
    //the token.
    else if (securityMechanism == 9) {
      for (int i = 0; i < 8; i++) {
        token[i] = initVector[i + 12];
      }
    }
    return token;
  }

  /**
   * This method generates a secret key using the application requester's
   * public key, and decrypts the usreid/password with the middle 8 bytes of
   * the generated secret key and a decryption token. Then it returns the
   * decrypted data in a byte array.
   *
   * @param cipherText        The byte array form userid/password to decrypt.
   * @param securityMechanism security mechanism
   * @param initVector        The byte array which is used to calculate the
   *                          decryption token for initializing the cipher
   * @param sourcePublicKey   application requester (encrypter)'s public key.
   * @return the decrypted data (plain text) in a byte array.
   */
  public byte[] decryptData (byte[] cipherText,
                 int    securityMechanism,
                 byte[] initVector,
                 byte[] sourcePublicKey) throws SQLException
  {
    byte[] plainText = null;
    byte[] token = calculateDecryptionToken (securityMechanism, initVector);
    try {

      //initiate a Diffie_Hellman KeyFactory object.
      KeyFactory keyFac = KeyFactory.getInstance ("DH", "IBMJCE");

      //Use server's public key to initiate a DHPublicKeySpec and then use
      //this DHPublicKeySpec to initiate a publicKey object
      BigInteger publicKey = new BigInteger (1, sourcePublicKey);
      DHPublicKeySpec dhKeySpec = new DHPublicKeySpec (publicKey, modulus__, base__);
      PublicKey pubKey = keyFac.generatePublic (dhKeySpec);

      //Execute the first phase of DH keyagreement protocal.
      keyAgreement_.doPhase (pubKey, true);

      //generate the shared secret key. The application requestor's shared secret
      //key should be exactly the same as the application server's shared secret
      //key
      byte[] sharedSecret = keyAgreement_.generateSecret();
      byte[] newKey = new byte[32];

      //We adjust the length here. If the length of secret key is 33 and the first byte is 0,
      //we trim off the frist byte. If the length of secret key is less than 32, we will
      //pad 0 to the beginning of the byte array tho make the secret key 32 bytes.
      if (sharedSecret.length == 33 && sharedSecret[0] == 0) {
        for (int i=0; i<newKey.length; i++)
          newKey[i] = sharedSecret[i+1];
      }
      if (sharedSecret.length < 32) {
        int i;
        for (i=0; i<(32 - sharedSecret.length); i++)
            newKey[i] = 0;
        for (int j=i; j<sharedSecret.length; j++)
             newKey[j] = sharedSecret[j-i];
      }

      //The Data Encryption Standard (DES) is going to be used to encrypt userid
      //and password. DES is a block cipher; it encrypts data in 64-bit blocks.
      //DRDA encryption uses DES CBC mode as defined by the FIPS standard
      //DES CBC requires an encryption key and an 8 byte token to encrypt the data.
      //The middle 8 bytes of Diffie-Hellman shared private key is used as the
      //encryption key. The following code retrieves middle 8 bytes of the shared
      //private key.
      byte[] key = new byte[8];

      //if secret key is not 32, we will use the adjust length secret key
      if (sharedSecret.length==32) {
        for (int i=0; i< 8;i++)
          key[i] = sharedSecret[i+12];
      }
      else if (sharedSecret.length==33 || sharedSecret.length < 32) {
        for (int i=0; i< 8;i++)
          key[i] = newKey[i+12];
      }
      else
        throw new SQLException ("sharedSecret key length error " + sharedSecret.length);

      // make parity bit right, even number of 1's
      byte temp;
      int changeParity;
      for (int i=0; i<8; i++)
      {
        temp = key[i];
        changeParity = 1;
        for (int j=0; j<8; j++)
        {
            if (temp < 0)
                changeParity = 1 - changeParity;
            temp = (byte) (temp << 1);
        }
        if (changeParity == 1)
        {
            if ((key[i] & 1) != 0)
                key[i] &= 0xfe;
            else
                key[i] |= 1;
        }
      }

      //use this encryption key to initiate a SecretKeySpec object
      SecretKeySpec desKey = new SecretKeySpec (key, "DES");

      //We use DES in CBC mode because this is the mode used in DRDA. The
      //encryption mode has to be consistent for encryption and decryption.
      //CBC mode requires an initialization vector(IV) parameter. In CBC mode
      //we need to initialize the Cipher object with an IV, which can be supplied
      // using the javax.crypto.spec.IvParameterSpec class.
      Cipher cipher= Cipher.getInstance ("DES/CBC/PKCS5Padding", "IBMJCE");

      //generate a IVParameterSpec object and use it to initiate the
      //Cipher object.
      IvParameterSpec ivParam = new IvParameterSpec (token);

      //initiate the Cipher using encryption mode, encryption key and the
      //IV parameter.
      cipher.init (javax.crypto.Cipher.DECRYPT_MODE, desKey,ivParam);

      //Execute the final phase of encryption
      plainText = cipher.doFinal (cipherText);
    }
    catch (java.security.NoSuchProviderException e) {
      throw new SQLException ("java.security.NoSuchProviderException is caught "
                  + "when encrypting data '" + e.getMessage() + "'");
    }
    catch (java.security.NoSuchAlgorithmException e) {
      throw new SQLException ("java.security.NoSuchAlgorithmException is caught "
                  + "when encrypting data '" + e.getMessage() + "'");
    }
    catch (java.security.spec.InvalidKeySpecException e) {
      throw new SQLException ("java.security.InvalidKeySpecException is caught "
                  + "when encrypting data");
    }
    catch (java.security.InvalidKeyException e) {
      throw new SQLException ("java.security.InvalidKeyException is caught "
                  + "when encrypting data '" + e.getMessage() + "'");
    }
    catch (javax.crypto.NoSuchPaddingException e) {
      throw new SQLException ("javax.crypto.NoSuchPaddingException is caught "
                  + "when encrypting data '" + e.getMessage() + "'");
    }
    catch (javax.crypto.BadPaddingException e) {
      throw new SQLException ("javax.crypto.BadPaddingException is caught "
                  + "when encrypting data '" + e.getMessage() + "'");
    }
    catch (java.security.InvalidAlgorithmParameterException e) {
      throw new SQLException ("java.security.InvalidAlgorithmParameterException is caught "
                  + "when encrypting data '" + e.getMessage() + "'");
    }
    catch (javax.crypto.IllegalBlockSizeException e) {
      throw new SQLException ("javax.crypto.IllegalBlockSizeException is caught "
                  + "when encrypting data '" + e.getMessage() + "'");
    }
    return plainText;
  }

    /**
     * This method generates an 8-Byte random seed.
     *
     * Required for the SECMEC_USRSSBPWD security mechanism
     *
     * @return a random 8-Byte seed.
     */
    protected static byte[] generateSeed() throws SQLException {
        java.security.SecureRandom secureRandom = null;
        try {
          // We're verifying that we can instantiate a randon number
          // generator (PRNG).
          secureRandom =
              java.security.SecureRandom.getInstance(SHA_1_PRNG_ALGORITHM);
        } catch (java.security.NoSuchAlgorithmException nsae) {
            throw new SQLException(
                    "java.security.NoSuchAlgorithmException is caught" +
                    " when initializing DecryptionManager '" +
                    nsae.getMessage() + "'");
        }
        byte randomSeedBytes[] = new byte[SECMEC_USRSSBPWD_SEED_LEN];
        secureRandom.setSeed(secureRandom.generateSeed(
                                        SECMEC_USRSSBPWD_SEED_LEN));
        secureRandom.nextBytes(randomSeedBytes);
        // Return the 8-byte generated random seed
        return randomSeedBytes;
    }

    /*********************************************************************
     * RESOLVE:                                                          *
     * The methods and static vars below should go into some 'shared'    *
     * package when the capability is put back in (StringUtil.java).     *
     *********************************************************************/

    private static char[] hex_table = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
                'a', 'b', 'c', 'd', 'e', 'f'
            };
    
    /**
        Convert a byte array to a String with a hexidecimal format.
        The String may be converted back to a byte array using fromHexString.
        <BR>
        For each byte (b) two characaters are generated, the first character
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        represents the high nibble (4 bits) in hexidecimal (<code>b &amp; 0xf0</code>),
        the second character represents the low nibble (<code>b &amp; 0x0f</code>).
        <BR>
        The byte at <code>data[offset]</code> is represented by the first two characters in the returned String.

        @param  data    byte array
        @param  offset  starting byte (zero based) to convert.
        @param  length  number of bytes to convert.

        @return the String (with hexidecimal format) form of the byte array
    */
    protected static String toHexString(byte[] data, int offset, int length)
    {
        StringBuffer s = new StringBuffer(length*2);
        int end = offset+length;

        for (int i = offset; i < end; i++)
        {
            int high_nibble = (data[i] & 0xf0) >>> 4;
            int low_nibble = (data[i] & 0x0f);
            s.append(hex_table[high_nibble]);
            s.append(hex_table[low_nibble]);
        }

        return s.toString();
    }

    /**
  
        Convert a string into a byte array in hex format.
        <BR>
        For each character (b) two bytes are generated, the first byte 
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        represents the high nibble (4 bits) in hexidecimal (<code>b &amp; 0xf0</code>),
        the second byte 
        represents the low nibble (<code>b &amp; 0x0f</code>).
        <BR>
        The character at <code>str.charAt(0)</code> is represented by the first two bytes 
        in the returned String.

        @param  str string
        @param  offset  starting character (zero based) to convert.
        @param  length  number of characters to convert.

        @return the byte[]  (with hexidecimal format) form of the string (str) 
    */
    protected static byte[] toHexByte(String str, int offset, int length)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        byte[] data = new byte[(length - offset) * 2];
        int end = offset+length;

        for (int i = offset; i < end; i++)
        {
            char ch = str.charAt(i);
            int high_nibble = (ch & 0xf0) >>> 4;
            int low_nibble = (ch & 0x0f);
            data[i] = (byte)high_nibble;
            data[i+1] = (byte)low_nibble;
        }
        return data;
    }
}
