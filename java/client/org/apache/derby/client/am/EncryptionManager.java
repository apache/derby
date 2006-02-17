/*

   Derby - Class org.apache.derby.client.am.EncryptionManager

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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

package org.apache.derby.client.am;

import java.security.Provider;
import java.security.Security;
import org.apache.derby.shared.common.reference.SQLState;


// This class is get used when using encrypted password and/or userid mechanism.
// The <b>EncryptionManager</b> classs uses Diffie_Hellman algorithm to get the publick key and
// secret key, and then DES encryption is done using certain token (based on security
// mechanism) and server side's public key. Basically, this class is called when using
// security mechanism User ID and encrypted password (usrencpwd) and Encrypted user ID and password
// (eusridpwd).
// This class uses JCE provider to do Diffie_Hellman algorithm and DES encryption,
// obtainPublicKey(), calculateEncryptionToken(int, byte[]) and encryptData(byte[], int, byte[], byte[])
// The agreed public value for the Diffie-Hellman prime is 256 bits
// and hence the encrytion will work only if the jce provider supports a 256 bits prime

public class EncryptionManager {
    transient Agent agent_; // for obtaining an exception log writer only

    // PROTOCOL's Diffie-Hellman agreed public value: prime.
    private static final byte modulusBytes__[] = {
        (byte) 0xC6, (byte) 0x21, (byte) 0x12, (byte) 0xD7,
        (byte) 0x3E, (byte) 0xE6, (byte) 0x13, (byte) 0xF0,
        (byte) 0x94, (byte) 0x7A, (byte) 0xB3, (byte) 0x1F,
        (byte) 0x0F, (byte) 0x68, (byte) 0x46, (byte) 0xA1,
        (byte) 0xBF, (byte) 0xF5, (byte) 0xB3, (byte) 0xA4,
        (byte) 0xCA, (byte) 0x0D, (byte) 0x60, (byte) 0xBC,
        (byte) 0x1E, (byte) 0x4C, (byte) 0x7A, (byte) 0x0D,
        (byte) 0x8C, (byte) 0x16, (byte) 0xB3, (byte) 0xE3
    };

    //the prime value in BigInteger form. It has to be in BigInteger form because this
    //is the form used in JCE library.
    private static final java.math.BigInteger modulus__
            = new java.math.BigInteger(1, modulusBytes__);

    //  PROTOCOL's Diffie-Hellman agreed public value: base.
    private static final byte baseBytes__[] = {
        (byte) 0x46, (byte) 0x90, (byte) 0xFA, (byte) 0x1F,
        (byte) 0x7B, (byte) 0x9E, (byte) 0x1D, (byte) 0x44,
        (byte) 0x42, (byte) 0xC8, (byte) 0x6C, (byte) 0x91,
        (byte) 0x14, (byte) 0x60, (byte) 0x3F, (byte) 0xDE,
        (byte) 0xCF, (byte) 0x07, (byte) 0x1E, (byte) 0xDC,
        (byte) 0xEC, (byte) 0x5F, (byte) 0x62, (byte) 0x6E,
        (byte) 0x21, (byte) 0xE2, (byte) 0x56, (byte) 0xAE,
        (byte) 0xD9, (byte) 0xEA, (byte) 0x34, (byte) 0xE4
    };

    // The base value in BigInteger form.
    private static final java.math.BigInteger base__ =
            new java.math.BigInteger(1, baseBytes__);

    //PROTOCOL's Diffie-Hellman agreed exponential length
    private static final int exponential_length__ = 255;

    private javax.crypto.spec.DHParameterSpec paramSpec_;
    private java.security.KeyPairGenerator keyPairGenerator_;
    private java.security.KeyPair keyPair_;
    private javax.crypto.KeyAgreement keyAgreement_;

    private byte[] token_; // init vector
    private byte[] secKey_; // security key
    private javax.crypto.SecretKeyFactory secretKeyFactory_ = null;
    private String providerName; // security provider name
    private Provider provider;

    // EncryptionManager constructor. In this constructor,DHParameterSpec,
    // KeyPairGenerator, KeyPair, and KeyAgreement  are initialized.
    public EncryptionManager(Agent agent) throws SqlException {
        agent_ = agent;
        try {
            // get a security provider that supports the diffie helman key agreement algorithm
            Provider[] list = Security.getProviders("KeyAgreement.DH");
            if (list == null) {
                throw new java.security.NoSuchProviderException();
            }
            provider = list[0];
            providerName = provider.getName();

            java.security.Security.addProvider((java.security.Provider) provider);

            paramSpec_ = new javax.crypto.spec.DHParameterSpec(modulus__, base__, exponential_length__);
            keyPairGenerator_ = java.security.KeyPairGenerator.getInstance("DH", providerName);
            keyPairGenerator_.initialize(paramSpec_);
            keyPair_ = keyPairGenerator_.generateKeyPair();
            keyAgreement_ = javax.crypto.KeyAgreement.getInstance("DH", providerName);
            keyAgreement_.init(keyPair_.getPrivate());
        } catch (java.security.GeneralSecurityException e) {
            throw new SqlException(agent_.logWriter_, 
                new MessageId(SQLState.SECURITY_EXCEPTION_ENCOUNTERED), e); 
        }
    }

    // This method generates the public key and returns it. This
    // shared public key is the application requester's connection key and will
    // be exchanged with the application server's connection key. This connection
    // key will be put in the sectkn in ACCSEC command and send to the application
    // server.
    // @param   null
    // @return  a byte array that is the application requester's public key
    public byte[] obtainPublicKey() {

        //we need to get the plain form public key because PROTOCOL accepts plain form
        //public key only.
        java.math.BigInteger aPub = ((javax.crypto.interfaces.DHPublicKey) keyPair_.getPublic()).getY();
        byte[] aPubKey = aPub.toByteArray();

        //the following lines of code is to adjust the length of the key. PublicKey
        //in JCE is in the form of BigInteger and it's a signed value. When tranformed
        //to a Byte array form, normally this array is 32 bytes. However, if the
        //value happens to take up all 32 X 8 bits and it is positive, an extra
        //bit is needed and then a 33 byte array will be returned. Since PROTOCOL can't
        //recogize the 33 byte key, we check the length here, if the length is 33,
        //we will just trim off the first byte (0) and get the rest of 32 bytes.
        if (aPubKey.length == 33 && aPubKey[0] == 0) {
            //System.out.println ("Adjust length");
            byte[] newKey = new byte[32];
            for (int i = 0; i < newKey.length; i++) {
                newKey[i] = aPubKey[i + 1];
            }
            return newKey;
        }

        //the following lines of code is to adjust the length of the key. Occasionally,
        //the length of the public key is less than 32, the reason of this is that the 0 byte
        //in the beginning is somehow not returned. So we check the length here, if the length
        //is less than 32, we will pad 0 in the beginning to make the public key 32 bytes
        if (aPubKey.length < 32) {
            byte[] newKey = new byte[32];
            int i;
            for (i = 0; i < 32 - aPubKey.length; i++) {
                newKey[i] = 0;
            }
            for (int j = i; j < newKey.length; j++) {
                newKey[j] = aPubKey[j - i];
            }
            return newKey;
        }
        return aPubKey;
    }

    // This method is used to calculate the encryption token. DES encrypts the
    // data using a token and the generated shared private key. The token used
    // depends on the type of security mechanism being used:
    // USRENCPWD - The userid is used as the token. The USRID is zero-padded to
    // 8 bytes if less than 8 bytes or truncated to 8 bytes if greater than 8 bytes.
    // EUSRIDPWD - The middle 8 bytes of the server's connection key is used as
    // the token.
    // @param  int     securityMechanism
    // @param  byte[]  userid or server's connection key
    // @return byte[]  the encryption token
    private byte[] calculateEncryptionToken(int securityMechanism, byte[] initVector) {
        byte[] token = new byte[8];

        //USRENCPWD, the userid is used as token
        if (securityMechanism == 7) {
            if (initVector.length < 8) { //shorter than 8 bytes, zero padded to 8 bytes
                for (int i = 0; i < initVector.length; i++) {
                    token[i] = initVector[i];
                }
                for (int i = initVector.length; i < 8; i++) {
                    token[i] = 0;
                }
            } else {  //longer than 8 bytes, truncated to 8 bytes
                for (int i = 0; i < 8; i++) {
                    token[i] = initVector[i];
                }
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

    //JDK 1.4 has a parity check on the DES encryption key. Each byte needs to have an odd number
    //of "1"s in it, and this is required by DES. Otherwise JDK 1.4 throws InvalidKeyException.
    //Older JDK doesn't check this. In order to make encryption work with JDK1.4, we are going to
    //check each of the 8 byte of our key and flip the last bit if it has even number of 1s.
    private void keyParityCheck(byte[] key) throws SqlException {
        byte temp;
        int changeParity;
        if (key.length != 8) {
            throw new SqlException(agent_.logWriter_, 
                new MessageId(SQLState.DES_KEY_HAS_WRONG_LENGTH), 
                new Integer(8), new Integer(key.length)); 
                        
        }
        for (int i = 0; i < 8; i++) {
            temp = key[i];
            changeParity = 1;
            for (int j = 0; j < 8; j++) {
                if (temp < 0) {
                    changeParity = 1 - changeParity;
                }
                temp = (byte) (temp << 1);
            }
            if (changeParity == 1) {
                if ((key[i] & 1) != 0) {
                    key[i] &= 0xfe;
                } else {
                    key[i] |= 1;
                }
            }
        }
    }

    // This method generates a secret key using the application server's
    // public key
    private byte[] generatePrivateKey(byte[] targetPublicKey) throws SqlException {
        try {

            //initiate a Diffie_Hellman KeyFactory object.
            java.security.KeyFactory keyFac = java.security.KeyFactory.getInstance("DH", provider);

            //Use server's public key to initiate a DHPublicKeySpec and then use
            //this DHPublicKeySpec to initiate a publicKey object
            java.math.BigInteger publicKey = new java.math.BigInteger(1, targetPublicKey);
            javax.crypto.spec.DHPublicKeySpec dhKeySpec =
                    new javax.crypto.spec.DHPublicKeySpec(publicKey, modulus__, base__);
            java.security.PublicKey pubKey = keyFac.generatePublic(dhKeySpec);

            //Execute the first phase of DH keyagreement protocal.
            keyAgreement_.doPhase(pubKey, true);

            //generate the shared secret key. The application requestor's shared secret
            //key should be exactly the same as the application server's shared secret
            //key
            byte[] sharedSecret = keyAgreement_.generateSecret();
            byte[] newKey = new byte[32];

            //We adjust the length here. If the length of secret key is 33 and the first byte is 0,
            //we trim off the frist byte. If the length of secret key is less than 32, we will
            //pad 0 to the beginning of the byte array tho make the secret key 32 bytes.
            if (sharedSecret.length == 33 && sharedSecret[0] == 0) {
                for (int i = 0; i < newKey.length; i++) {
                    newKey[i] = sharedSecret[i + 1];
                }

            }
            if (sharedSecret.length < 32) {
                int i;
                for (i = 0; i < (32 - sharedSecret.length); i++) {
                    newKey[i] = 0;
                }
                for (int j = i; j < sharedSecret.length; j++) {
                    newKey[j] = sharedSecret[j - i];
                }
            }

            //The Data Encryption Standard (DES) is going to be used to encrypt userid
            //and password. DES is a block cipher; it encrypts data in 64-bit blocks.
            //PROTOCOL encryption uses DES CBC mode as defined by the FIPS standard
            //DES CBC requires an encryption key and an 8 byte token to encrypt the data.
            //The middle 8 bytes of Diffie-Hellman shared private key is used as the
            //encryption key. The following code retrieves middle 8 bytes of the shared
            //private key.
            byte[] key = new byte[8];

            //if secret key is not 32, we will use the adjust length secret key
            if (sharedSecret.length == 32) {
                for (int i = 0; i < 8; i++) {
                    key[i] = sharedSecret[i + 12];
                }
            } else if (sharedSecret.length == 33 || sharedSecret.length < 32) {
                for (int i = 0; i < 8; i++) {
                    key[i] = newKey[i + 12];
                }
            } else {
                throw new SqlException(agent_.logWriter_, 
                    new MessageId(SQLState.SHARED_KEY_LENGTH_ERROR),
                    new Integer(sharedSecret.length)); 
            }

            //we do parity check here and flip the parity bit if the byte has even number of 1s
            keyParityCheck(key);
            return key;
        }
        catch (java.security.GeneralSecurityException e) {
            throw new SqlException(agent_.logWriter_, 
                new MessageId(SQLState.SECURITY_EXCEPTION_ENCOUNTERED), e);
        }
    }

    // This method encrypts the usreid/password with the middle 8 bytes of
    // the generated secret key and an encryption token. Then it returns the
    // encrypted data in a byte array.
    // plainText   The byte array form userid/password to encrypt.
    // initVector  The byte array which is used to calculate the
    //                             encryption token.
    // targetPublicKey   DERBY' public key.
    // Returns the encrypted data in a byte array.
    public byte[] encryptData(byte[] plainText,
                              int securityMechanism,
                              byte[] initVector,
                              byte[] targetPublicKey) throws SqlException {

        byte[] cipherText = null;
        java.security.Key key = null;

        if (token_ == null) {
            token_ = calculateEncryptionToken(securityMechanism, initVector);
        }

        try {
            if (secKey_ == null) {
                //use this encryption key to initiate a SecretKeySpec object
                secKey_ = generatePrivateKey(targetPublicKey);
                javax.crypto.spec.SecretKeySpec desKey = new javax.crypto.spec.SecretKeySpec(secKey_, "DES");
                key = desKey;
            } else {
                //use this encryption key to initiate a SecretKeySpec object
                javax.crypto.spec.DESKeySpec desKey = new javax.crypto.spec.DESKeySpec(secKey_);
                if (secretKeyFactory_ == null) {
                    secretKeyFactory_ = javax.crypto.SecretKeyFactory.getInstance("DES", providerName);
                }
                key = secretKeyFactory_.generateSecret(desKey);
            }

            //We use DES in CBC mode because this is the mode used in PROTOCOL. The
            //encryption mode has to be consistent for encryption and decryption.
            //CBC mode requires an initialization vector(IV) parameter. In CBC mode
            //we need to initialize the Cipher object with an IV, which can be supplied
            // using the javax.crypto.spec.IvParameterSpec class.
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("DES/CBC/PKCS5Padding", providerName);

            //generate a IVParameterSpec object and use it to initiate the
            //Cipher object.
            javax.crypto.spec.IvParameterSpec ivParam = new javax.crypto.spec.IvParameterSpec(token_);

            //initiate the Cipher using encryption mode, encryption key and the
            //IV parameter.
            cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, key, ivParam);

            //Execute the final phase of encryption
            cipherText = cipher.doFinal(plainText);
        } catch (javax.crypto.NoSuchPaddingException e) {
            throw new SqlException(agent_.logWriter_, 
                        new MessageId(SQLState.CRYPTO_NO_SUCH_PADDING)); 
        } catch (javax.crypto.BadPaddingException e) {
            throw new SqlException(agent_.logWriter_, 
                        new MessageId(SQLState.CRYPTO_BAD_PADDING)); 
        } catch (javax.crypto.IllegalBlockSizeException e) {
            throw new SqlException(agent_.logWriter_, 
                        new MessageId(SQLState.CRYPTO_ILLEGAL_BLOCK_SIZE)); 
        } catch (java.security.GeneralSecurityException e) {
            throw new SqlException(agent_.logWriter_, 
                new MessageId(SQLState.SECURITY_EXCEPTION_ENCOUNTERED), e); 
        }

        return cipherText;
    }


    // This method decrypts the usreid/password with the middle 8 bytes of
    // the generated secret key and an encryption token. Then it returns the
    // decrypted data in a byte array.
    // plainText   The byte array form userid/password to encrypt.
    // initVector  The byte array which is used to calculate the
    //                             encryption token.
    // targetPublicKey   DERBY' public key.
    // Returns the decrypted data in a byte array.
    public byte[] decryptData(byte[] cipherText,
                              int securityMechanism,
                              byte[] initVector,
                              byte[] targetPublicKey) throws SqlException {

        byte[] plainText = null;
        java.security.Key key = null;

        if (token_ == null) {
            token_ = calculateEncryptionToken(securityMechanism, initVector);
        }

        try {
            if (secKey_ == null) {
                //use this encryption key to initiate a SecretKeySpec object
                secKey_ = generatePrivateKey(targetPublicKey);
                javax.crypto.spec.SecretKeySpec desKey = new javax.crypto.spec.SecretKeySpec(secKey_, "DES");
                key = desKey;
            } else {
                //use this encryption key to initiate a SecretKeySpec object
                javax.crypto.spec.DESKeySpec desKey = new javax.crypto.spec.DESKeySpec(secKey_);
                if (secretKeyFactory_ == null) {
                    secretKeyFactory_ = javax.crypto.SecretKeyFactory.getInstance("DES", providerName);
                }
                key = secretKeyFactory_.generateSecret(desKey);
            }

            //We use DES in CBC mode because this is the mode used in PROTOCOL. The
            //encryption mode has to be consistent for encryption and decryption.
            //CBC mode requires an initialization vector(IV) parameter. In CBC mode
            //we need to initialize the Cipher object with an IV, which can be supplied
            // using the javax.crypto.spec.IvParameterSpec class.
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("DES/CBC/PKCS5Padding", providerName);

            //generate a IVParameterSpec object and use it to initiate the
            //Cipher object.
            javax.crypto.spec.IvParameterSpec ivParam = new javax.crypto.spec.IvParameterSpec(token_);

            //initiate the Cipher using encryption mode, encryption key and the
            //IV parameter.
            cipher.init(javax.crypto.Cipher.DECRYPT_MODE, key, ivParam);

            //Execute the final phase of encryption
            plainText = cipher.doFinal(cipherText);
        } catch (javax.crypto.NoSuchPaddingException e) {
            throw new SqlException(agent_.logWriter_, 
                        new MessageId(SQLState.CRYPTO_NO_SUCH_PADDING)); 
        } catch (javax.crypto.BadPaddingException e) {
            throw new SqlException(agent_.logWriter_, 
                        new MessageId(SQLState.CRYPTO_BAD_PADDING)); 
        } catch (javax.crypto.IllegalBlockSizeException e) {
            throw new SqlException(agent_.logWriter_, 
                        new MessageId(SQLState.CRYPTO_ILLEGAL_BLOCK_SIZE)); 
        } catch (java.security.GeneralSecurityException e) {
            throw new SqlException(agent_.logWriter_, 
                new MessageId(SQLState.SECURITY_EXCEPTION_ENCOUNTERED), e); 
        }
        return plainText;
    }

    public void setInitVector(byte[] initVector) {
        token_ = initVector;
    }

    public void setSecKey(byte[] secKey) {
        secKey_ = secKey;
    }

    public void resetSecurityKeys() {
        token_ = null;
        secKey_ = null;
    }

}

