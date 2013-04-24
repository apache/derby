/*

   Derby - Class org.apache.derby.client.am.EncryptionManager

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

package org.apache.derby.client.am;

import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyAgreement;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.interfaces.DHPublicKey;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.DHParameterSpec;
import javax.crypto.spec.DHPublicKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;

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
//
// This class also have methods for the SECMEC_USRSSBPWD security mechanism.

public class EncryptionManager {
     // for obtaining an exception log writer only
    private transient Agent agent_;

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
    private static final BigInteger modulus__
            = new BigInteger(1, modulusBytes__);

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
    private static final BigInteger base__ =
            new BigInteger(1, baseBytes__);

    //PROTOCOL's Diffie-Hellman agreed exponential length
    private static final int exponential_length__ = 255;

    private DHParameterSpec paramSpec_;
    private KeyPairGenerator keyPairGenerator_;
    private KeyPair keyPair_;
    private KeyAgreement keyAgreement_;

    private byte[] token_; // init vector
    private byte[] secKey_; // security key
    private SecretKeyFactory secretKeyFactory_ = null;
    private String providerName; // security provider name
    private Provider provider;

    // Required for SECMEC_USRSSBPWD DRDA security mechanism
    // NOTE: In a next incarnation, these constants are being moved
    // to a dedicated/specialized SecMec_USRSSBPWD class implementing
    // a SecurityMechanism interface.
    private MessageDigest messageDigest = null;
    private SecureRandom secureRandom = null;
    private final static int SECMEC_USRSSBPWD_SEED_LEN = 8;  // Seed length
    // PWSEQs's 8-byte value constant - See DRDA Vol 3
    private static final byte SECMEC_USRSSBPWD_PWDSEQS[] = {
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01
    };
    // Random Number Generator (PRNG) Algorithm
    private final static String SHA_1_PRNG_ALGORITHM = "SHA1PRNG";
    public final static String SHA_1_DIGEST_ALGORITHM = "SHA-1";

    // EncryptionManager constructor. In this constructor,DHParameterSpec,
    // KeyPairGenerator, KeyPair, and KeyAgreement  are initialized.
    public EncryptionManager(Agent agent) throws SqlException {
        agent_ = agent;
        try {
            // get a security provider that supports the diffie helman key agreement algorithm
            Provider[] list = Security.getProviders("KeyAgreement.DH");
            if (list == null) {
                throw new NoSuchProviderException();
            }
            provider = list[0];
            providerName = provider.getName();
            paramSpec_ =
                new DHParameterSpec(modulus__, base__, exponential_length__);
            keyPairGenerator_ =
                KeyPairGenerator.getInstance("DH", providerName);
            keyPairGenerator_.initialize(paramSpec_);
            keyPair_ = keyPairGenerator_.generateKeyPair();
            keyAgreement_ = KeyAgreement.getInstance("DH", providerName);
            keyAgreement_.init(keyPair_.getPrivate());
        } catch (GeneralSecurityException e) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.SECURITY_EXCEPTION_ENCOUNTERED), e); 
        }
    }

    // Retrieve a particular instance of the Encryption manager for a given
    // (Messsage Digest) algorithm. This is currently required for the
    // SECMEC_USRSSBPWD (strong password substitute) security mechanism.
    // 
    // NOTE: This is temporary logic as the encryption manager is being
    // rewritten into a DRDASecurityManager and have some of the
    // client/engine common logic moved to the Derby 'shared' package.
    public EncryptionManager(Agent agent, String algorithm) throws SqlException {
        agent_ = agent;
        try {
            // Instantiate the encryption manager for the passed-in security
            // algorithm and this from the default provider
            // NOTE: We're only dealing with Message Digest algorithms for now.
            messageDigest = MessageDigest.getInstance(algorithm);
            // We're also verifying that we can instantiate a randon number
            // generator (PRNG).
            secureRandom =
                SecureRandom.getInstance(SHA_1_PRNG_ALGORITHM);
        } catch (NoSuchAlgorithmException nsae) {
            // The following exception should not be raised for SHA-1 type of
            // message digest as we've already verified during boot-up that this
            // algorithm was available as part of the JRE (since BUILT-IN
            // authentication requires it); but we still raise the exception if
            // a client were to request a different algorithm.
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.SECURITY_EXCEPTION_ENCOUNTERED),
                                    nsae); 
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
        BigInteger aPub = ((DHPublicKey) keyPair_.getPublic()).getY();
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
                System.arraycopy(initVector, 0, token, 0, initVector.length);
                for (int i = initVector.length; i < 8; i++) {
                    token[i] = 0;
                }
            } else {  //longer than 8 bytes, truncated to 8 bytes
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

    //JDK 1.4 has a parity check on the DES encryption key. Each byte needs to have an odd number
    //of "1"s in it, and this is required by DES. Otherwise JDK 1.4 throws InvalidKeyException.
    //Older JDK doesn't check this. In order to make encryption work with JDK1.4, we are going to
    //check each of the 8 byte of our key and flip the last bit if it has even number of 1s.
    private void keyParityCheck(byte[] key) throws SqlException {
        byte temp;
        int changeParity;
        if (key.length != 8) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.DES_KEY_HAS_WRONG_LENGTH), 
                8, key.length);
                        
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
            KeyFactory keyFac = KeyFactory.getInstance("DH", provider);

            //Use server's public key to initiate a DHPublicKeySpec and then use
            //this DHPublicKeySpec to initiate a publicKey object
            BigInteger publicKey = new BigInteger(1, targetPublicKey);
            DHPublicKeySpec dhKeySpec =
                    new DHPublicKeySpec(publicKey, modulus__, base__);
            PublicKey pubKey = keyFac.generatePublic(dhKeySpec);

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
                    new ClientMessageId(SQLState.SHARED_KEY_LENGTH_ERROR),
                    sharedSecret.length);
            }

            //we do parity check here and flip the parity bit if the byte has even number of 1s
            keyParityCheck(key);
            return key;
        }
        catch (GeneralSecurityException e) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.SECURITY_EXCEPTION_ENCOUNTERED), e);
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
        Key key = null;

        if (token_ == null) {
            token_ = calculateEncryptionToken(securityMechanism, initVector);
        }

        try {
            if (secKey_ == null) {
                //use this encryption key to initiate a SecretKeySpec object
                secKey_ = generatePrivateKey(targetPublicKey);
                SecretKeySpec desKey = new SecretKeySpec(secKey_, "DES");
                key = desKey;
            } else {
                //use this encryption key to initiate a SecretKeySpec object
                DESKeySpec desKey = new DESKeySpec(secKey_);
                if (secretKeyFactory_ == null) {
                    secretKeyFactory_ =
                        SecretKeyFactory.getInstance("DES", providerName);
                }
                key = secretKeyFactory_.generateSecret(desKey);
            }

            //We use DES in CBC mode because this is the mode used in PROTOCOL. The
            //encryption mode has to be consistent for encryption and decryption.
            //CBC mode requires an initialization vector(IV) parameter. In CBC mode
            //we need to initialize the Cipher object with an IV, which can be supplied
            // using the javax.crypto.spec.IvParameterSpec class.
            Cipher cipher =
                Cipher.getInstance("DES/CBC/PKCS5Padding", providerName);

            //generate a IVParameterSpec object and use it to initiate the
            //Cipher object.
            IvParameterSpec ivParam = new IvParameterSpec(token_);

            //initiate the Cipher using encryption mode, encryption key and the
            //IV parameter.
            cipher.init(Cipher.ENCRYPT_MODE, key, ivParam);

            //Execute the final phase of encryption
            cipherText = cipher.doFinal(plainText);
        } catch (NoSuchPaddingException e) {
            throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.CRYPTO_NO_SUCH_PADDING)); 
        } catch (BadPaddingException e) {
            throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.CRYPTO_BAD_PADDING)); 
        } catch (IllegalBlockSizeException e) {
            throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.CRYPTO_ILLEGAL_BLOCK_SIZE)); 
        } catch (GeneralSecurityException e) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.SECURITY_EXCEPTION_ENCOUNTERED), e); 
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
        Key key = null;

        if (token_ == null) {
            token_ = calculateEncryptionToken(securityMechanism, initVector);
        }

        try {
            if (secKey_ == null) {
                //use this encryption key to initiate a SecretKeySpec object
                secKey_ = generatePrivateKey(targetPublicKey);
                SecretKeySpec desKey = new SecretKeySpec(secKey_, "DES");
                key = desKey;
            } else {
                //use this encryption key to initiate a SecretKeySpec object
                DESKeySpec desKey = new DESKeySpec(secKey_);
                if (secretKeyFactory_ == null) {
                    secretKeyFactory_ =
                        SecretKeyFactory.getInstance("DES", providerName);
                }
                key = secretKeyFactory_.generateSecret(desKey);
            }

            //We use DES in CBC mode because this is the mode used in PROTOCOL. The
            //encryption mode has to be consistent for encryption and decryption.
            //CBC mode requires an initialization vector(IV) parameter. In CBC mode
            //we need to initialize the Cipher object with an IV, which can be supplied
            // using the javax.crypto.spec.IvParameterSpec class.
            Cipher cipher =
                Cipher.getInstance("DES/CBC/PKCS5Padding", providerName);

            //generate a IVParameterSpec object and use it to initiate the
            //Cipher object.
            IvParameterSpec ivParam = new IvParameterSpec(token_);

            //initiate the Cipher using encryption mode, encryption key and the
            //IV parameter.
            cipher.init(Cipher.DECRYPT_MODE, key, ivParam);

            //Execute the final phase of encryption
            plainText = cipher.doFinal(cipherText);
        } catch (NoSuchPaddingException e) {
            throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.CRYPTO_NO_SUCH_PADDING)); 
        } catch (BadPaddingException e) {
            throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.CRYPTO_BAD_PADDING)); 
        } catch (IllegalBlockSizeException e) {
            throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.CRYPTO_ILLEGAL_BLOCK_SIZE)); 
        } catch (GeneralSecurityException e) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.SECURITY_EXCEPTION_ENCOUNTERED), e); 
        }
        return plainText;
    }

    public void resetSecurityKeys() {
        token_ = null;
        secKey_ = null;
    }

    /****************************************************************
     * Below are methods for the SECMEC_USRSSBPWD security mechanism.
     ****************************************************************/

    /**
     * This method generates an 8-Byte random seed for the client (source).
     *
     * @return a random 8-Byte seed.
     */
    public byte[] generateSeed() {
        byte randomSeedBytes[] = new byte[SECMEC_USRSSBPWD_SEED_LEN];
        secureRandom.setSeed(secureRandom.generateSeed(
                                        SECMEC_USRSSBPWD_SEED_LEN));
        secureRandom.nextBytes(randomSeedBytes);
        return randomSeedBytes;
    }

    /**
     * Strong Password Substitution (USRSSBPWD).
     *
     * This method generates a password substitute to send to the target
     * server.
     * 
     * Substitution algorithm works as follow:
     *
     * PW_TOKEN = SHA-1(PW, ID)
     * The password (PW) and user name (ID) can be of any length greater
     * than or equal to 1 byte.
     * The client generates a 20-byte password substitute (PW_SUB) as follows:
     * PW_SUB = SHA-1(PW_TOKEN, RDr, RDs, ID, PWSEQs)
     * 
     * w/ (RDs) as the random client seed and (RDr) as the server one.
     * 
     * See PWDSSB - Strong Password Substitution Security Mechanism
     * (DRDA Vol.3 - P.650)
     *
     * @param userName The user's name
     * @param password The user's password
     * @param sourceSeed_ random client seed (RDs)
     * @param targetSeed_ random server seed (RDr)
     *
     * @return a password substitute.
     */
    public byte[] substitutePassword(
                String userName,
                String password,
                byte[] sourceSeed_,
                byte[] targetSeed_) throws SqlException {

        // Pattern that is prefixed to the BUILTIN encrypted password
        String ID_PATTERN_NEW_SCHEME = "3b60";
        
        // Generated password substitute
        byte[] passwordSubstitute;

        // Assert we have a SHA-1 Message Digest already instantiated
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT((messageDigest != null) &&
                                 (SHA_1_DIGEST_ALGORITHM.equals(
                                    messageDigest.getAlgorithm())));
        }

        // IMPORTANT NOTE: As the password is stored single-hashed in the
        // database on the target side, it is impossible for the target to
        // decrypt the password and recompute a substitute to compare with
        // one generated on the source side - Hence, for now we have to
        // single-hash and encrypt the password the same way the target is
        // doing it and we will still generate a substitute obviously - The
        // password, even pre-hashed will never make it across the wire as
        // a substitute is generated. In other words, if the target cannot
        // figure what the original password is (because of not being able
        // to decrypt it or not being able to retrieve it (i.e. LDAP), then
        // It may be problematic - so in a way, Strong Password Substitution
        // (USRSSBPWD) cannot be supported for targets which can't access or
        // decrypt some password on their side.
        //
        // So in short, SECMEC_USRSSBPWD is only supported if the
        // authentication provider on the target side is NONE or Derby's
        // BUILTIN one and if using Derby's Client Network driver (for now).
        //
        // Encrypt the password as it is done by the derby engine - Note that
        // this code (logic) is not shared yet - will be in next revision.
        //
        // Note that this code assumes that the Derby engine has encrypted
        // the password using one particular algorithm (based on SHA-1). After
        // DERBY-4483, it is possible that the engine uses another algorithm.
        // Since the engine has no way to decrypt the encrypted password, it
        // has no way to compared the stored password with the hash we send, so
        // authentication will fail unless the engine actually uses the SHA-1
        // based scheme.

        messageDigest.reset();

        messageDigest.update(this.toHexByte(password, 0, password.length()));
        byte[] encryptVal = messageDigest.digest();
        String hexString = ID_PATTERN_NEW_SCHEME +
                     this.toHexString(encryptVal, 0, encryptVal.length);

        // Generate some 20-byte password token
        byte[] userBytes = this.toHexByte(userName, 0, userName.length());
        messageDigest.update(userBytes);
        messageDigest.update(this.toHexByte(hexString, 0, hexString.length()));
        byte[] passwordToken = messageDigest.digest();
        
        // Now we generate the 20-byte password substitute
        messageDigest.update(passwordToken);
        messageDigest.update(targetSeed_);
        messageDigest.update(sourceSeed_);
        messageDigest.update(userBytes);
        messageDigest.update(SECMEC_USRSSBPWD_PWDSEQS);

        passwordSubstitute = messageDigest.digest();

        return passwordSubstitute;
    }

    /*********************************************************************
     * RESOLVE:                                                          *
     * The methods and static vars below should go into some 'shared'    *
     * package when the capability is put back in (StringUtil.java).     *
     *********************************************************************/

    private static final char[] hex_table = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
                'a', 'b', 'c', 'd', 'e', 'f'
            };

    /**
        Convert a byte array to a String with a hexadecimal format.
        The String may be converted back to a byte array using fromHexString.
        <BR>
        For each byte (b) two characters are generated, the first character
        represents the high nibble (4 bits) in hexadecimal ({@code b & 0xf0}),
        the second character represents the low nibble ({@code b & 0x0f}).
        <BR>
        The byte at {@code data[offset]} is represented by the first two
        characters in the returned String.

        @param  data    byte array
        @param  offset  starting byte (zero based) to convert.
        @param  length  number of bytes to convert.

        @return the String (with hexidecimal format) form of the byte array
    */
    private String toHexString(byte[] data, int offset, int length)
    {
        StringBuilder s = new StringBuilder(length*2);
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
        represents the high nibble (4 bits) in hexadecimal ({@code b & 0xf0}),
        the second byte represents the low nibble ({@code b & 0x0f}).
        <BR>
        The character at {@code str.charAt(0)} is represented by the
        first two bytes in the returned String.

        @param  str string
        @param  offset  starting character (zero based) to convert.
        @param  length  number of characters to convert.

        @return the byte[]  (with hexadecimal format) form of the string (str)
    */
    private byte[] toHexByte(String str, int offset, int length)
    {
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
