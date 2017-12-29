/*

   Derby - Class org.apache.derbyTesting.unitTests.crypto.T_Cipher

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

package org.apache.derbyTesting.unitTests.crypto;

import org.apache.derbyTesting.unitTests.harness.T_Generic;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.services.crypto.*;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.shared.common.error.StandardException;

import java.security.AccessController;
import java.security.Key;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedAction;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;

import java.util.Properties;


/*
// PT
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.spec.KeySpec;
import java.security.AlgorithmParameters;
// import java.security.spec.AlgorithmParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.lang.reflect.*;
*/


/*
	To run, put the following line in derby.properties
	derby.module.test.T_Cipher=org.apache.derbyTesting.unitTests.crypto.T_Cipher

	and run java org.apache.derbyTesting.unitTests.harness.UnitTestMain

*/
public class T_Cipher extends T_Generic
{
	private static final String testService = "CipherText";

	CipherProvider enEngine;
	CipherProvider deEngine;
	Key secretKey;
	byte[] IV;

	CipherFactory factory;
    
	public T_Cipher()
	{
		super();
	}

	/*
	** Methods required by T_Generic
	*/

	public String getModuleToTestProtocolName() {
		return org.apache.derby.shared.common.reference.Module.CipherFactoryBuilder;
	}

    protected String getAlgorithm()
    {
        return "DES/CBC/NoPadding";
    }

    protected String getProvider()
    {
	// allow for alternate providers
	String testProvider = 
		
        AccessController.doPrivileged(new PrivilegedAction<String>() {
            public String run() {
		    	return System.getProperty("testEncryptionProvider");
		    }
	    });
	
	if (testProvider != null) 
		return testProvider;
	else
		return null;	

    }

	public void runTests() throws T_Fail {

		File testFile = new File("extinout/T_Cipher.data");
		deleteFile(testFile);

		String bootPassword = "a secret, don't tell anyone";

		try
		{
			RandomAccessFile file = new RandomAccessFile(testFile, "rw");

			setupCiphers(bootPassword);

			// run thru some in patterns
			int patternLength = 8192;
			byte[] pattern = new byte[patternLength];
			for (int i = 0; i < patternLength; i++)
				pattern[i] = (byte)(i & 0xFF);

			test(pattern, 0, 8, file);	// test short patterns
			test(pattern, 8, 8, file);
			test(pattern, 1, 16, file);

			test(pattern, 0, patternLength, file); // test long pattern
			test(pattern, 0, patternLength/2, file);
			test(pattern, 1, patternLength/2, file);
			test(pattern, 2, patternLength/2, file);
			test(pattern, 3, patternLength/2, file);

			file.seek(0);
			check(pattern, 0, 8, file);	// file offset 0
			check(pattern, 8, 8, file);	// file offset 8
			check(pattern, 1, 16, file);	// file offset 16
			check(pattern, 0, patternLength, file);	// file offset 32
			check(pattern, 0, patternLength/2, file);// file offset 32+patternLength
			check(pattern, 1, patternLength/2, file);// file offset 32+patternLength+(patternLength/2)
			check(pattern, 2, patternLength/2, file);// file offset 32+(2*patternLength)
			check(pattern, 3, patternLength/2, file);// file offset 32+(2*patternLength)+(patternLength/2);

			REPORT("starting random test");

			// now do some random testing from file
			file.seek(32+patternLength);
			check(pattern, 0, patternLength/2, file);

			file.seek(32);
			check(pattern, 0, patternLength, file);

			file.seek(32+(2*patternLength));
			check(pattern, 2, patternLength/2, file);

			file.seek(0);
			check(pattern, 0, 8, file);

			file.seek(16);
			check(pattern, 1, 16, file);

			file.seek(32+(2*patternLength)+(patternLength/2));
			check(pattern, 3, patternLength/2, file);

			file.seek(8);
			check(pattern, 8, 8, file);

			file.seek(32+patternLength+(patternLength/2));
			check(pattern, 1, patternLength/2, file);

			file.close();
		}
		catch (StandardException se)
		{
			se.printStackTrace(System.out);
			throw T_Fail.exceptionFail(se);
		}
		catch (IOException ioe)
		{
			throw T_Fail.exceptionFail(ioe);
		}


		PASS("T_Cipher");
	}


	protected void setupCiphers(String bootPassword) throws T_Fail, StandardException
	{
        // set properties for testing
        Properties props = new Properties();
        props.put("encryptionAlgorithm",getAlgorithm());
        String provider = getProvider();
        if (provider != null)
            props.put("encryptionProvider",getProvider());
		props.put("bootPassword", bootPassword);

        REPORT("encryption algorithm used : " + getAlgorithm());
        REPORT("encryption provider used : " + provider);

        CipherFactoryBuilder cb =  (CipherFactoryBuilder)
            startSystemModule(org.apache.derby.shared.common.reference.Module.CipherFactoryBuilder);

        factory = cb.createCipherFactory(true, props, false);

		if (factory == null)
			throw T_Fail.testFailMsg("cannot find Cipher factory ");

		enEngine = factory.createNewCipher(CipherFactory.ENCRYPT);
		deEngine = factory.createNewCipher(CipherFactory.DECRYPT);

		if (enEngine == null)
			throw T_Fail.testFailMsg("cannot create encryption engine");
		if (deEngine == null)
			throw T_Fail.testFailMsg("cannot create decryption engine");
	}

	protected void test(byte[] cleartext, int offset, int length,
					  RandomAccessFile outfile)
		 throws T_Fail, StandardException, IOException
	{
		byte[] ciphertext = new byte[length];
		System.arraycopy(cleartext, offset, ciphertext, 0, length);

		if (enEngine.encrypt(ciphertext, 0, length, ciphertext, 0) != length)
			throw T_Fail.testFailMsg("encrypted text length != length");

		if (byteArrayIdentical(ciphertext, cleartext, offset, length))
			throw T_Fail.testFailMsg("encryption just made a copy of the clear text");

		outfile.write(ciphertext);

		// now decrypt it and check
		deEngine.decrypt(ciphertext, 0, length, ciphertext, 0);
		if (byteArrayIdentical(ciphertext, cleartext, offset, length) == false)
			throw T_Fail.testFailMsg("decryption did not yield the same clear text");
	}

	protected void check(byte[] cleartext, int offset, int length,
					   RandomAccessFile infile)
		 throws IOException, T_Fail, StandardException
	{
		byte[] ciphertext = new byte[length];
		infile.read(ciphertext);

		if (deEngine.decrypt(ciphertext, 0, length, ciphertext, 0) != length)
			throw T_Fail.testFailMsg("decrypted text length != length");

		if (byteArrayIdentical(ciphertext, cleartext, offset, length) == false)
			throw T_Fail.testFailMsg("decryption did not yield the same clear text");

	}

	// see if 2 byte arrays are identical
	protected boolean byteArrayIdentical(byte[] compare, byte[] original,
									  int offset, int length)
	{
		for (int i = 0; i < length; i++)
		{
			if (compare[i] != original[offset+i])
				return false;
		}
		return true;
	}


    /*
    private void testBlowfish()
    {
        System.out.println("Running testBlowfish");
        try
        {
            // set up the provider
            java.security.Provider sunJce = new com.sun.crypto.provider.SunJCE();
            java.security.Security.addProvider(sunJce);

            // String key = "Paula bla la da trish123 sdkfs;ldkg;sa'jlskjgklad";
            String key = "Paulabla123456789012345";
            byte[] buf = key.getBytes();
            System.out.println("key length is " + buf.length);
            SecretKeySpec sKeySpec = new SecretKeySpec(buf,"Blowfish");
            // SecretKeySpec sKeySpec = new SecretKeySpec(buf,"DESede");

            Cipher cipher = Cipher.getInstance("Blowfish/CBC/NoPadding");
            // Cipher cipher = Cipher.getInstance("DESede/CBC/NoPadding");
            // Cipher cipher = Cipher.getInstance("Blowfish/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE,sKeySpec);
            // only works with NoPadding if size is a multiple of 8 bytes
            // with PKCS5Padding, works for all sizes
            byte[] original = "This is what should get encrypte".getBytes();
            System.out.println("original length is " + original.length);
            byte[] encrypted = cipher.doFinal(original);
            // works
            // AlgorithmParameters algParam = cipher.getParameters();
            byte[] iv = cipher.getIV();
            System.out.println("length of iv is " + iv.length);

            Cipher cipher2 = Cipher.getInstance("Blowfish/CBC/NoPadding");
            // Cipher cipher2 = Cipher.getInstance("DESede/CBC/NoPadding");
            // Cipher cipher2 = Cipher.getInstance("Blowfish/CBC/PKCS5Padding");

            // works
            // cipher2.init(Cipher.DECRYPT_MODE,sKeySpec,algParam);
            IvParameterSpec ivClass = new IvParameterSpec(iv);
            cipher2.init(Cipher.DECRYPT_MODE,sKeySpec,ivClass);
            byte[] decrypted = cipher2.doFinal(encrypted);

            if (byteArrayIdentical(original,decrypted,0,original.length))
                System.out.println("PASSED");
            else
                System.out.println("FAILED");

            System.out.println("original length is " + original.length);
            System.out.println("encrypted length is " + encrypted.length);
            System.out.println("decrypted length is " + decrypted.length);
        }
        catch (Throwable t)
        {
            System.out.println("got an exception");
            t.printStackTrace();
        }

        System.out.println("Finished testBlowfish");
    }


    private void testCryptix()
    {
        System.out.println("Running testCryptix");
        try
        {
            // set up the provider
            Class jceClass = Class.forName("cryptix.jce.provider.Cryptix");
            java.security.Provider cryptixProvider = (java.security.Provider) jceClass.newInstance();
            java.security.Security.addProvider(cryptixProvider);

		    byte[] userkey = "a secret".getBytes();
            System.out.println("userkey length is " + userkey.length);
            Key secretKey = (Key) (new SecretKeySpec(userkey, "DES"));
		    byte[] IV = "anivspec".getBytes();

            Cipher enCipher = Cipher.getInstance("DES/CBC/NoPadding","Cryptix");
            Cipher deCipher = Cipher.getInstance("DES/CBC/NoPadding","Cryptix");
			IvParameterSpec ivspec = new IvParameterSpec(IV);

            enCipher.init(Cipher.ENCRYPT_MODE,secretKey,ivspec);
            deCipher.init(Cipher.DECRYPT_MODE,secretKey,ivspec);

            int patternLength = 8;
            byte[] pattern = new byte[patternLength];
			for (int i = 0; i < patternLength; i++)
				pattern[i] = (byte)(i & 0xFF);

            byte[] cipherOutput1 = new byte[patternLength];
            byte[] cipherOutput2 = new byte[patternLength];

            int retval = 0;
            retval = enCipher.doFinal(pattern, 0, 8, cipherOutput1, 0);

            retval = deCipher.doFinal(cipherOutput1, 0, 8, cipherOutput2, 0);

            if (byteArrayIdentical(cipherOutput2,pattern,0,patternLength))
                System.out.println("PASSED TEST 1");
            else
                System.out.println("FAILED TEST 1");

            retval = deCipher.doFinal(cipherOutput1, 0, 8, cipherOutput2, 0);

            if (byteArrayIdentical(cipherOutput2,pattern,0,patternLength))
                System.out.println("PASSED TEST 2");
            else
                System.out.println("FAILED TEST 2");
        }
        catch (Throwable t)
        {
            System.out.println("got an exception");
            t.printStackTrace();
        }

        System.out.println("Finished testCryptix");
    }



    private void testMessageDigest()
    {
        // No provider needs to be installed for this to work.
        try
        {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] data = "Paulas digest".getBytes();
            byte[] digest = md.digest(data);
            byte[] digest2 = md.digest(data);
            if (byteArrayIdentical(digest,digest2,0,digest.length))
                System.out.println("PASSED");
            else
                System.out.println("FAILED");

            System.out.println("data length is " + data.length);
            System.out.println("digest length is " + digest.length);
            System.out.println("digest2 length is " + digest2.length);
        }
        catch (Throwable t)
        {
            System.out.println("got an exception");
            t.printStackTrace();
        }

        System.out.println("Finished testBlowfish");
    }

    // PT
    private void testPCBC()
    {
        System.out.println("Running testPCBC");
        try
        {
            // set up the provider
            Class jceClass = Class.forName("com.sun.crypto.provider.SunJCE");
            java.security.Provider myProvider = (java.security.Provider) jceClass.newInstance();
            java.security.Security.addProvider(myProvider);
            // java.security.Provider sunJce = new com.sun.crypto.provider.SunJCE();
            // java.security.Security.addProvider(sunJce);

            // String key = "Paula bla la da trish123 sdkfs;ldkg;sa'jlskjgklad";
            String key = "PaulablaPaulablaPaulabla";
            byte[] buf = key.getBytes();
            System.out.println("key length is " + buf.length);
            SecretKeySpec sKeySpec = new SecretKeySpec(buf,"DESede");

            Cipher cipher = Cipher.getInstance("DESede/PCBC/NoPadding");
            // Cipher cipher = Cipher.getInstance("DESede/CBC/NoPadding");
            // Cipher cipher = Cipher.getInstance("Blowfish/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE,sKeySpec);
            // only works with NoPadding if size is a multiple of 8 bytes
            // with PKCS5Padding, works for all sizes
            byte[] original = "This is what should get encrypte".getBytes();
            System.out.println("original length is " + original.length);
            byte[] encrypted = cipher.doFinal(original);
            // works
            // AlgorithmParameters algParam = cipher.getParameters();
            byte[] iv = cipher.getIV();
            System.out.println("length of iv is " + iv.length);

            Cipher cipher2 = Cipher.getInstance("DESede/PCBC/NoPadding");
            // Cipher cipher2 = Cipher.getInstance("DESede/CBC/NoPadding");
            // Cipher cipher2 = Cipher.getInstance("Blowfish/CBC/PKCS5Padding");

            // works
            // cipher2.init(Cipher.DECRYPT_MODE,sKeySpec,algParam);
            IvParameterSpec ivClass = new IvParameterSpec(iv);
            cipher2.init(Cipher.DECRYPT_MODE,sKeySpec,ivClass);
            byte[] decrypted = cipher2.doFinal(encrypted);

            if (byteArrayIdentical(original,decrypted,0,original.length))
                System.out.println("PASSED");
            else
                System.out.println("FAILED");

            System.out.println("original length is " + original.length);
            System.out.println("encrypted length is " + encrypted.length);
            System.out.println("decrypted length is " + decrypted.length);
        }
        catch (Throwable t)
        {
            System.out.println("got an exception");
            t.printStackTrace();
        }

        System.out.println("Finished testPCBC");
    }


    private void testPCBC2()
    {
        System.out.println("Running testPCBC2");
        try
        {
            // set up the provider
            Class jceClass = Class.forName("com.sun.crypto.provider.SunJCE");
            java.security.Provider myProvider = (java.security.Provider) jceClass.newInstance();
            java.security.Security.addProvider(myProvider);

		    byte[] userkey = "a secreta secreta secret".getBytes();
            System.out.println("userkey length is " + userkey.length);
            Key secretKey = (Key) (new SecretKeySpec(userkey, "DESede"));
		    byte[] IV = "anivspec".getBytes();

            Cipher enCipher = Cipher.getInstance("DESede/PCBC/NoPadding","SunJCE");
            Cipher deCipher = Cipher.getInstance("DESede/PCBC/NoPadding","SunJCE");
			IvParameterSpec ivspec = new IvParameterSpec(IV);

            enCipher.init(Cipher.ENCRYPT_MODE,secretKey,ivspec);
            deCipher.init(Cipher.DECRYPT_MODE,secretKey,ivspec);

            int patternLength = 24;
            byte[] pattern = new byte[patternLength];
			for (int i = 0; i < patternLength; i++)
				pattern[i] = (byte)(i & 0xFF);

            byte[] cipherOutput1 = new byte[patternLength];
            byte[] cipherOutput2 = new byte[patternLength];

            int retval = 0;
            retval = enCipher.doFinal(pattern, 0, 24, cipherOutput1, 0);

            retval = deCipher.doFinal(cipherOutput1, 0, 24, cipherOutput2, 0);

            if (byteArrayIdentical(cipherOutput2,pattern,0,patternLength))
                System.out.println("PASSED TEST 1");
            else
                System.out.println("FAILED TEST 1");

            retval = deCipher.doFinal(cipherOutput1, 0, 24, cipherOutput2, 0);

            if (byteArrayIdentical(cipherOutput2,pattern,0,patternLength))
                System.out.println("PASSED TEST 2");
            else
                System.out.println("FAILED TEST 2");
        }
        catch (Throwable t)
        {
            System.out.println("got an exception");
            t.printStackTrace();
        }

        System.out.println("Finished testPCBC2");
    }

    private void testIAIK()
    {
        System.out.println("Running testIAIK");
        try
        {
            // set up the provider
            Class jceClass = Class.forName("iaik.security.provider.IAIK");
            java.security.Provider myProvider = (java.security.Provider) jceClass.newInstance();
            java.security.Security.addProvider(myProvider);

            // iaik.security.provider.IAIK.addAsProvider(true);

            // iaik.utils.Util.loadClass("iaik.security.provider.IAIK",true);
            // IAIK p=new IAIK();
            // iaik.security.provider.IAIK.getMd5();

		    byte[] userkey = "a secret".getBytes();
            System.out.println("userkey length is " + userkey.length);
            Key secretKey = (Key) (new SecretKeySpec(userkey, "DES"));
		    byte[] IV = "anivspec".getBytes();

            Cipher enCipher = Cipher.getInstance("DES/CBC/NoPadding","IAIK");
            Cipher deCipher = Cipher.getInstance("DES/CBC/NoPadding","IAIK");
			IvParameterSpec ivspec = new IvParameterSpec(IV);

            enCipher.init(Cipher.ENCRYPT_MODE,secretKey,ivspec);
            deCipher.init(Cipher.DECRYPT_MODE,secretKey,ivspec);

            int patternLength = 8;
            byte[] pattern = new byte[patternLength];
			for (int i = 0; i < patternLength; i++)
				pattern[i] = (byte)(i & 0xFF);

            byte[] cipherOutput1 = new byte[patternLength];
            byte[] cipherOutput2 = new byte[patternLength];

            int retval = 0;
            retval = enCipher.doFinal(pattern, 0, 8, cipherOutput1, 0);

            retval = deCipher.doFinal(cipherOutput1, 0, 8, cipherOutput2, 0);

            if (byteArrayIdentical(cipherOutput2,pattern,0,patternLength))
                System.out.println("PASSED TEST 1");
            else
                System.out.println("FAILED TEST 1");

            retval = deCipher.doFinal(cipherOutput1, 0, 8, cipherOutput2, 0);

            if (byteArrayIdentical(cipherOutput2,pattern,0,patternLength))
                System.out.println("PASSED TEST 2");
            else
                System.out.println("FAILED TEST 2");
        }
        catch (Throwable t)
        {
            System.out.println("got an exception");
            t.printStackTrace();
        }

        System.out.println("Finished testIAIK");
    }

    private void printByteArray(String name, byte[] array)
    {
        System.out.println("printing array " + name);
        for (int i = 0; i < array.length; i++)
            System.out.println("index " + i + " : " + array[i]);
    }
    */
	
	/**
	 * Delete a file in a Privileged block as these tests are
	 * run under the embedded engine code.
	 */
	private void deleteFile(final File f)
	{
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run()  {
		    	if (f.exists())
		    	    f.delete();
		    	return null;
		    }
	    });
	}
    
    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object  startSystemModule( final String factoryInterface )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.startSystemModule( factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

}
