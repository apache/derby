/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_MarkedLimitInputStream

   Copyright 2001, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.unitTests.services;

import org.apache.derbyTesting.unitTests.harness.T_Generic;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
  A simple unit test for a MarkedLimitInputStream.
  */
public class T_MarkedLimitInputStream extends T_Generic
{

    private static final int TEST_SIZE = 10000;
    private static final int BLOCK_SIZE = 256;


    private static MarkedLimitInputStream setup(byte[] data)
        throws Exception
    {
        // make an InputStream on top of an array
        InputStream inputStream = new ByteArrayInputStream(data);

        // make an OutputStream on top of an empty array
        ByteArrayOutputStream baos = new ByteArrayOutputStream(TEST_SIZE + 200);
        // make it into a DataOutputStream
        DataOutputStream dos = new DataOutputStream(baos);
        // fill it with data in the correct (block) format
        writeDos(inputStream,dos);

        // make a MarkedLimitInputStream
        return makeMLIS(baos.toByteArray());

	}

	private static void writeDos(InputStream x, DataOutputStream out)
        throws Exception
	{
        boolean isLastBlock = false;
        byte[] b = new byte[BLOCK_SIZE];

        while (isLastBlock == false)
        {
            int len = x.read(b);
            if (len != BLOCK_SIZE)
            {
                isLastBlock = true;
                if (len < 0)
                {
                    len = 0;
                }
            }
            out.writeBoolean(isLastBlock);
            out.writeInt(len);
            for (int i = 0; i < len; i++)
            {
                out.writeByte(b[i]);
            }
        }
    }


    private static MarkedLimitInputStream makeMLIS(byte[] b)
        throws Exception
    {
        // make an InputStream
        InputStream inputStream = new ByteArrayInputStream(b);
        // make a DataInputStream
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        // make a MarkedLimitInputStream
        return new MarkedLimitInputStream(dataInputStream);
    }


    private static boolean readAndCompare(MarkedLimitInputStream mlis, byte[] x)
        throws Exception
    {
        int b;
        int i = 0;
        while ((b = mlis.read()) != -1)
        {
            if (x[i] != (byte) b)
            {
                System.out.println("Stream and array differ at position " + i);
                return false;
            }
            i++;
        }
        // read to end of stream, check array size
        if (i != x.length)
        {
            System.out.println("array size and stream size differ");
            return false;
        }
        return true;

    }


    private static boolean readAndCompareChunks(MarkedLimitInputStream mlis,
        byte[] x)
        throws Exception
    {
        int chunkSize = 10;
        byte[] chunk = new byte[chunkSize];
        int c = 0;
        int base = 0;
        while ((c = mlis.read(chunk)) > 0)
        {
            for (int offset = 0; offset < c; offset++)
            {
                if (x[base + offset] != chunk[offset])
                {
                    System.out.println("Stream and array differ at position " +
                        (base + offset));
                    System.out.println("Array : x[" + (base + offset) + "] = " + x[base+offset]);
                    System.out.println("Stream : chunk[" + offset + "] = " + chunk[offset]);
                    return false;
                }
            }
            base += c;
        }

        // read to end of stream, check array size
        if (base != x.length)
        {
            System.out.println("array size ( " + x.length +
                " ) and stream size ( " + base + " ) differ");
            return false;
        }
        return true;

    }


    private static boolean skipAndCompare(MarkedLimitInputStream mlis, byte[] x,
        long skipTo)
        throws Exception
    {
        long c = mlis.skip(skipTo);
        T_Fail.T_ASSERT(c == skipTo);
        byte[] y = new byte[x.length - (int) c];
        System.arraycopy(x,(int) skipTo, y, 0, x.length - (int) c);
        return readAndCompare(mlis,y);
    }


	/** Methods required by T_Generic
	*/
	public String getModuleToTestProtocolName()
	{
		return "internalUtils.MarkedLimitInputStream";
	}


	protected void runTests()
        throws Exception
    {
        boolean success = true;
        // create and initialize array
        byte[] data = new byte[TEST_SIZE];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = (byte)(i & 0xFF);
        }

        MarkedLimitInputStream mlis = setup(data);
        // compare MarkedLimitInputStream with original byte array
        if (readAndCompare(mlis, data))
        {
            PASS("test1");
        }
        else
        {
            FAIL("test1");
            success = false;
        }

        MarkedLimitInputStream mlis2 = setup(data);
        // compare MarkedLimitInputStream with original byte array
        // read in chunks
        if (readAndCompareChunks(mlis2, data))
        {
            PASS("test2");
        }
        else
        {
            FAIL("test2");
            success = false;
        }

        MarkedLimitInputStream mlis3 = setup(data);
        // skip and compare MarkedLimitInputStream with original byte array
        if (skipAndCompare(mlis3, data, TEST_SIZE/2))
        {
            PASS("test3");
        }
        else
        {
            FAIL("test3");
            success = false;
        }

        MarkedLimitInputStream mlis4 = setup(data);
        // skip and compare MarkedLimitInputStream with original byte array
        if (skipAndCompare(mlis4, data, TEST_SIZE-1))
        {
            PASS("test4");
        }
        else
        {
            FAIL("test4");
            success = false;
        }

        if (!success)
        {
            throw T_Fail.testFail();
        }


        // create and initialize array with size BLOCK_SIZE
        byte[] data2 = new byte[BLOCK_SIZE];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = (byte)(i & 0xFF);
        }
        MarkedLimitInputStream mlis5 = setup(data2);
        // skip and compare MarkedLimitInputStream with original byte array
        if (readAndCompare(mlis5, data2))
        {
            PASS("test5");
        }
        else
        {
            FAIL("test5");
            success = false;
        }

        if (!success)
        {
            throw T_Fail.testFail();
        }

    }

}
