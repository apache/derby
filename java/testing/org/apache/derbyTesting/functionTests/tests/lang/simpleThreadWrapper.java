/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.tests.lang
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.tests.lang;

/**
 * Test of strings longer than 64K.
   This is the wrapper class used by the test harness.
 */

public class simpleThreadWrapper
{
 
    public static void main(String[] args) 
    {
        try
        {
            System.out.println("Starting simpleThread");
            simpleThread st = new org.apache.derbyTesting.functionTests.tests.lang.simpleThread(args);
            System.out.println("End of simpleThread");
        }
        catch(Exception e)
        {
            System.out.println("simpleThreadWraper: " + e);
        }
    }
}
