/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.util.Properties;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;


/**
  Determine if the named test is one which should not be
  run in a particular framework (defined by the propFileName).
  For instance, there could be a nowl.properties for a list of
  tests which do not currently work under the WebLogic framework.
  */
public class SkipTest
{ 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
 
    private SkipTest()
    {
    }

    public static boolean skipIt(String listFileName, String testName)
        throws Exception
    {
        boolean answer = false;
	    InputStream is =
		RunTest.loadTestResource("suites" + '/' + listFileName);
        if (is == null)
        {
            System.out.println("File not found: " + listFileName);
            answer = false;
            return answer;
        }
        
        // Create a BufferedReader to read the list of tests to skip
        BufferedReader listFile = new BufferedReader(new InputStreamReader(is));
        String str = "";
        // Read the list of tests to skip, compare to testName
        while ( (str = listFile.readLine()) != null )
        {
	       if ( (testName.equals(str)) )
	            answer = true;
	    }
        return answer;
    }
}
		
			
