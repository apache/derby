/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.SkipTest

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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
		
			
