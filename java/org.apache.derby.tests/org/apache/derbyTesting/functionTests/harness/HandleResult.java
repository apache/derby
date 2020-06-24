/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.HandleResult

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

package org.apache.derbyTesting.functionTests.harness;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.IOException;

/**
  Class: HandleResult
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
  Purpose: To capture stdout and stderr to a file
  (PrintWriter is used for writing the output)
*/

public class HandleResult
{

	public static void main(String[] args) throws Exception
	{
	}

	public static String handleResult(int exitCode, InputStream stdout,
//IC see: https://issues.apache.org/jira/browse/DERBY-683
	        InputStream stderr, PrintWriter printWriter)
	        throws IOException
	{
		return handleResult(exitCode, stdout, stderr, printWriter, null);
	}
	
    public static String handleResult(int exitCode, InputStream stdout,
        InputStream stderr, PrintWriter printWriter, String encoding)
        throws IOException
    {
		StringBuffer sb = new StringBuffer();

		// only used for debugging
		sb.append("exitcode=");
		sb.append(exitCode);

        if (stdout != null)
        {
    		// reader for stdout
//IC see: https://issues.apache.org/jira/browse/DERBY-683
        	BufferedReader outReader;
        	if(encoding != null)
        		outReader = new BufferedReader(new InputStreamReader(stdout, encoding));
        	else
        		outReader = new BufferedReader(new InputStreamReader(stdout));

            // Read each line and write to printWriter
    		String s = null;
    		int lines = 0;
    		while ((s = outReader.readLine()) != null)
    		{
    		    lines++;
    		    if (printWriter == null)
    			    System.out.println(s);
    			else
    			    printWriter.println(s);
    		}
    		sb.append(",");
    		sb.append(lines);
    		outReader.close();
    		printWriter.flush();
        }

        if (stderr != null)
        {
            // reader for stderr
//IC see: https://issues.apache.org/jira/browse/DERBY-683
        	BufferedReader errReader;
        	if(encoding != null)
        		errReader = new BufferedReader(new InputStreamReader(stderr, encoding));
        	else
        		errReader = new BufferedReader(new InputStreamReader(stderr));

    		String s = null;
    		int lines = 0;
    		while ((s = errReader.readLine()) != null)
    		{
    		    if (printWriter == null)
    			    System.out.println(s);
    			else
    			    printWriter.println(s);
    		}
    		errReader.close();
    		printWriter.flush();
    	}

		return sb.toString();
	}
}


