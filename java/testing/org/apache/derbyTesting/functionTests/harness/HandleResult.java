/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.IOException;

/**
  Class: HandleResult
  Purpose: To capture stdout & stderr to a file
  (PrintWriter is used for writing the output)
*/

public class HandleResult
{ 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	public static void main(String[] args) throws Exception
	{
	}

    public static String handleResult(int exitCode, InputStream stdout,
        InputStream stderr, PrintWriter printWriter)
        throws IOException
    {
		StringBuffer sb = new StringBuffer();

		// only used for debugging
		sb.append("exitcode=");
		sb.append(exitCode);

        if (stdout != null)
        {
    		// reader for stdout
    		BufferedReader outReader = new BufferedReader(new InputStreamReader(stdout));

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
    		BufferedReader errReader = new BufferedReader(new InputStreamReader(stderr));

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


