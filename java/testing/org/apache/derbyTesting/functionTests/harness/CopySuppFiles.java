/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.CopySuppFiles

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

import java.io.*;
import java.util.StringTokenizer;

/**
  For tests which require support files.
  Copy them to the output directory for the test.
  */
public class CopySuppFiles
{

	public static void main(String[] args) throws Exception
	{
	}

	public static void copyFiles(File outDir, String suppFiles)
	    throws ClassNotFoundException, IOException
	{
	    // suppFiles is a comma separated list of the files
	    StringTokenizer st = new StringTokenizer(suppFiles,",");
	    String scriptName = ""; // example: test/math.sql
	    InputStream is = null; // To be used for each support file
        while (st.hasMoreTokens())
        {
            scriptName = st.nextToken();
    	    File suppFile = null;
    	    String fileName = "";
    	    // Try to locate the file
            is = RunTest.loadTestResource(scriptName); 
    		if ( is == null )
    			System.out.println("Could not locate: " + scriptName);
    		else
    		{
    		    // Copy the support file so the test can use it
    			int index = scriptName.lastIndexOf('/');
    			fileName = scriptName.substring(index+1);
 //   			suppFile = new File((new File(outDir, fileName)).getCanonicalPath());

		//these calls to getCanonicalPath catch IOExceptions as a workaround to
		//a bug in the EPOC jvm. 
    		try {suppFile = new File((new File(outDir, fileName)).getCanonicalPath());}
		catch (IOException e) {
		    File f = new File(outDir, fileName);
		    FileWriter fw = new FileWriter(f);
		    fw.close();
		    suppFile = new File(f.getCanonicalPath());
		}


    			FileOutputStream fos = new FileOutputStream(suppFile);
                byte[] data = new byte[4096];
                int len;
    			while ((len = is.read(data)) != -1)
    			{
    			    fos.write(data, 0, len);
    			}
    			fos.close();
			}
        }
	}
}
