/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
