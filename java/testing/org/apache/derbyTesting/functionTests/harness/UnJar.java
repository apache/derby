/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.io.*;

/**
  The upgrade tests use jar files containing older version
  databases. These need to be "unjarred" in order to do the tests.
  */
public class UnJar
{
 
    public UnJar()
    {
    }
    
    public static void main(String args[]) throws Exception
    {
        UnJar uj = new UnJar();
        uj.unjar(args[0], null, true);
    }
    
    public static void unjar(String jarname, String outputdir, boolean useprocess)
        throws ClassNotFoundException, IOException
    {
        if (outputdir == null)
            outputdir = System.getProperty("user.dir");
        
	    InputStream is =
            RunTest.loadTestResource("upgrade" + '/' + jarname);
        if (is == null)
        {
            System.out.println("File not found: " + jarname);
            System.exit(1);
        }
        
        // Copy to the current directory in order to unjar it
        //System.out.println("Copy the jarfile to: " + outputdir);
        File jarFile = new File((new File(outputdir, jarname)).getCanonicalPath());
        //System.out.println("jarFile: " + jarFile.getPath());
    	FileOutputStream fos = new FileOutputStream(jarFile);
        byte[] data = new byte[1024];
        int len;
    	while ((len = is.read(data)) != -1)
    	{
    	    fos.write(data, 0, len);
    	}
    	fos.close();
        
        // Now unjar the file
        String jarCmd = "jar xf " + jarFile.getPath();
        if ( useprocess == true )
        {
            // Now execute the jar command
            Process pr = null;
        	try
        	{
        		//System.out.println("Use process to execute: " + jarCmd);
                pr = Runtime.getRuntime().exec(jarCmd);
                
                pr.waitFor();
                //System.out.println("Process done.");
                pr.destroy();
            }
            catch(Throwable t)
            {
                System.out.println("Process exception: " + t.getMessage());
                if (pr != null)
                {
                    pr.destroy();
                    pr = null;
                }
            }
        }
        else
        {
            System.out.println("Jar not implemented yet with useprocess=false");
        }
    }
}
		
			
