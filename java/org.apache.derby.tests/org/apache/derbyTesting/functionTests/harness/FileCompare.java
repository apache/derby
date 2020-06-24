/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.FileCompare

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

/***
 * FileCompare.java
 *
 * Compare two files using SimpleDiff
 * Purpose: simulate diff
 * Note: if usesysdiff=true, we execute the system's diff
 ***/

import java.io.*;
import java.util.StringTokenizer;
import java.util.Properties;

import org.apache.derby.shared.common.info.ProductGenusNames;
import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derby.shared.common.reference.ModuleUtil;

public class FileCompare
{

    private String testBase;
    private String framework;
    private String jvmName;
    private String jvmString;
    private String serverJvm;
    private int iminor;
    private boolean searchJdk12 = false;
    private boolean searchJdk13 = false;
    private boolean searchJdk14 = false;
    private int driverVersionMajor = 0;
    private int driverVersionMinor = 0;
    private boolean searchFrame;
    private boolean searchDriverVersion;
    private InputStream master = null;
    private boolean verbose;
    
    public FileCompare()
    {
      verbose = Boolean.getBoolean("verbose");
    }

    // The arguments should be the names of the input and output files
    public boolean exec(String outfile, File outDir, PrintWriter pwDiff,
        String testBaseOrig, String framework, String jvmName,
        int iminor, boolean useprocess, boolean usesysdiff, 
        String canondir, String canonpath, String serverJvm)
        throws IOException, ClassNotFoundException
    {
        testBase = testBaseOrig;
        this.framework = framework;
        this.jvmName = jvmName;
        this.iminor = iminor;
        this.jvmString = jvmName;
        this.serverJvm = serverJvm;

        BufferedReader outFile;
        BufferedReader masterFile;
        StringBuffer sb = new StringBuffer();
 
        // If framework is DerbyNet, we may need to check subdirs of the master canon dir
        // for specific masters by client  we're running against. So, get version
        // for later use if that is the case.
        if (framework.startsWith("DerbyNet"))
        {
	  try	
	  {
        Class<?> c = Class.forName(NetServer.getDriverName(framework));
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        Object o = c.getConstructor().newInstance();
        driverVersionMajor = (Integer) c.getMethod("getMajorVersion").invoke(o);
        driverVersionMinor = (Integer) c.getMethod("getMinorVersion").invoke(o);
            if (framework.startsWith("DerbyNet")) searchDriverVersion = true;
	  } catch ( Exception e )
	  {
	    //if anything goes wrong, make sure the driver version values are set to zero
	    //forget about it.
		  
		  System.out.println("Cannot determine driver version:" + e);
		  driverVersionMinor = 0;
		  driverVersionMajor = 0;
		  searchDriverVersion = false;
 	  }
        }
        

        // The outfile name is known -- outfile
        // But the master canon needs to be located
        // The user can set canondir (or it defaults to "master")
        String topdir = "";
        if ( (canondir != null) && (canondir.length()>0) )
            topdir = canondir;
        else {
			// if this is using product jars, use product_master first
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            String resourceName = "/" + ProductGenusNames.DBMS_INFO;
            InputStream is;
            if (JVMInfo.isModuleAware())
            {
                Module engineModule = ModuleUtil.derbyModule(ModuleUtil.ENGINE_MODULE_NAME);
                is = engineModule.getResourceAsStream(resourceName);
            }
            else
            {
                Class c = FileCompare.class; // get our class loader
                is = c.getResourceAsStream(resourceName);
            }
            
			Properties dbprop = new Properties();
			dbprop.load(is);
			is.close();

			String filename=dbprop.getProperty("derby.product.file");
			if (filename != null) {
				//looks like it might be one of our jars?
				if (filename.startsWith("derby") && filename.endsWith(".jar")) {
					canondir = "product_master"; // remember redirection
                    topdir = "product_master";
				}
				else
                    topdir = "master";
			}
			else
                topdir = "master";
		}

        // There can be subdirs under the master for framework, jvm
        String subdir = "";
        boolean searchDefault = true; // if no framework or special jvm
        boolean searchBoth = false;
        boolean searchJvm = false;
        if ( (framework != null) && (framework.length()>0) )
        {
            searchFrame = true;
            subdir = framework;
        }
        if ( (jvmName != null) && (jvmName.length()>0)
                & (!jvmName.equals("currentjvm")) )
        {
            searchJvm = true;
            if (searchFrame)
                searchBoth = true;
            if ( iminor >= 2 ) // jdk12 or higher may use jdk12 masters
                jvmString = "jdk12";
	    if ( iminor >= 2 ) searchJdk12 = true;
            if ( iminor >= 3 ) searchJdk13 = true;
            if ( iminor >= 4 ) searchJdk14 = true;
            subdir += jvmName;
        }

        if ( searchFrame || searchJvm || searchBoth )
            searchDefault = false;
       
        sb.append(topdir);
        if (subdir.length()>0)
            sb.append(subdir + '/');
        sb.append(testBase + ".out");
        String masterfilename = sb.toString();
	
	InputStream is = null;
        
        // Now try to locate the master file
	
        if (is == null)
	{
	  searchCanondir(topdir);
	  is = master;
	}
                
        // If the master is still not found, create an empty master
        if ( is == null )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-689
			is = new ByteArrayInputStream( new byte[] {} );
        }
		// compress blanks in output columns to make up for column width differences
		// for JCC output
		if (NetServer.isClientConnection(framework))
		{
            try
            {
		        Sed sed = new Sed();
				File JCCOutFile = new File(outDir, testBase + ".tmpmstr");
		        sed.execJCC(is, JCCOutFile);
				is = new FileInputStream(JCCOutFile);
		    }
		    catch (ClassFormatError cfe)
		    {
		        System.out.println("SED Error: " + cfe.getMessage());
		    }
		}	
//IC see: https://issues.apache.org/jira/browse/DERBY-658
        else 
        {
            // read in in fixed format, but write out relying on default encoding
            File EncodedOutFile = new File(outDir, testBase + ".tmpmstr");
            BufferedReader inFile = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            
            PrintWriter pw = new PrintWriter
            ( new BufferedWriter(new FileWriter(EncodedOutFile), 10000), true );
            int c;
            while ((c = inFile.read()) != -1)
                pw.write(c);
            pw.flush();
            pw.close();
            is = new FileInputStream(EncodedOutFile);
        }	
        
        // Define the input and output files
        outFile = new BufferedReader(new FileReader(outfile));
        masterFile = new BufferedReader(new InputStreamReader(is));
        
        // Do the comparison (diff)
        if (usesysdiff == true)
            return doSysDiff(is, testBase, outfile, outDir, pwDiff);
        else
        {
            return doDiff2(outFile,masterFile, pwDiff);
        }
    }

    public boolean doDiff2(BufferedReader outFile, BufferedReader masterFile, PrintWriter pwDiff) throws IOException {
		return ((new SimpleDiff()).doWork(masterFile,outFile,pwDiff));
    }

    public boolean doSysDiff(InputStream masterIS, String testBase, String outfile,
        File outDir, PrintWriter pwDiff)
        throws IOException
    {
        // Create a temp file to copy the master (located as an InputStream)
        BufferedReader in =
            new BufferedReader(new InputStreamReader(masterIS));
        File tempMaster =
            new File((new File(outDir,testBase + ".master")).getCanonicalPath());

        // Create a PrintWriter for copying the master temporarily for the diff
        PrintWriter pwMaster = new PrintWriter( new BufferedWriter
            (new FileWriter(tempMaster.getPath()), 10000), true );
        String str = "";
        while ( (str = in.readLine()) != null )
        {
            pwMaster.println(str);
        }
        pwMaster.close();
        pwMaster = null;
        in = null;

        String diffs = "0";
        // Now create a process and do the system diff, capture to .out
        Process pr = null;
        try
        {
            StringBuffer sb = new StringBuffer();
            sb.append("diff ");
            sb.append(tempMaster.getCanonicalPath());
            sb.append(" ");
            sb.append(outfile);
            String diffCmd = sb.toString();

            //System.out.println("diffCmd = " + diffCmd);

            pr = Runtime.getRuntime().exec(diffCmd);

            // We need the process inputstream to capture into the diff file
            //System.out.println("Capture the process InputStream...");
            BackgroundStreamDrainer stdout =
                new BackgroundStreamDrainer(pr.getInputStream(), null);
            BackgroundStreamDrainer stderr =
                new BackgroundStreamDrainer(pr.getErrorStream(), null);

            pr.waitFor();
            String result = HandleResult.handleResult(pr.exitValue(),
                stdout.getData(), stderr.getData(), pwDiff);
            diffs = result.substring( result.lastIndexOf(',')+1 );
            //System.out.println("diffs: " + diffs);
            pr.destroy();
            pr = null;
        }
        catch(Throwable t)
        {
            System.out.println("Process exception: " + t);
            if (pr != null)
            {
                pr.destroy();
                pr = null;
            }
        }
        tempMaster.delete();
        if ( diffs.equals("0") )
            return false;
        else
            return true;
    }

    public boolean doDiff(BufferedReader outFile, BufferedReader masterFile, PrintWriter pwDiff) throws IOException {
        String str1;
        String str2;
        boolean diff = false;

        int line = 0;
        int diffnum = 0;
        int diffline = 0;
        while ( (str1 = outFile.readLine()) != null )
        {
            line++;
            str1 = str1.trim();
            //System.out.println("Reading line: " + line);
            // Read the line from the master file and compare
            if ( (str2 = masterFile.readLine()) != null )
            {
                str2 = str2.trim();
                if (!str1.equals(str2))
                {
                    // Some lines diff because of too many spaces
                    StringBuffer sb1 = new StringBuffer();
                    StringBuffer sb2 = new StringBuffer();
                    StringTokenizer st1 = new StringTokenizer(str1);
                    while (st1.hasMoreTokens())
                    {
                        sb1.append(st1.nextToken());
                    }
                    //System.out.println("Out line: " + sb1.toString());

                    StringTokenizer st2 = new StringTokenizer(str2);
                    while (st2.hasMoreTokens())
                    {
                        sb2.append(st2.nextToken());
                    }
                    //System.out.println("Master line: " + sb2.toString());

                    if ( sb1.toString().equals(sb2.toString()) )
                        diff = false;
                    // If the two lines are dashes, but wrong #, just ignore
                    else if ( (str1.startsWith("-----")) && (str1.endsWith("-----")) )
                    {
                        if ( (str2.startsWith("-----")) && (str2.endsWith("-----")) )
                            diff = false;
                    }
                    else
                    {
                        diff = true;
                        diffnum++;
                        System.out.println("Diff occurred at line: " + line);
                        pwDiff.println("Diff occurred  at line: " + line);
                        pwDiff.flush();
                        break;
                    }
                }
                else
                {
                    diff = false;
                }
            }
        } // end while
        outFile.close();
        masterFile.close();
        return diff;
    }// end exec

    private void searchCanondir(String canondir)
    {
	String prefix = canondir + '/';
	if (master == null && searchFrame) searchFramework(prefix);
        if (master == null) searchJvm(prefix);
	if (master == null && searchDriverVersion) searchDriverVersion(prefix);
	if (master == null) getmaster(prefix);
	if (master == null && canondir != "master") searchCanondir("master");
    }

    private void searchJvm(String prefix)
    {
	// The JVM search follows the following pattern, with one exception:
	// first search jvmName (to support unnamed/non-IBM or Sun JVMs)
	// if vendor == IBM, search ibm+rev then jdk+rev, decrementing rev by one until rev=13,
	// in each dir, search framework and driver version if applicable.
	// BUT, if it's j9, unless j9dee, first j9_foundation, then search j9_22 for 22, otherwise, j9_13 then
	// the normal ibm13 search pattern: ibm13 then jdk13.

	String newprefix;
//IC see: https://issues.apache.org/jira/browse/DERBY-3972
	if ((jvmName.startsWith("j9") || (serverJvm != null && serverJvm.startsWith("j9")))
            && (!jvmName.startsWith("j9dee")))
	{
	    if (jvmName.startsWith("j9_foundation"))
            {
                newprefix = prefix + "j9_foundation" + '/';
		if (master == null && searchDriverVersion) searchDriverVersion(newprefix);
		if (master == null) getmaster(newprefix);
            }
	    else
            {
                newprefix = prefix + jvmName + '/';
                if ((!jvmName.equals("j9_13")) && (iminor  > 1))
                {
                    for (int i = iminor; i > 1; i--)
                    {
                        if (master == null) 
                        {
                            newprefix = prefix + "j9_2" + i + '/'; 
                            getmaster(newprefix);
                        }
                    }
                }
                if (master == null) newprefix = prefix + "j9_13" + '/';
            }
	    if (master == null && searchDriverVersion) searchDriverVersion(newprefix);
	    if (master == null) getmaster(newprefix);
	    
	}
	for (int i = iminor; i >= 2; i--)
	{
	    if (jvmName.startsWith("ibm"))
            {
		newprefix = prefix + "ibm1" + i + '/';
		if (master == null && searchDriverVersion) searchDriverVersion(newprefix);
		if (master == null) getmaster(newprefix);
	    }
	    newprefix = prefix + "jdk1" + i + '/';
	    if (master == null && searchDriverVersion) searchDriverVersion(newprefix);
	    if (master == null) getmaster(newprefix);
        } 
    }

    private void searchFramework(String prefix)
    {
        String newprefix;
	newprefix = prefix + framework + '/';
	if (master == null) searchJvm(newprefix);
	if (master == null && searchDriverVersion) searchDriverVersion(newprefix);
	if (master == null) getmaster(newprefix);
    }

    private void searchDriverVersion(String prefix)
    {
	// It is not sufficient to simply search the current driver version. 
	// We must search down through the versions to find the newest applicable master. 
        
	String newprefix;
	
	for (int j = ((driverVersionMajor * 10) + driverVersionMinor); j >= 10; j--)
	{
            newprefix = prefix + "ver" + j / 10 + "." + j % 10 + '/';
	    if (master == null) getmaster(newprefix); 
        }
    }

    private void getmaster(String prefix)
    {
        String fullname = prefix + testBase + ".out";
        master = RunTest.loadTestResource(fullname);
        if (master != null)
            if (verbose) System.out.println("MasterFileName = "+fullname);
    }
}
