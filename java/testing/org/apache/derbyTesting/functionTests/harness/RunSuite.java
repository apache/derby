/* 

   Derby - Class org.apache.derbyTesting.functionTests.harness.RunSuite

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

import org.apache.derby.tools.sysinfo;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.lang.ClassNotFoundException;
import java.sql.Timestamp;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;
import java.util.StringTokenizer;

public class RunSuite
{

    static final boolean verbose=true;

    static String suites; // list of subsuites in this suite
    static Vector suitesToRun; // Vector of suites to run

    // Properties which may be specified
	static String jvmName = "";
	static String javaCmd = "java";
	static String jvmflags = ""; // jvm flags as one string
	static String javaVersion;
	static String classpath;
	static String classpathServer;
	static String testJavaFlags = ""; // formerly systest_javaflags
	static String userdir;
	static String framework;
	static String runwithibmjvm;
	static String excludeJCC;
	static boolean useprocess = true;
	static boolean skipsed = false;
	static String systemdiff = "false";
	static String topSuiteName = "";
	static String outputdir; // location of output (default is userdir)
	static String outcopy = "false"; // true if support files should go to outDir
	static String canondir; // location of master dir (default is master)
	static String bootcp; //  path for j9 bootclasspath setting
	static String serverJvm; //  path for j9 bootclasspath setting
	static String ijdefaultResourcePackage; // for ij tests only
	static String debug; // for setting verbose mode to pass down to RunTest
    static String timeout; // to allow killing a hanging test
    static String shutdownurl; //used mainly by useprocess=false tests
	static String reportstderr; // can set to disable (to turn off JIT errors, etc.)
	static Properties suiteProperties;
	static Properties specialProperties;

	// Output variables
    static PrintWriter pwOut = null; // for writing suite output
    static File outDir; // test out dir
    static File runDir; // where the suite/tests are run
    static File outFile; // suite output file

	public static void main(String[] args) throws Exception
	{
		if ((System.getProperty("java.vm.name") != null) && System.getProperty("java.vm.name").equals("J9"))
		{
		 		javaCmd = "j9";
				String javaHome = System.getProperty("java.home");
				//jvmflags = "-Xiss16k -Xss512k -Xmso16k -Xmx392388k";
		}
		String j9config = System.getProperty("com.ibm.oti.configuration");
		if (j9config != null) 
			if (j9config.equals("foun")) 
				jvmName="j9_foundation";
			else if (j9config.equals("max"))
				jvmName="j9_13";

	    String suiteName = args[0];
	    if ( suiteName == null )
	    {
	        System.out.println("No suite name argument.");
	        System.exit(1);
	    }
	    topSuiteName = suiteName;
	    System.out.println("Top suite: " + suiteName);

	    // suiteName may be one suite or a list of suites
        suitesToRun = new Vector();

        // Get properties set in the suite's properties file
		suiteProperties = getSuiteProperties(suiteName, true);

		// There may be system properties which will override
		// the suiteProperties. This will make it easier when you
		// do not want to edit the suite props for a special case
		getSystemProperties();

        // Get any special properties that are not the usual
        // expected properties (we separate these from suiteProperties
        // to avoid conflicts)
        specialProperties =
            SpecialFlags.getSpecialProperties(suiteProperties);

        // Setup the initial output
        setOutput(suiteName);

        // Get the current time to write a timestamp
        String startTime = CurrentTime.getTime();

        pwOut.println("******* Start Suite: " + suiteName +
            " " + startTime + " *******");

        // Write sysinfo to the output file
        if (useprocess) // doesn't work on Mac
        {
            SysInfoLog sysLog= new SysInfoLog();
            sysLog.exec(jvmName, javaCmd, classpath, framework, pwOut, useprocess);
        }
            
        getSuitesList(suiteName, true);
        
        // Get the current time to write a timestamp
        String endTime = CurrentTime.getTime();
        pwOut.println("******* End Suite: " + suiteName +
            " " + endTime + " *******");
        pwOut.close();
        
		String genrep = System.getProperty("genrep");
		boolean isGenrep = true;
		if (genrep!=null) isGenrep = "true".equalsIgnoreCase(genrep);
		if (isGenrep) 
		{
		    String[] genargs = new String[6];
		    genargs[0] = args[0];
		    genargs[1] = jvmName;
		    genargs[2] = javaCmd;
		    genargs[3] = classpath;
		    genargs[4] = framework;
		    if (useprocess)
		        genargs[5] = "true";
		    else
		        genargs[5] = "false";
		    GenerateReport.main(genargs);
		}
	}

    static void getSuitesList(String topparent, boolean isTop)
        throws Exception, ClassNotFoundException, IOException
    {
		// Get the suite properties if it exists
        Properties p;
        if ( (suites == null) || (suites.length()==0) )
        {
            // There is a single suite, not a list, just add it
            if (verbose) System.out.println("Suite to run: " + topparent+":"+topparent);
            suitesToRun.addElement(topparent+":"+topparent);
            // Use RunList class to issue the RunTest commands
            if (verbose) System.out.println("Now do RunList");
            //System.out.println("skipsed: " + skipsed);
            RunList rl = new RunList(suitesToRun, runDir, outDir, pwOut,
                suiteProperties, specialProperties, topparent);
            suitesToRun.removeAllElements();
        }
        else
        {
            isTop = false;
            // Build the Vector from suites string
	        StringTokenizer st = new StringTokenizer(suites);
	        String subparent = "";
            while (st.hasMoreTokens())
            {
                subparent = st.nextToken();
                p = getSuiteProperties(subparent, isTop);
                if ( (p.getProperty("suites") == null) || (subparent.equals(topparent)) )
                {
                    suitesToRun.addElement(topparent+":"+subparent);
                    //System.out.println("Add to suitesToRun: " + topparent+":"+subparent);
                    // Use RunList class to issue the RunTest commands
                    if (verbose) System.out.println("Now do RunList");
                    //System.out.println("skipsed: " + skipsed);
                    RunList rl = new RunList(suitesToRun, runDir, outDir, pwOut,
                        suiteProperties, specialProperties, topparent);
                    suitesToRun.removeAllElements();
                }
                else // This suite also has nested suites
                {
                    String sublist = p.getProperty("suites");
                    //System.out.println("list for this SubSuite= " + sublist);
                    BuildSuitesVector(subparent, sublist);
                    // Use RunList class to issue the RunTest commands
                    if (verbose) System.out.println("Now do RunList");
                    //System.out.println("skipsed: " + skipsed);
                    RunList rl = new RunList(suitesToRun, runDir, outDir, pwOut,
                        suiteProperties, specialProperties, subparent);
                    suitesToRun.removeAllElements();                    
                }
            }
        }
    }
    
    static void BuildSuitesVector(String parent, String subsuites)
        throws ClassNotFoundException, IOException
    {
        Properties p;
        StringTokenizer st = new StringTokenizer(subsuites);
        String child = "";
        while (st.hasMoreTokens())
        {
            child = st.nextToken();
            if (child.equals(parent))
            {
                suitesToRun.addElement(parent+":"+child);
                //System.out.println("Add this: " + parent+":"+child);
            }
            else
            {
                p = getSuiteProperties(child, false);
                if ( p.getProperty("suites") == null )
                {
                    suitesToRun.addElement(parent+":"+child);
                    //System.out.println("Add this: " + parent+":"+child);
                }
                else
                {
                    String moresuites = p.getProperty("suites");
                    BuildSuitesVector(child, moresuites);
                }
            }
        }
    }
            
    
    static Properties getSuiteProperties(String suiteName, boolean isTop)
        throws ClassNotFoundException, IOException
    {
        // Locate the suite's config file and get the properties
        // The file should be in the harness dir or user.dir
        String suiteProps = "suites" + '/' + suiteName + ".properties";
        userdir = System.getProperty("user.dir");

        InputStream is = RunTest.loadTestResource(suiteProps);
        if (is == null)
        {
            // Look in userdir
            suiteProps = userdir + '/' + suiteName + ".properties";
            is = RunTest.loadTestResource(suiteProps);
        }
        Properties p = new Properties();
        if (is == null)
            return p;

        p.load(is);
        // The top level suite may have special properties
        // which get propagated to any subsuites
        if (isTop == true)
        {
			String tmpjvmName=jvmName;	
            jvmName = p.getProperty("jvm");
		    if ( (jvmName == null) || (jvmName.length()==0) )
		    {
		        javaVersion = System.getProperty("java.version");
		    }
		    else
		        javaVersion = jvmName;
    		    
		    JavaVersionHolder jvh = new JavaVersionHolder(javaVersion);
		    String majorVersion = jvh.getMajorVersion();
		    String minorVersion = jvh.getMinorVersion();
            int iminor = jvh.getMinorNumber();
            int imajor = jvh.getMajorNumber();
    		
		    if ( (iminor < 2) && (imajor < 2) )
		        jvmName = "currentjvm";
		    else
		        jvmName = "jdk" + majorVersion + minorVersion;
		if ( tmpjvmName != null)
			jvmName= tmpjvmName;
            javaCmd = p.getProperty("javaCmd");
            jvmflags = p.getProperty("jvmflags");
            testJavaFlags = p.getProperty("testJavaFlags");
            classpath = p.getProperty("classpath");
            classpathServer = p.getProperty("classpathServer");
            framework = p.getProperty("framework");
		    String usepr = p.getProperty("useprocess");
		    if (usepr != null)
		    {
		        usepr = usepr.toLowerCase();
		        if (usepr.equals("false"))
		            useprocess = false;
		        else
		            useprocess = true;
		    }
		    else
		        useprocess = true;

            String nosed = p.getProperty("skipsed");
            if (nosed != null)
            {
                nosed = nosed.toLowerCase();
                if (nosed.equals("true"))
                    skipsed = true;
                else
                    skipsed = false;
            }
            else
                skipsed = false;
                
            outputdir = p.getProperty("outputdir");
            canondir = p.getProperty("canondir");
            bootcp = p.getProperty("bootcp");
            serverJvm = p.getProperty("serverJvm");
            systemdiff = p.getProperty("systemdiff");
            ijdefaultResourcePackage = p.getProperty("ij.defaultResourcePackage");
            outcopy = p.getProperty("outcopy");
            debug = p.getProperty("verbose");
            reportstderr = p.getProperty("reportstderr");
            timeout = p.getProperty("timeout");
            shutdownurl = p.getProperty("shutdownurl");
        }
        suites = p.getProperty("suites");
		return p;
    }

    private static void getSystemProperties()
    {
        // Get any properties specified on the command line
        // which may not have been specified in the suite prop file
        Properties sp = System.getProperties();
        String searchCP = sp.getProperty("ij.searchClassPath");
        if (searchCP != null)
            suiteProperties.put("ij.searchClassPath", searchCP);
		String frm = sp.getProperty("framework");
		if ( (frm != null) && (!frm.equals("embedded")) )
		{
		    framework = frm;
		    suiteProperties.put("framework", framework);
		}
		String j = sp.getProperty("jvm");
		if (j != null)
		    suiteProperties.put("jversion", j);
		
		String jcmd = sp.getProperty("javaCmd");
		if ((System.getProperty("java.vm.name") != null) && System.getProperty("java.vm.name").equals("J9"))
			jcmd = "j9";
		if (jcmd != null)
		{
		    javaCmd = jcmd;
		    suiteProperties.put("javaCmd", javaCmd);
		}
		String jflags = sp.getProperty("jvmflags");
		if (jflags != null)
		{
		    jvmflags = jflags;
		    suiteProperties.put("jvmflags", jvmflags);
		}
		String testflags = sp.getProperty("testJavaFlags");
		if (testflags != null)
		{
		    if (testJavaFlags == null)
		        testJavaFlags = testflags;
		    else // add to testJavaFlags
		        testJavaFlags = testJavaFlags + "^" + testflags;
		    suiteProperties.put("testJavaFlags", testJavaFlags);
		}
		String clpth = sp.getProperty("classpath");
		if (clpth != null)
		{
		    classpath = clpth;
		    suiteProperties.put("classpath", classpath);
		}
		String clsrv = sp.getProperty("classpathServer");
		if ( (clsrv != null) && (!clsrv.startsWith("${")) )
		{
		    classpathServer = clsrv;
		    suiteProperties.put("classpathServer", clsrv);
		}
		String usesys = sp.getProperty("usesystem");
		if (usesys != null)
		    suiteProperties.put("usesystem", usesys);
		String jarf = sp.getProperty("jarfile");
		if (jarf != null)
		    suiteProperties.put("jarfile", jarf);
		String upgtest = sp.getProperty("upgradetest");
		if (upgtest != null)
		    suiteProperties.put("upgradetest", upgtest);
		String rep = sp.getProperty("replication");
		if (rep != null)
		    suiteProperties.put("replication", rep);
		String encrypt = sp.getProperty("encryption");
		if (encrypt != null)
		    suiteProperties.put("encryption", encrypt);
		String jdk12test = sp.getProperty("jdk12test");
		if (jdk12test != null)
		    suiteProperties.put("jdk12test", jdk12test);
		String jdk12ex = sp.getProperty("jdk12exttest");
		if (jdk12ex != null)
		    suiteProperties.put("jdk12exttest", jdk12ex);
		String runwithibmjvm = sp.getProperty("runwithibmjvm");
		if (runwithibmjvm != null)
		    suiteProperties.put("runwithibmjvm", runwithibmjvm);
		String excludeJCC = sp.getProperty("excludeJCC");
		if (excludeJCC != null)
		    suiteProperties.put("excludeJCC", excludeJCC);
		String keep = sp.getProperty("keepfiles");
		if (keep != null)
		    suiteProperties.put("keepfiles", keep);
		String outd = sp.getProperty("outputdir");
		if (outd != null)
		{
		    outputdir = outd;
		    suiteProperties.put("outputdir", outputdir);
		}
		String canond = sp.getProperty("canondir");
		if (canond != null)
		{
		    canondir = canond;
		    suiteProperties.put("canondir", canondir);
		}
		String j9bootcp = sp.getProperty("bootcp");
		if (j9bootcp != null)
		{
		    bootcp = j9bootcp;
		    suiteProperties.put("bootcp", bootcp);
		}
		String serverJvm = sp.getProperty("serverJvm");
		if (serverJvm != null)
		    suiteProperties.put("serverJvm", serverJvm);
		String testout = sp.getProperty("testoutname");
		if (testout != null)
		    suiteProperties.put("testoutname", testout); // toursDemo
		String mtdir = sp.getProperty("mtestdir"); // used by multi tests
		if (mtdir != null)
		    suiteProperties.put("mtestdir", mtdir);
		String usepr = sp.getProperty("useprocess");
		if (usepr != null)
		{
		    // Some platforms cannot handle process exec
		    usepr = usepr.toLowerCase();
		    if (usepr.equals("false"))
		    {
		        useprocess = false;
		        suiteProperties.put("useprocess", usepr);
		    }
		}
		
        String nosed = sp.getProperty("skipsed");
        if (nosed != null)
        {
            // in some cases (like locales, we may want to skip the Sed)
            nosed = nosed.toLowerCase();
            if (nosed.equals("true"))
            {
                skipsed = true;
                suiteProperties.put("skipsed", nosed);
            }
        }
		
		String sysdiff = sp.getProperty("systemdiff");
		if (sysdiff != null)
		{
		    // Use system diff if set to true
		    sysdiff = sysdiff.toLowerCase();
		    if (sysdiff.equals("true"))
		        suiteProperties.put("systemdiff", "true");
		}
		String defrespckg = sp.getProperty("ij.defaultResourcePackage");
		if (defrespckg != null)
		    suiteProperties.put("ij.defaultResourcePackage", defrespckg);
		String outcpy = sp.getProperty("outcopy");
		if (outcpy != null)
		    suiteProperties.put("outcopy", outcpy);
		String topsuite = sp.getProperty("suitename");
		if (topsuite != null)
		    suiteProperties.put("suitename", topsuite);
		else
		    suiteProperties.put("suitename", topSuiteName);
        String dbug = sp.getProperty("verbose");
		if (dbug != null)
		    suiteProperties.put("verbose", dbug);
		String reporterr = sp.getProperty("reportstderr");
		if (reporterr != null)
		    suiteProperties.put("reportstderr", reporterr);
		String tout = sp.getProperty("timeout");
		if (tout != null)
		    suiteProperties.put("timeout", tout);
    }

    private static void setOutput(String suiteName)
        throws ClassNotFoundException, FileNotFoundException, IOException
    {
        boolean status = false;
        // Use the defined output directory or user.dir by default
        File tmpoutDir;
        if ( (outputdir == null) || (outputdir.length()==0) )
        {
            tmpoutDir =
		        new File((new File(userdir)).getCanonicalPath());
		}
        else
        {
            tmpoutDir =
                new File((new File(outputdir)).getCanonicalPath());
		}
        outDir = tmpoutDir;
        outDir.mkdir();
        
		// runDir is where the suites/tests are run and where
		// any support files or scripts will be expected to live
		runDir =
		    new File((new File(userdir)).getCanonicalPath());
		    
        // Set the suite property outputdir
        suiteProperties.put("outputdir", outDir.getCanonicalPath());

        // Define the final suite summary file file
        outFile = new File(outDir, suiteName + ".sum");
        if (outFile.exists())
            status = outFile.delete();
 
        // Define the suite.pass file
        File passFile = new File(outDir, suiteName + ".pass");
        if (passFile.exists())
            status = passFile.delete();

        // Define the suite.fail file
        File failFile = new File(outDir, suiteName + ".fail");
        if (failFile.exists())
            status = failFile.delete();

        // Create a PrintWriter for writing env and test info to the diff file
        pwOut = new PrintWriter
            (new BufferedWriter(new FileWriter(outFile.getPath()), 4096), true);
    }
}
