/*

   Derby - Class org.apache.derby.impl.tools.sysinfo.Main

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.tools.sysinfo;

import java.util.Locale;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.InputStream;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.StringTokenizer;
import java.io.File;
import java.util.zip.ZipFile;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.io.FileInputStream;
import java.util.Vector;
import java.io.InputStream;
import java.lang.reflect.Method;

import org.apache.derby.iapi.services.info.PropertyNames;
import org.apache.derby.iapi.services.info.ProductVersionHolder;
import org.apache.derby.iapi.services.info.ProductGenusNames;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.tools.i18n.*;


/**
  <i>Copyright &#169; 1998, Cloudscape, Inc.   All rights reserved.</i>

  <P>
  SysInfo reports values relevant to the Cloudscape product found on
  the CLASSPATH.  It looks for a file called sysinfo.properties in
  the CLASSPATH using getResourceAsStream. If the file
  is not found, or some other exception occurs, the
  value returned will be that set for the key
  SysInfo.failureTag, or be the value "<info unavailable>".

  <P>
  This class can be used to print out system information at the
  command line by issuing the command:
  <PRE>
    java org.apache.derby.tools.sysinfo
  </PRE>
  Alternatively, you can use SysInfo within your program to display
  Cloudscape information; a Cloudscape version string is returned by this Java code:
  <PRE>
    new Main().toString();
  </PRE>

 */


public final class Main {

  /**
    Application entry point for SysInfo.   This will print out
    the Cloudscape product information as well as a snapshot of
    the System properties.
  */
  public static void main(String args[]) {
        // adjust the application in accordance with derby.ui.locale and derby.ui.codeset
        LocalizedResource.getInstance();

		LocalizedOutput out;

        //using AppStreamReader(extends InputStreamReader) for conversion into specific codeset

		out = LocalizedResource.OutputWriter();

       // because we're in a static method, we need to
       // get our own instance variable
    parseArgs (args);

    if (cptester == true)
	getClasspathInfo (args, out);
    else
	getMainInfo (out, setPause);

  } // end of main (String args[])

public static void getMainInfo (java.io.PrintWriter aw, boolean pause) {

    aw.println (javaSep);
    reportJavaInfo (aw);
    aw.println (jbmsSep);
    reportCloudscape (aw);

    aw.println (sep);

    // Locales info
    try {
      reportLocales (aw);
    }
    catch (Exception e) {

      aw.println (Main.getTextMessage ("SIF01.Q"));
      aw.println (Main.getTextMessage ("SIF01.B"));
    }


    if (pause) {
     pause();
    }

  } // end of getMainInfo (AppStreamWriter aw, boolean printLicense, boolean pause)


  private static boolean setPause = false;

  private static boolean setLicense = false;

  private static boolean cptester = false;

  private static void parseArgs (String args[]) {

    if (args == null) {

      return;
    }


    for (int i = 0; i < args.length; i++) {

      if (args[i].equals ("-pause")) {

        setPause = true;
      }

      if (args[i].equals ("-cp")) {

        cptester=true;
      }

    } // end for

  } // end of parseArgs (String args[])


  /**
    For the benefit of DOS box users, this method waits for input
    before returning
  */
  private static void pause () {

    try {

      System.out.print (Main.getTextMessage ("SIF01.C"));
      BufferedReader br = new BufferedReader (new InputStreamReader (System.in));
      br.readLine ();
    }
    catch (IOException ioe) {

      //just return
    }

  } // end of pause ()

  /**
    prints out the jbms info to the specified AppStreamWriter.
    @param aw the AppStreamWriter to use. If null, System.out is
    used
  */

  private static void reportCloudscape (java.io.PrintWriter localAW) {

	  String classpath;

	  try {
		  classpath = System.getProperty("java.class.path");
	  }
	  catch (SecurityException se) {
		  classpath = null;
	  }

    ZipInfoProperties zip[]= Main.getAllInfo (classpath);

    if (zip != null) {

      for (int i = 0; i < zip.length; i++) {

        String thisInfo = "[" + zip[i].getLocation () + "] " +
                                zip[i].getVersionBuildInfo ();

        localAW.println (thisInfo);
      }
    }

    else {

      localAW.println (Main.getTextMessage ("SIF01.D"));
    }


  } // end of reportCloudscape


  /**
    Writes out the relevant info about the Java environment to
    the specified AppStreamWriter.

    @param aw The AppStreamWriter to write info out to. If this is
    null, the info is written to System.out
  */

  private static void reportJavaInfo (java.io.PrintWriter localAW) {

	

    localAW.println (Main.getTextMessage ("SIF02.A",
                                                           getJavaProperty ("java.version")));

    localAW.println (Main.getTextMessage ("SIF02.B",
                                                           getJavaProperty ("java.vendor")));

    localAW.println (Main.getTextMessage ("SIF02.C",
                                                           getJavaProperty ("java.home")));

    localAW.println (Main.getTextMessage ("SIF02.D",
                                                           getJavaProperty ("java.class.path")));

    localAW.println (Main.getTextMessage ("SIF02.E",
                                                           getJavaProperty ("os.name")));

    localAW.println (Main.getTextMessage ("SIF02.F",
                                                           getJavaProperty ("os.arch")));

    localAW.println (Main.getTextMessage ("SIF02.G",
                                                           getJavaProperty ("os.version")));

    localAW.println (Main.getTextMessage ("SIF02.H",
                                                           getJavaProperty ("user.name")));

    localAW.println (Main.getTextMessage ("SIF02.I",
                                                           getJavaProperty ("user.home")));

    localAW.println (Main.getTextMessage ("SIF02.J",
                                                           getJavaProperty ("user.dir")));

  } // end of reportJavaInfo



  /**
    Return Java properties from java.lang.System. Will catch
    SecurityExceptions and note them for displaying information.

    @return the Java property value or a string capturing a
    security exception.
   */

  private static String getJavaProperty (String whichProperty) {

    String property;
    String unavailable = Main.getTextMessage ("SIF01.H");

    try {

      property = System.getProperty (whichProperty, unavailable);
      return property;
    }
    catch (SecurityException se) {

      return Main.getTextMessage ("SIF01.I", se);
    }

  } // end of getJavaProperty (String whichProperty)



  /**
    for use by the main () method
   */

  private final static String sep     = "------------------------------------------------------";
  private final static String javaSep = Main.getTextMessage ("SIF01.L");

  private final static String jbmsSep = Main.getTextMessage ("SIF01.M");

  private final static String licSep  = Main.getTextMessage ("SIF01.N");

  private final static String locSep  = Main.getTextMessage ("SIF01.P");

  private final static String curLoc  = Main.getTextMessage ("SIF01.T");

  /**
    The name of the failure tag in the information file.
    The failure tag's value provides a default value if
    any other properties are missing.
   */
  private final static String failureTag = Main.getTextMessage ("SIF01.J");

  private static void getClasspathInfo (String args[], java.io.PrintWriter aw) {

    Main.useMe (args, aw);
  }




  /**
    Writes out information about the locales with the
    product.

    @param aw the AppStreamWriter to which the info is written. If this
    value is null, the info is written to System.out

  */
  private static void reportLocales (java.io.PrintWriter localAW) {          // throws StandardException {

    boolean cur_loc = true;

    localAW.println (locSep);

    // Read all the possible locales, and test for each one, if it loads.
    // If so, then read the properties, and print them out.

	Locale[] supportedLocales = Locale.getAvailableLocales();
	String[] stringLocales = new String[supportedLocales.length];
    for (int i = 0; i < supportedLocales.length; i++)
	{
		stringLocales[i] = supportedLocales[i].toString();
	}
	java.util.Arrays.sort(stringLocales);

    Properties p = new Properties ();
    for (int i = 0; i < stringLocales.length; i++) {

      String localeResource =
         "/org/apache/derby/info/locale_" + stringLocales[i] + ".properties";

      try {

        InputStream is = p.getClass().getResourceAsStream (localeResource);

        if (is == null) {
//           localAW.println("resource is null: " + localeResource);
        }
        else {

          try {
			  p.clear();
            p.load (is);
        //Displaying Current Locale
	    if (cur_loc)
		{
	Locale loc = null;
	loc = Locale.getDefault();
        localAW.println(Main.getTextMessage ("SIF01.T") + "  [" + loc.getDisplayLanguage() + "/" +  loc.getDisplayCountry() + " [" + loc + "]]");
		cur_loc = false;
		}

	//Beetle 5079: do not print unlocalized locale names to console, only print locale code.
	String localeName = p.getProperty("derby.locale.external.name");
	localeName = localeName.substring(localeName.indexOf("[")+1);
	localeName = localeName.substring(0,localeName.indexOf("]"));
	
            localAW.println (Main.getTextMessage ("SIF01.R",
                                                                   localeName));


			int major = Integer.valueOf(p.getProperty ("derby.locale.version.major")).intValue();
			int minor = Integer.valueOf(p.getProperty ("derby.locale.version.minor")).intValue();
			int maint = Integer.valueOf(p.getProperty ("derby.locale.version.maint")).intValue();
			String build = p.getProperty ("derby.locale.build.number");

			String lv = ProductVersionHolder.fullVersionString(major, minor, maint, false, build);


            localAW.println (Main.getTextMessage ("SIF01.S", lv));


          }
          catch (IOException ioe) {

            //This case is a bit ugly. If we get an IOException, we return
            //null. Though this correctly reflects that the product is not
            //available for use, it may be confusing to users that we swallow
            //the IO error here.

            localAW.println("Could not get locale properties from : " + is);
          }
        }

      }
      catch (Throwable t) {
        localAW.println ("Could not load resource: " + localeResource);
        localAW.println ("Exception: " + t);
      }

    }


    localAW.println (sep);

  } // end of reportLocales

	/* for arguments, choose from one of:*/
	private static final String EMBEDDED = "embedded";

	/* you can add this if you like*/
	private static final String TOOLS = "tools";

	private static final String NET = "server";
	private static final String CLIENT = "client";

	/* you can add this if you like */

	private static final String MAINUSAGESTRING = "java org.apache.derby.tools.sysinfo -cp";

	private static final String USAGESTRINGPARTA = MAINUSAGESTRING + " [ [ " + EMBEDDED + " ][ " + NET + " ][ " + CLIENT + "] [ " + TOOLS + " ] [ ";
    private static final String USAGESTRINGPARTB = ".class ] ]";

  static  void useMe(String[] args, java.io.PrintWriter pw) {
	  java.io.PrintWriter localPW = pw;

	    if (localPW == null)
	    {
	        localPW = new java.io.PrintWriter(System.out);
	    }


      int length = args.length;
	  if (length==1) {

		  try {
			  tryAllClasspaths(localPW);

		  }

		  catch (Throwable t) {

		  }
	  }
	  else {
		  try {
			  trySomeClasspaths(args, localPW);
		  }

		  catch (Throwable t) {

		  }
	  }

  }





	  private static void tryAllClasspaths(java.io.PrintWriter localPW) throws Throwable {
		  localPW.println(Main.getTextMessage("SIF08.B"));
		  localPW.println(Main.getTextMessage("SIF08.C", MAINUSAGESTRING + " args"));
		  StringBuffer successes = new StringBuffer(Main.getTextMessage("SIF08.D")+ crLf());
		  StringBuffer failures = new StringBuffer(crLf() + Main.getTextMessage("SIF08.E") + crLf());
		  tryCoreClasspath(successes, failures);
		  tryNetClasspath(successes, failures);
		  tryClientClasspath(successes, failures);
		  tryUtilsClasspath(successes, failures);
		  localPW.println(successes.toString());
		  if (!failures.toString().equals(crLf() + Main.getTextMessage("SIF08.E") + crLf())) {
			  localPW.println(failures.toString());
		  }
		  else {

			  localPW.println(Main.getTextMessage("SIF08.F"));
		  }
		  localPW.flush();
	  }

	private static void trySomeClasspaths(String[] args, java.io.PrintWriter localPW) throws Throwable {

		boolean seenArg = false;
		StringBuffer successes = new StringBuffer(Main.getTextMessage("SIF08.D")+ crLf());
		StringBuffer failures = new StringBuffer(crLf() + Main.getTextMessage("SIF08.E") + crLf());

		if (argumentsContain(args, EMBEDDED))
		{

			tryCoreClasspath(successes, failures);
			seenArg =true;

		}
		if (argumentsContain(args,NET)) {
		  tryNetClasspath(successes, failures);
			seenArg =true;

		}
		if (argumentsContain(args,CLIENT)) {
		  tryClientClasspath(successes, failures);
			seenArg =true;

		}

		if (argumentsContain(args,TOOLS) || argumentsContain(args,"utils")) {
		  tryUtilsClasspath(successes, failures);
			seenArg =true;

		}


		String userclass = argumentMatches(args, ".class");
		if (!userclass.equals("")) {
			tryMyClasspath(argumentMatches(args, ".class"), Main.getTextMessage("SIF08.H", userclass), successes, failures);
			seenArg =true;
		}

		if (seenArg)
		{

			localPW.println(successes.toString());
			if (!failures.toString().equals(crLf() + Main.getTextMessage("SIF08.E") + crLf())) {
				localPW.println(failures.toString());
			}
			else {

				localPW.println(Main.getTextMessage("SIF08.F"));
			}
		}
		else
		{
			localPW.println(Main.getTextMessage("SIF08.A", USAGESTRINGPARTA, USAGESTRINGPARTB));
		}
		localPW.flush();

	}

	private static void tryCoreClasspath(StringBuffer successes, StringBuffer failures) {
		tryMyClasspath("org.apache.derby.database.Database", Main.getTextMessage("SIF08.J","derby.jar" ), successes, failures);
	}
	private static void tryNetClasspath(StringBuffer successes, StringBuffer failures) {
		tryMyClasspath("org.apache.derby.drda.NetworkServerControl", Main.getTextMessage("SIF08.I", "derbynet.jar"), successes, failures);
	}
	private static void tryClientClasspath(StringBuffer successes, StringBuffer failures) {
		tryMyClasspath("com.ibm.db2.jcc.DB2Driver", Main.getTextMessage("SIF08.L", "db2jcc.jar"), successes, failures);
	}

	private static void tryUtilsClasspath(StringBuffer successes, StringBuffer failures) {
		tryMyClasspath("org.apache.derby.tools.ij", Main.getTextMessage("SIF08.Q", "derbytools.jar"), successes, failures);
	}

	private static void tryMyClasspath(String cn, String library, StringBuffer successes, StringBuffer failures) {

		try {
			Class.forName(cn);
			successes.append(found(cn, library));
		}

		catch (Throwable t) {

			failures.append(notFound(cn, library));

		}


	}

	private static void tryAsResource(String cn, String library, StringBuffer successes, StringBuffer failures) {

		try {
			java.io.InputStream in = cn.getClass().getResourceAsStream(cn);
			in.close();
			successes.append(found(cn, library));
		}

		catch (Throwable t) {
			failures.append(notFound(cn, library));

		}

	}

	private static String found(String cn, String library) {
		StringBuffer temp = new StringBuffer(crLf());
		temp.append("   " + library);
		temp.append(crLf());
		temp.append(crLf());
		return temp.toString();
	}
	private static String notFound(String cn, String library) {

		StringBuffer temp = new StringBuffer(crLf());
		temp.append("   " + library);
		temp.append(crLf());
		temp.append("    " + Main.getTextMessage("SIF08.U", cn));
		temp.append(crLf());
		temp.append(crLf());
		return temp.toString();
	}

	private static String crLf() {
		return System.getProperty("line.separator");
	}

	private static String lookForMainArg(String[] args, java.io.PrintWriter localPW)
	{
		int length=args.length;
		String[] legalargs = new String[1];
		legalargs[0] = EMBEDDED;

		int argsfound = 0;
		String whichargument="";

		for (int i = 0; i < length; i++) {

			for (int j=0; j < legalargs.length; j++) {
				if (args[i].toUpperCase(java.util.Locale.ENGLISH).equals(legalargs[j].toUpperCase(java.util.Locale.ENGLISH))) {
					argsfound++;
					whichargument=legalargs[j];
				}
			}
		}
		if (argsfound > 1 || argsfound < 1) {
            localPW.println(Main.getTextMessage("SIF08.A", USAGESTRINGPARTA, USAGESTRINGPARTB));
			return "";
		}
		return whichargument;
	}

	private static boolean argumentsContain(String[] args, String s) {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase(s))
				return true;
		}
		return false;

	}

	private static String argumentMatches(String[] args, String ss) {
	    String userclass = "";
		int length = args.length;
		for (int i = 0; i < length; i++) {
			if (args[i].endsWith(ss)) {
				userclass = args[i].substring(0,args[i].length()-6) ;

			}

		}
		return userclass;
	}

	/*
	** Code related to loading info fromjar files.
	*/

    private static final String infoNames[] = {

                                    "org/apache/derby/info/" +
                                    org.apache.derby.iapi.services.info.ProductGenusNames.DBMS +
                                    ".properties",


                                    "org/apache/derby/info/" +
                                    org.apache.derby.iapi.services.info.ProductGenusNames.TOOLS +
                                    ".properties",

                                    "org/apache/derby/info/" +
                                    org.apache.derby.iapi.services.info.ProductGenusNames.NET +
                                    ".properties"
                                };

    public static ZipInfoProperties[] getAllInfo(String classpath)
    {
        try
        {
			if (classpath != null) {
				String cp [] = parseClasspath(classpath);
				Vector v = new Vector();
				for (int i = 0; i < cp.length; i++)
				{
					ZipInfoProperties zip = checkForInfo(cp[i]);
					if (zip != null)
					{
						v.addElement(zip);
					}
				}
				if (v.size() > 0)
				{
					ZipInfoProperties zips[] = new ZipInfoProperties[v.size()];
					v.copyInto(zips);
					return zips;
				}
			}
            return loadZipFromResource();

        }
        catch (SecurityException se)
        {
            ZipInfoProperties zip[] = new ZipInfoProperties[1];
            zip[0] = new ZipInfoProperties(null);
            zip[0].setLocation (Main.getTextMessage ("SIF03.C"));
            return zip;
        }
    }

    /**
        This method returns exactly one ZipInfoProperty in the array.
        If it is able to load the sysinfo file as a resource, it returns
        the ZipInfoProperty associated with that. Otherwise, the ZipInfoProperty
        will be empty.
     */
    private static ZipInfoProperties [] loadZipFromResource()
    {
		java.util.ArrayList al = new java.util.ArrayList();

        for (int i = 0; i < infoNames.length; i++)
        {
			String resource = "/".concat(infoNames[i]);

            InputStream is = new Main().getClass().getResourceAsStream(resource);
			if (is == null)
				continue;

			ZipInfoProperties ze = new ZipInfoProperties(ProductVersionHolder.getProductVersionHolderFromMyEnv(is));
			ze.setLocation(resource);

			al.add(ze);
        }

        if (al.size() == 0)
        {
            return null;
        }

        ZipInfoProperties[] zip = new ZipInfoProperties[al.size()];

		al.toArray(zip);

        return zip;
    }

    private static String [] parseClasspath(String cp)
    {
        StringTokenizer st = new StringTokenizer(cp, File.pathSeparator);
        int count = st.countTokens();
        if (count == 0)
        {
            return null;
        }

        String vals[] = new String[count];
        for (int i =0; i < count; i++)
        {
            vals[i] = st.nextToken();
        }
        return vals;
    }

    private static ZipInfoProperties checkForInfo(String cpEntry)
    {
        File f = new File(cpEntry);
        if ( ! f.exists())
        {
            return null;
        }

        if (f.isDirectory())
        {
            ZipInfoProperties zip = checkDirectory(cpEntry);
            return zip;
        }

        if (f.isFile())
        {
            ZipInfoProperties zip = checkFile(cpEntry);
            return zip;
        }
        return null;


    }

    private static ZipInfoProperties checkDirectory(String dirname)
    {
        boolean foundOne = false;
        File f = null;
        for (int i = 0; i < infoNames.length; i++)
        {
            String localSysinfo = infoNames[i].replace('/', File.separatorChar);
            f = new File(dirname, localSysinfo);
            if (f.exists())
            {
                foundOne = true;
                break;
            }
        }

        if (!foundOne || (f == null))
        {
            return null;
        }

        try
        {
            InputStream bis = new FileInputStream(f);

            ZipInfoProperties zip = new ZipInfoProperties(ProductVersionHolder.getProductVersionHolderFromMyEnv(bis));
            zip.setLocation(new File(dirname).getCanonicalPath().replace('/', File.separatorChar));
            return zip;
        }
        catch (IOException ioe)
        {
            return null;
        }

    }

    private static ZipInfoProperties checkFile(String filename)
    {
        // try to create a ZipFile from it

	// Check to see if it's a version of db2jcc.jar and if so, report the version number. 
	if (filename.indexOf("db2jcc") >= 0)
	{
	    Class c = null;
	    Method m = null;
	    Object o = null;
	    Integer build = null;
	    Integer major = null;
            Integer minor = null;
	    try 
	    {
                try 
		{
		    c = Class.forName("com.ibm.db2.jcc.DB2Driver");
		    m = c.getMethod("getJCCBuildNumber", null);
		    o = c.newInstance();
		    build = (Integer)m.invoke(o,null);
		} catch (ClassNotFoundException cnfe) {
		    c = Class.forName("com.ibm.db2.jcc.DB2Version");
		    m = c.getMethod("getBuildNumber", null);
		    o = c.newInstance();
		    build = (Integer)m.invoke(o,null);
	        } 
		m = c.getMethod("getMajorVersion", null);
		major = (Integer)m.invoke(o,null);
		m = c.getMethod("getMinorVersion", null);
		minor = (Integer)m.invoke(o,null);

		ProductVersionHolder jccVersion = ProductVersionHolder.getProductVersionHolder(
			"IBM Corp.",
			"DB2 Java Common Client",
			"DRDA:jcc",
			major.intValue(),
			minor.intValue(),
			0,
			0,
			build.toString(),
			Boolean.FALSE);

		ZipInfoProperties zip = new ZipInfoProperties(jccVersion);

        zip.setLocation(new File(filename).getCanonicalPath().replace('/', File.separatorChar));
		return zip;
            } catch (Exception e) { return null; }
	}

        try
        {
            ZipFile zf = new ZipFile(filename);
            // try to get a ZipEntry from the ZipFile

            ZipEntry thisEntry = null;

            for (int i =0; i < infoNames.length; i++)
            {
                thisEntry = zf.getEntry(infoNames[i]);
                if (thisEntry != null)
                {
                    break;
                }
            }

            if (thisEntry == null)
            {
                return null;
            }

            InputStream bis = zf.getInputStream(thisEntry);
            if (bis == null)
            {
                return null;
            }

            ZipInfoProperties zip = new ZipInfoProperties(ProductVersionHolder.getProductVersionHolderFromMyEnv(bis));
            zip.setLocation(new File(filename).getCanonicalPath().replace('/', File.separatorChar));
            return zip;

        }
        catch (IOException ioe)
        {
            //guess not
            return null;
        }

    }

	/*
	** Message handling
	*/
	private static ResourceBundle getBundle() {
		try {
			return ResourceBundle.getBundle("org.apache.derby.loc.sysinfoMessages");
		} catch (MissingResourceException mre) {
		}
		return null;
	}

	public static String getTextMessage(String msgId) {
		return getCompleteMessage(msgId, (Object[]) null);
	}
	public static String getTextMessage(String msgId, Object a1) {

		return getCompleteMessage(msgId, new Object[] {a1});
	}
	public static String getTextMessage(String msgId, Object a1, Object a2) {
		return getCompleteMessage(msgId, new Object[] {a1, a2});
	}
	public static String getTextMessage(String msgId, Object a1, Object a2, Object a3) {
		return getCompleteMessage(msgId, new Object[] {a1, a2, a3});
	}
	public static String getTextMessage(String msgId, Object a1, Object a2, Object a3, Object a4) {
		return getCompleteMessage(msgId, new Object[] {a1, a2, a3, a4});
	}

	/**
	 */
	public static String getCompleteMessage(String msgId, Object[] arguments) {

		// we have a base file (sysinfoMessages.properties) so don't give us a last chance.
		return org.apache.derby.iapi.services.i18n.MessageService.formatMessage(getBundle(), msgId, arguments, false);
	}
} // end of class Main

