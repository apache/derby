/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.JavaVersionHolder

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.util.StringTokenizer;

/**
  To break down the java version into major and minor
  Used by the test harness for special cases
  */
public class JavaVersionHolder
{
 
    private String majorVersion;
    private String minorVersion;
    private int major;
    private int minor;
    
    public JavaVersionHolder(String javaVersion)
        throws java.lang.NumberFormatException
    {
		// check for jdk12 or higher
		int i = javaVersion.indexOf('.');
		int j = javaVersion.indexOf('.', i+1);
		majorVersion = javaVersion.substring(0, i);
		try
	    {
		    Integer imajor = new Integer(majorVersion);
		    major = imajor.intValue();
		    if (j != -1)
		    {
		        minorVersion = javaVersion.substring(i+1, j);
		        Integer iminor = new Integer(minorVersion);
		        minor = iminor.intValue();
		    }
		    else
		    {
		        minorVersion = javaVersion.substring(i+1);
		        Integer iminor = new Integer(minorVersion);
		        minor = iminor.intValue();
		    }
		}
		catch (NumberFormatException nfe)
		{
		    // Cannot parse the version as an Integer
		    // such as on HP: hack for this special case
		    if (javaVersion.startsWith("HP"))
		    {
		        // attempt to get the version
		        StringTokenizer st = new StringTokenizer(javaVersion,".");
		        String tmp = st.nextToken();
		        majorVersion = st.nextToken();
		        if (majorVersion.equals("01"))
		            majorVersion = "1";
		        else if (majorVersion.equals("02"))
		            majorVersion = "2";
		        minorVersion = st.nextToken();
		        if (minorVersion.startsWith("1"))
		            minorVersion = "1";
		        else if (minorVersion.startsWith("2"))
		            minorVersion = "2";
		        //System.out.println("majorVersion: " + majorVersion);
		        //System.out.println("minorVersion: " + minorVersion);
		        try
	            {
		            Integer imajor = new Integer(majorVersion);
		            major = imajor.intValue();
		            Integer iminor = new Integer(minorVersion);
		            minor = iminor.intValue();
		        }
		        catch (NumberFormatException nfe2)
		        {
		            System.out.println("Could not parse version: " + nfe2);
		            // Still couldn't parse the vesion
		            // have to give up
		        }
            }
            else
            {
                System.out.println("NumberFormatException thrown trying to parse the version.");
                System.out.println("The test harness only handles the HP special case.");
            }
                
        }
    }

    public String getMajorVersion()
    {
        return majorVersion;
    }
    
    public String getMinorVersion()
    {
        return minorVersion;
    }
    
    public int getMajorNumber()
    {
        return major;
    }
    
    public int getMinorNumber()
    {
        return minor;
    }
}
