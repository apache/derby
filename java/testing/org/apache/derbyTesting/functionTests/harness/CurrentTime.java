/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.sql.Timestamp;
import java.lang.String;

/**
  Return the current system time as a String
  Used to print a timestamp for suite/test runs
*/
public class CurrentTime
{

	public static String getTime()
	{
        // Get the current time and convert to a String
        long millis = System.currentTimeMillis();
        Timestamp ts = new Timestamp(millis);
        String s = ts.toString();
        s = s.substring(0, s.lastIndexOf("."));
        return s;
	}

	// no instances permitted.
	private CurrentTime() {}
}
