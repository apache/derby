/* 

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.io.FilenameFilter;
import java.io.File;

/**
	Filter to only accept interesting files
	for generating reports.
 */
class GRFileFilter implements FilenameFilter {

	public boolean accept (File dir, String name) {
	    if (name.endsWith(".skip")) return true;
		if (name.endsWith(".pass")) return true;
		if (name.endsWith(".fail")) {
			// special case from rundtest script
			if (name.equals("runall.fail")) return false;
			return true;
		}
		if (name.endsWith(".diff")) {
			// special case from rundtest script
			if (name.equals("runall.diff")) return false;
			if (name.equals("failures.diff")) return false;
			return true;
		}
		File f = new File(dir,name);
		if (f.isDirectory()) return true;
		return false;
	}

	GRFileFilter() {}
	
}
