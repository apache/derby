/* 

   Derby - Class org.apache.derbyTesting.functionTests.harness.GRFileFilter

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
