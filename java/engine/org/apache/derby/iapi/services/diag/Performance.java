/*

   Derby - Class org.apache.derby.iapi.services.diag.Performance

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

package org.apache.derby.iapi.services.diag;

public abstract class Performance {

	// If you want to do some performance measurement, check out this
	// file and change the value of this to `true', then compile
	// whichever other classes are depending on this.  In general,
	// such a check-out should only be temporary.
	public static final boolean MEASURE = false;
}
