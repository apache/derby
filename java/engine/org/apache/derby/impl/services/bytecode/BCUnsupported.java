/*

   Derby - Class org.apache.derby.impl.services.bytecode.BCUnsupported

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package	org.apache.derby.impl.services.bytecode;

import org.apache.derby.iapi.services.sanity.SanityManager;
/**
	Convienence SanityManager methods.
 */
class BCUnsupported {

	static void checkImplementationMatch(boolean matches) {
		if (!matches)
			SanityManager.THROWASSERT("Implementation does not match in ByteCode compiler");
	}

	static RuntimeException hadIOException(Throwable t) {
		return new RuntimeException("had an i/o expcetio");
	}

	static void checkCatchBlock(boolean rightPlace) {
		if (! rightPlace)
        {
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT("Catch block must be generated off of a try block or another catch block.");
        }
	}
}
