/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.bytecode
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
