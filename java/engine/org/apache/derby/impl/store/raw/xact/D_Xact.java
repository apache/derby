/*

   Derby - Class org.apache.derby.impl.store.raw.xact.D_Xact

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

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.services.diag.Diagnosticable;
import org.apache.derby.iapi.services.diag.DiagnosticableGeneric;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;
import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.iapi.error.StandardException;

/**

The D_Xact class provides diagnostic information about the Xact class.

**/

public class D_Xact extends DiagnosticableGeneric
{

    /**
     * Default implementation of diagnostic on the object.
     * <p>
     * This routine returns a string with whatever diagnostic information
     * you would like to provide about this object.
     * <p>
     * This routine should be overriden by a real implementation of the
     * diagnostic information you would like to provide.
     * <p>
     *
	 * @return A string with diagnostic information about the object.
     * @exception StandardException Standard Cloudscape Error
     *
     **/
    public String diag()
        throws StandardException
    {
        TransactionId id = ((Xact) diag_object).getId();

        if (id instanceof XactId)
            return("Transaction:(" + ((XactId) id).getId() + ")");
        else
            return(id.toString());
    }
}
