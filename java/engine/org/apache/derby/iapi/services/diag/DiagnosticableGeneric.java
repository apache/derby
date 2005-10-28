/*

   Derby - Class org.apache.derby.iapi.services.diag.DiagnosticableGeneric

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

package org.apache.derby.iapi.services.diag;

import org.apache.derby.iapi.error.StandardException;

import java.util.Properties;

/**

  The Diagnosticable class implements the Diagnostics protocol, and can
  be used as the parent class for all other Diagnosticable objects.

**/

public class DiagnosticableGeneric implements Diagnosticable
{
	/*
	** Fields of Diagnosticable
	*/
    protected Object diag_object = null;

   
    public DiagnosticableGeneric()
    {
    }

	/*
	** Methods of Diagnosticable
	*/
    public void init(Object obj)
    {
        // This is the pointer to the instance of the object to work on.
        this.diag_object = obj;
    }

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
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public String diag()
        throws StandardException
    {
        return(diag_object.toString());
    }

    /**
     * Default implementation of detail diagnostic on the object.
     * <p>
     * This routine should be overriden if there is detail diagnostics to
     * be provided by a real implementation.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void diag_detail(Properties prop)
        throws StandardException
    {
        return;
    }
}
