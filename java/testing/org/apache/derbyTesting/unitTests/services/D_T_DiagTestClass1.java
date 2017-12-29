/*

   Derby - Class org.apache.derbyTesting.unitTests.services.D_T_DiagTestClass1

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.unitTests.services;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.diag.DiagnosticableGeneric;

/**

A diagnostic class for the T_DiagTestClass1 class.  This class is used for
unit testing by T_Diagnosticable.

**/

public class D_T_DiagTestClass1 extends DiagnosticableGeneric
{
    /* Constructors for This class: */
    /* Private/Protected methods of This class: */
    /* Public Methods of This class: */

    /* Public Methods of Diagnosticable interface: */

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
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public String diag()
        throws StandardException
    {
        return("D_T_DiagTestClass1: " + ((T_DiagTestClass1) diag_object).state);
    }
}
