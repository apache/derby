/*

   Derby - Class org.apache.derby.iapi.services.diag.Diagnosticable

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

public interface Diagnosticable
{
	/*
	** Methods of Diagnosticable
	*/
    public void init(Object obj);

    /**
     * Default implementation of diagnostic on the object.
     * <p>
     * This routine returns a string with whatever diagnostic information
     * you would like to provide about this associated object passed in
     * the init() call.
     * <p>
     * This routine should be overriden by a real implementation of the
     * diagnostic information you would like to provide.
     * <p>
     *
	 * @return A string with diagnostic information about the object.
     *
     * @exception StandardException  Standard cloudscape exception policy
     **/
    public String diag() throws StandardException;

    /**
     * Default implementation of detail diagnostic on the object.
     * <p>
     * This interface provides a way for an object to pass back pieces of
     * information as requested by the caller.  The information is passed
     * back and forth through the properties argument.  It is expected that
     * the caller knows what kind of information to ask for, and correctly
     * handles the situation when the diagnostic object can't provide the
     * information.
     * <p>
     * As an example assume an object TABLE exists, and that we have created
     * an object D_TABLE that knows how to return the number of pages in the
     * TABLE object.  The code to get that information out would looks something
     * like the following:
     * <p>
     * print_num_pages(Object table)
     * {
     *     Properties prop = new Properties();
     *     prop.put(Page.DIAG_NUM_PAGES,        "");
     *
     *     DiagnosticUtil.findDiagnostic(table).diag_detail(prop);
     *
     *     System.out.println(
     *        "number of pages = " + prop.getProperty(Page.DIAG_NUM_PAGES));
     * }
     * <p>
     * This routine should be overriden if there is detail diagnostics to
     * be provided by a real implementation.
     * <p>
     *
     * @exception StandardException  Standard cloudscape exception policy
     **/
    public void diag_detail(Properties prop) throws StandardException;
}
