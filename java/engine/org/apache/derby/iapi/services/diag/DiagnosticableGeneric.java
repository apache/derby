/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.diag
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
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
