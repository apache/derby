/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.locks
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.locks;

import org.apache.derby.iapi.services.diag.Diagnosticable;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;
import org.apache.derby.iapi.error.StandardException;

import java.util.Properties;

/**
**/

public class D_Lock implements Diagnosticable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
    protected Lock lock;

    public D_Lock()
    {
    }

    /* Private/Protected methods of This class: */

	/*
	** Methods of Diagnosticable
	*/
    public void init(Object obj)
    {
        lock = (Lock) obj;
    }

    /**
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public String diag()
        throws StandardException
    {
		StringBuffer sb = new StringBuffer(128);

		sb.append("Lockable=");
		sb.append(DiagnosticUtil.toDiagString(lock.getLockable()));

		sb.append(" Qual=");
		sb.append(DiagnosticUtil.toDiagString(lock.getQualifier()));

		sb.append(" CSpc=");
		sb.append(lock.getCompatabilitySpace());

		sb.append(" count=" + lock.count + " ");

		return sb.toString();
    }

	public void diag_detail(Properties prop) {}
}

