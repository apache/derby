/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.sql.SQLException;
import java.io.IOException;

/**
    Wrapper class for SQLExceptions
 */
class SQLExceptionWrapper extends SQLException
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
    private Exception myException;

    SQLExceptionWrapper(Exception e)
    {
        myException = e;
    }

    void handleMe()
        throws IOException, ClassNotFoundException
    {
        if (myException instanceof IOException)
        {
            throw ((IOException) myException);
        }
        else if (myException instanceof ClassNotFoundException)
        {
            throw ((ClassNotFoundException) myException);
        }

        if (SanityManager.DEBUG)
        {
            SanityManager.NOTREACHED();
        }
    }

    void handleMeToo()
        throws IOException
    {
        if (myException instanceof IOException)
        {
            throw ((IOException) myException);
        }

        if (SanityManager.DEBUG)
        {
            SanityManager.NOTREACHED();
        }
    }


}
