/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;

import java.io.ObjectInput;

/**
	Limit and ObjectInput capabilities.

	Combin
 */
public interface ErrorObjectInput extends ObjectInput, ErrorInfo
{
	public String getErrorInfo();

    public Exception getNestedException();

}

