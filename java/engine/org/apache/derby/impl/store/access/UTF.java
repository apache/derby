/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.UserType;

/**
  A class that is used to store java.lang.Strings and provide
  ordering capability.

  @see org.apache.derby.iapi.services.io.FormatIdOutputStream
 **/

public class UTF extends UserType
{
    public UTF()
    {
    }

    public UTF(String value)
    {
        super(value);
    }

    /*
     * The following methods implement the Orderable protocol.
     */

    public int compare(DataValueDescriptor other)
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(other instanceof UTF);

        UTF arg = (UTF) other;

		return ((String) getObject()).compareTo((String) arg.getObject());

    }
}
