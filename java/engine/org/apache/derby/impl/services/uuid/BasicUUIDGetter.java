/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.uuid
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.uuid;

import org.apache.derby.iapi.services.io.FormatableInstanceGetter;

/**

**/

public class BasicUUIDGetter extends FormatableInstanceGetter
{
	public Object getNewInstance() {
		return new BasicUUID();
	}
}

