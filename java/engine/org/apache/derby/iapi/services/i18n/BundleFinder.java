/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.i18n
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.i18n;

import java.util.ResourceBundle;

/**
 */
public interface BundleFinder {

	/**
		Return the bundle to be used. The msgIdf is passed
		in to allow it to be a factor in determing the resource name
		of the messages file.

		@param msgId Message being searched for.
	*/
	ResourceBundle getBundle(String msgId);
}
