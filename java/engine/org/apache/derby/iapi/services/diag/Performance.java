/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.diag
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.diag;

public abstract class Performance {

	// If you want to do some performance measurement, check out this
	// file and change the value of this to `true', then compile
	// whichever other classes are depending on this.  In general,
	// such a check-out should only be temporary.
	public static final boolean MEASURE = false;
}
