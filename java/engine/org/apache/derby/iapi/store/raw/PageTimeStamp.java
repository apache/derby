/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw;

/**
	The type definition of a time stamp that can be associated with pages that
	supports 'time stamp'.

	What a time stamp contains is up to the page.  It is expected that a time
	stamp implementation will collaborate with the page to implement a value
	equality.
	@see Page#equalTimeStamp
*/

public interface PageTimeStamp
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/** No method definition.  This is a type definition */
}
