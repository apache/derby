/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw;

/**

  The interface of objects which can be associated with a page while it's in cache.

  @see Page#setAuxObject

*/
public interface AuxObject
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/** 
		This method is called by the page manager when it's about to evict a
		page which is holding an aux object, or when a rollback occurred on the
		page.  The aux object should release its resources.  The aux object can
		assume that no one else has access to it via the raw store during this
		method call. After this method returns the raw store throws away any
		reference to this object. 
	*/
	public void auxObjectInvalidated();
}
