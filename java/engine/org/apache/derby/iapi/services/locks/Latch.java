/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.locks
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.locks;

/**
	A Latch represents a latch held in the lock manager.
*/
public interface Latch {

	/**	
		Get the compatability space the latch is held in.
	*/
	public Object getCompatabilitySpace();

	/**
		Gte the object the latch is held on.
	*/
	public Lockable getLockable();

	/**
		Get the qualifier used when the latch was obtained.
	*/
	public Object getQualifier();

	public int getCount();
}
