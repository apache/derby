/*

   Licensed Materials - Property of IBM
   Cloudscape - Package com.ihost.cs
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.util;

/**
	Provides the ability for an object to
	match a subset of a group of other objects.
	E.g in a cache.
*/
public interface Matchable { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/**
		Return true if the passed in object matches
		this object.
	*/
	public boolean match(Object other);
}
