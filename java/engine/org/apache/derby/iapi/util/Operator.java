/*

   Licensed Materials - Property of IBM
   Cloudscape - Package com.ihost.cs
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.util;

/**
	Provides an interface for an operator that operates on a range of objects
	E.g in a cache.
*/
public interface Operator {

	/**
		Operate on an input object
	*/
	public void operate(Object other);
}
