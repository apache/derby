/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.util
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.util;

import java.io.Serializable;

/**
 * This class holds a short.  This class exists for basic testing of
 * user-defined types in JSQL.
 *
 * @author	Jeff Lichtman
 */

public class ShortHolder implements Serializable
{ 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	short		value;

	/**
	 * Constructor for an ShortHolder
	 *
	 * @param value		The value of the short to store in the new object
	 */

	public ShortHolder(short value)
	{
		this.value = value;
	}

	/**
	 * Get the short value out of this object
	 *
	 * @return	The value of the short in this object
	 */

	public short getValue()
	{
		return value;
	}
}
