/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.ij;

/**
 */
public class mtTime
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	public int hours;
	public int minutes;
	public int seconds;

	mtTime(int hours, int minutes, int seconds)
	{ 
		this.hours = hours;
		this.minutes = minutes;
		this.seconds = seconds;
	}

	public String toString()
	{
		return hours+":"+minutes+":"+seconds;
	}
}
