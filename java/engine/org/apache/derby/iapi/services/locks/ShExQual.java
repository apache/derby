/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.locks
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.locks;

/**
 * This class is intended to be used a the qualifier class for ShExLockable.
 */
public class ShExQual
{
	private int	lockState;

	private ShExQual(int lockState)
	{
		this.lockState = lockState;
	}

	/*
	** These are intentionally package protected.  They are intended to
	** be used in this class and by ShExLockable, and by no one else.
	*/
	public	static final int SHARED = 0;
	public	static final int EXCLUSIVE = 1;

	/* Shared Lock */
	public static final ShExQual SH = new ShExQual(SHARED);

	/* Exclusive Lock */
	public static final ShExQual EX = new ShExQual(EXCLUSIVE);

	public int getLockState()
	{
		return lockState;
	}

	public String toString()
	{
		if (lockState == SHARED)
			return "S";
		else
			return "X";
	}
}
