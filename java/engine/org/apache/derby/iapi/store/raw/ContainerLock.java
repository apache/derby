/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw;

/**
	A ContainerLock represents a qualifier that is to be used when
	locking a container through a ContainerHandle.

	<BR>
	MT - Immutable

	@see ContainerHandle
	@see LockingPolicy
*/

public final class ContainerLock {

	private final int type;

	private ContainerLock(int type) {
		this.type = type;
	}

    // Names of locks for virtual lock table print out
	private static String[] shortnames = {"IS", "IX", "S", "U", "X" };

	/** Container Intent Shared lock  */
	public static final ContainerLock CIS = new ContainerLock(0);
	/**	Container Intent Exclusive lock */
 	public static final ContainerLock CIX = new ContainerLock(1);
	/**  Container Shared lock */
	public static final ContainerLock CS  = new ContainerLock(2);
	/** Container Update lock */
	public static final ContainerLock CU  = new ContainerLock(3);
	/** Container Exclusive lock */
	public static final ContainerLock CX  = new ContainerLock(4);

	/** number of types of container locks */
	public static final int C_NUMBER = 5;

	/** Container lock compatability table */
	private static final boolean[][] C_COMPAT = {

	//                          Granted
	// Request \	CIS		CIX		CS		CU		CX        
	//	
	/* CIS	*/  {	true,	true,	true,	false,	false    },
	/* CIX	*/  {	true,	true,	false,	false,	false    },
	/* CS	*/  {	true,	false,	true,	false,	false    },
	/* CU	*/	{	false,	false,	true,	false,	false    },
	/* CX	*/  {	false,	false,	false,	false,	false    }

	};

	/**
		Get an integer representation of the type of the lock. This method is guaranteed
		to return an integer >= 0 and < C_NUMBER. No correlation between the value
		and one of the static variables (CIS etc.) is guaranteed, except that
		the values returned do not change.
	*/
	public int getType() {
		return type;
	}

	public boolean isCompatible(ContainerLock granted) {

		return isCompatible(granted.getType());
	}

	public boolean isCompatible(int granted) {

		return C_COMPAT[getType()][granted];
	}

	public String toString() {

		return shortnames[getType()];
	}
}
