/*

   Derby - Class org.apache.derby.iapi.store.raw.ContainerLock

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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

	/** Integer representation of the type of the lock. */
	private final int type;
	/** Bit mask with one bit set. The position of the bit tells the type of
	 * the lock. */
	private final int typeBit;
	/** Bit mask which represents the lock types that are compatible with this
	 * lock type. */
	private final int compat;

	/** Number of types of container locks. */
	public static final int C_NUMBER = 5;

	/** Container lock compatibility table. */
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

	private ContainerLock(int type) {
		this.type = type;
		typeBit = (1 << type);
		int bitmask = 0;
		for (int i = 0; i < C_NUMBER; i++) {
			// set a bit in bitmask for each compatible lock type
			if (C_COMPAT[type][i]) {
				bitmask |= (1 << i);
			}
		}
		compat = bitmask;
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

	/**
		Get an integer representation of the type of the lock. This method is guaranteed
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
		to return an integer &gt;= 0 and &lt; C_NUMBER. No correlation between the value
		and one of the static variables (CIS etc.) is guaranteed, except that
		the values returned do not change.
	*/
	public int getType() {
		return type;
	}

	public boolean isCompatible(ContainerLock granted) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2122
		return (granted.typeBit & compat) != 0;
	}

	public String toString() {

		return shortnames[getType()];
	}
}
