/*

   Derby - Class org.apache.derby.iapi.services.locks.ShExQual

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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
