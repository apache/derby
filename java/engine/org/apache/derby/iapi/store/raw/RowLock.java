/*

   Derby - Class org.apache.derby.iapi.store.raw.RowLock

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
	A RowLock represents a qualifier that is to be used when
	locking a Row through a RecordHandle.

	<BR>
	MT - Immutable

	@see RecordHandle
	@see LockingPolicy
*/

public final class RowLock {

	/** Integer representation of the type of the lock. */
	private final int   type;
	/** Bit mask with one bit set. The position of the bit tells the type of
	 * the lock. */
	private final int typeBit;
	/** Bit mask which represents the lock types that are compatible with this
	 * lock type. */
	private final int compat;

    // Names of locks for virtual lock table print out
	private static final String[] shortnames =  { "S", "S", "U", "U", "X", "X", "X", "X" };

	/** Number of row locks. */
	public static final int R_NUMBER = 8;

	/** Row lock compatibility table. */
	private static final boolean[][] R_COMPAT = {
        //          Granted
        // Request   RS2     RS3    RU2    RU3    RIP    RI     RX2    RX3
        //
        /* RS2 */    {true,  true,  true,  true,  true,  false, false, false },
        /* RS3 */    {true,  true,  true,  true,  false, false, false, false },
        /* RU2 */    {true,  true,  false, false, true,  false, false, false },
        /* RU3 */    {true,  true,  false, false, false, false, false, false },
        /* RIP */    {true,  false, true,  false, true,  true , true,  false },
        /* RI  */    {false, false, false, false, true,  false, false, false },
        /* RX2 */    {false, false, false, false, true,  false, false, false },
        /* RX3 */    {false, false, false, false, false, false, false, false }
	};

	/* Row Shared lock for repeatable read and below isolation level */
	public static final RowLock RS2  = new RowLock(0);
	/* Row Shared lock for serialized read isolation level */
	public static final RowLock RS3  = new RowLock(1);
	/* Row Update lock for reapeatable read and below isolation level*/
	public static final RowLock RU2  = new RowLock(2);
	/* Row Update lock for serializable isolation level*/
	public static final RowLock RU3  = new RowLock(3);
	/* Row Insert previous key lock */
	public static final RowLock RIP  = new RowLock(4);
	/* Row Insert lock */
	public static final RowLock RI   = new RowLock(5);
	/* Row exclusive write lock for repeatable read and below isolation level */
	public static final RowLock RX2  = new RowLock(6);
	/* Row exclusive write lock for serializable isolation level */
	public static final RowLock RX3  = new RowLock(7);

    /* lock debugging stuff */
    public static final String DIAG_INDEX       = "index";
    public static final String DIAG_XACTID      = "xactid";
    public static final String DIAG_LOCKTYPE    = "locktype";
    public static final String DIAG_LOCKMODE    = "lockmode";
    public static final String DIAG_CONGLOMID   = "conglomId";
    public static final String DIAG_CONTAINERID = "containerId";
    public static final String DIAG_SEGMENTID   = "segmentId";
    public static final String DIAG_PAGENUM     = "pageNum";
    public static final String DIAG_RECID       = "RecId";
    public static final String DIAG_COUNT       = "count";
    public static final String DIAG_GROUP       = "group";
    public static final String DIAG_STATE       = "state";

	private RowLock(int type) {
		this.type = type;
		typeBit = (1 << type);
		int bitmask = 0;
		for (int i = 0; i < R_NUMBER; i++) {
			// set a bit in bitmask for each compatible lock type
			if (R_COMPAT[type][i]) {
				bitmask |= (1 << i);
			}
		}
		compat = bitmask;
	}

	/**
		Get an integer representation of the type of the lock. This method is 
        guaranteed to return an integer &gt;= 0 and &lt; R_NUMBER. No correlation 
        between the value and one of the static variables (CIS etc.) is 
        guaranteed, except that the values returned do not change.
	*/
	public int getType() {
		return type;
	}

	public boolean isCompatible(RowLock granted) {
		return (granted.typeBit & compat) != 0;
	}

	public String toString()
	{
		return shortnames[getType()];
	}
}
