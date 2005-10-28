/*

   Derby - Class org.apache.derby.impl.store.access.RllRAMAccessManager

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

package org.apache.derby.impl.store.access;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.LockingPolicy;

import org.apache.derby.iapi.services.property.PropertyUtil;

import java.util.Properties;

import org.apache.derby.iapi.reference.Property;

/**

Implements the row level locking accessmanager.

**/

public class RllRAMAccessManager extends RAMAccessManager
{
    private int system_lock_level = TransactionController.MODE_RECORD;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public RllRAMAccessManager()
    {
        super();
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /***************************************************************************
    ** Concrete methods of RAMAccessManager, interfaces that control locking
    ** level of the system.
    ****************************************************************************
    */

    /**
     * Return the locking level of the system.
     * <p>
     * This routine controls the lowest level of locking enabled for all locks
     * for all tables accessed through this accessmanager.  The concrete 
     * implementation may set this value always to table level locking for
     * a client configuration, or it may set it to row level locking for a
     * server configuration.
     * <p>
     * If TransactionController.MODE_RECORD is returned table may either be
     * locked at table or row locking depending on the type of access expected
     * (ie. level 3 will require table locking for heap scans.)
     *
	 * @return TransactionController.MODE_TABLE if only table locking allowed,
     *         else returns TransactionController.MODE_RECORD.
     **/
    protected int getSystemLockLevel()
    {
        return(system_lock_level);
    }

    /**
     * Query property system to get the System lock level.
     * <p>
     * This routine will be called during boot after access has booted far 
     * enough, to allow access to the property conglomerate.  This routine
     * will call the property system and set the value to be returned by
     * getSystemLockLevel().
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void bootLookupSystemLockLevel(
    TransactionController tc)
		throws StandardException
    {
        // The default for this module is TransactionController.MODE_RECORD,
        // only change it if the setting is different.


		if (isReadOnly() || !PropertyUtil.getServiceBoolean(tc, Property.ROW_LOCKING, true)) 
        {
            system_lock_level = TransactionController.MODE_TABLE;
		}
    }
}
