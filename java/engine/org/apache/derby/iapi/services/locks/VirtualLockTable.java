/*

   Derby - Class org.apache.derby.iapi.services.locks.VirtualLockTable

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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
	This class acts as a conduit of information between the lock manager and
	the outside world.  Once a virtual lock table is initialized, it contains
	a snap shot of all the locks currently held in the lock manager.  A VTI can
	then be written to query the content of the lock table.
	<P>
	Each lock held by the lock manager is represented by a Hashtable.  The key
	to each Hashtable entry is a lock attribute that is of interest to the
	outside world, such as transaction id, type, mode, etc.  
 */

public interface VirtualLockTable {

	// flags for Lockable.lockAttributes
	public static final int LATCH = 1;
	public static final int TABLE_AND_ROWLOCK = 2;
    public static final int SHEXLOCK = 4;
	public static final int ALL = ~0;	// turn on all bits

	// This is a list of attributes that is known to the Virtual Lock Table.

	// list of attributes to be supplied by a participating Lockable
	public static final String LOCKTYPE		= "TYPE";	// mandatory
	public static final String LOCKNAME		= "LOCKNAME"; // mandatory
		 // either one of conglomId or containerId mandatory
	public static final String CONGLOMID	= "CONGLOMID"; 
	public static final String CONTAINERID	= "CONTAINERID";
	public static final String SEGMENTID	= "SEGMENTID";	 // optional
    public static final String PAGENUM		= "PAGENUM"; // optional
    public static final String RECID		= "RECID"; // optional

	// list of attributes added by the virtual lock table by asking
	// the lock for its compatibility space and count
	public static final String XACTID		= "XID";
    public static final String LOCKCOUNT	= "LOCKCOUNT";

	// list of attributes added by the virtual lock table by asking
	// the lock qualifier
	public static final String LOCKMODE		= "MODE";

	// list of attributes to be supplied the virtual lock table by looking at 
	// the lock table
    public static final String STATE		= "STATE";
	public static final String LOCKOBJ		= "LOCKOBJ";

	// list of attributes filled in by virtual lock table with help from data
	// dictionary 
	public static final String TABLENAME	= "TABLENAME";
	public static final String INDEXNAME	= "INDEXNAME";
	public static final String TABLETYPE	= "TABLETYPE";

}
