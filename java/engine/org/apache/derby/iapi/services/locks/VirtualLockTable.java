/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.locks
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

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
