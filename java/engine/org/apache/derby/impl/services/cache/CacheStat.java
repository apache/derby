/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.cache
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.cache;

import org.apache.derby.iapi.services.sanity.SanityManager;

class CacheStat { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/*
	** Fields
	*/
	protected int findHit;
	protected int findMiss;
	protected int findFault;
	protected int findCachedHit;
	protected int findCachedMiss;
	protected int create;
	protected int ageOut;
	protected int cleanAll;
	protected int remove;
	protected long initialSize;
	protected long maxSize;
	protected long currentSize;

	protected long[] data;

	public long[] getStats() 
	{
		if (data == null)
			data = new long[14];

		data[0] = findHit + findMiss;
		data[1] = findHit;
		data[2] = findMiss;
		data[3] = findFault;
		data[4] = findCachedHit + findCachedMiss;
		data[5] = findCachedHit;
		data[6] = findCachedMiss;
		data[7] = create;
		data[8] = ageOut;
		data[9] = cleanAll;
		data[10] = remove;
		data[11] = initialSize;
		data[12] = maxSize;
		data[13] = currentSize;

		return data;
	}

	public void reset()
	{
		findHit = 0;
		findMiss = 0;
		findFault = 0;
		findCachedHit = 0;
		findCachedMiss = 0;
		create = 0;
		ageOut = 0;
		cleanAll = 0;
		remove = 0;
	}
}
