/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.store.raw.PageTimeStamp;

/**
	A per page version number is one way to implement a page time stamp
*/

public class PageVersion implements PageTimeStamp
{
	private long pageNumber;
	private long pageVersion;

	public PageVersion(long number, long version)
	{
		pageNumber = number;
		pageVersion = version;
	}

	public long getPageVersion()
	{
		return pageVersion;
	}

	public long getPageNumber()
	{
		return pageNumber;
	}

	public void setPageVersion(long pageVersion)
	{
		this.pageVersion = pageVersion;
	}

	public void setPageNumber(long pageNumber)
	{
		this.pageNumber = pageNumber;
	}

}
