/*

   Derby - Class org.apache.derby.impl.store.raw.data.PageVersion

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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
