/*

   Derby - Class org.apache.derby.iapi.store.raw.PageKey

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

import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
	A key that identifies a BasePage. Used as the key for the caching mechanism.

	<BR> MT - Immutable :
*/


public final class PageKey
{
	private final ContainerKey	container;
	private final long	pageNumber;		// page number

	public PageKey(ContainerKey key, long pageNumber) {
		container = key;
		this.pageNumber = pageNumber;
	}

	public long getPageNumber() {
		return pageNumber;
	}

	public ContainerKey getContainerId() {
		return container;
	}

	/*
	** Methods to read and write
	*/

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		container.writeExternal(out);
		CompressedNumber.writeLong(out, pageNumber);
	}

	public static PageKey read(ObjectInput in) throws IOException
	{
		ContainerKey c = ContainerKey.read(in);
		long pn = CompressedNumber.readLong(in);

		return new PageKey(c, pn);
	}


	/*
	** Methods of object
	*/

	public boolean equals(Object other) {

		if (other instanceof PageKey) {
			PageKey otherKey = (PageKey) other;

			return (pageNumber == otherKey.pageNumber) &&
				   container.equals(otherKey.container);
		}

		return false;
	}


	public int hashCode() {
        int hash = 7;
        hash = 79 * hash + container.hashCode();
        hash = 79 * hash + (int) (pageNumber ^ (pageNumber >>> 32));
        return hash;
	}

	public String toString() {
		//return "Page(" + pageNumber + "," + container.toString() + ")";
		return "Could not read page " + pageNumber + " from segment " + container.getSegmentId() + "of container " + container.getContainerId();
	}

}
