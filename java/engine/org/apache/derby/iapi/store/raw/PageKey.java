/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
	A key that identifies a BasePage. Used as the key for the caching mechanism.

	<BR> MT - Immutable :
*/


public class PageKey
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

		return (int) (pageNumber ^ container.hashCode());
	}

	public String toString() {
		return "Page(" + pageNumber + "," + container.toString() + ")";
	}

}
