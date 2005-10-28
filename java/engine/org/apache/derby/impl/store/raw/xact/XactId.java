/*

   Derby - Class org.apache.derby.impl.store.raw.xact.XactId

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

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.iapi.services.io.CompressedNumber;


import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
	Use this class for a short hand representation of the transaction.  This
	value is only guarentee to be unique within one continuous operation of the
	raw store, in other words, every reboot may reuse the same value.

	Whereas GlobalXactId is unique for all times across all raw store, a XactId
	is only unique within a particular rawstore and may be reused.

	XactId keeps track of the outstanding transactionId and is responsible
	for dispensing new transactionIds
*/
public class XactId implements TransactionId
{
	/*
	** Fields
	*/
	private long id;			// immutable 

	/*
	** Constructor
	*/
	public XactId(long id) {
		this.id = id;
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public XactId() { super(); }

	/**
		Write this out.
		@exception IOException error writing to log stream
	*/
	public void writeExternal(ObjectOutput out) throws IOException 
	{
		CompressedNumber.writeLong(out, id);
	}

	/**
		Read this in
		@exception IOException error reading from log stream
	*/
	public void readExternal(ObjectInput in) throws IOException
	{
		id = CompressedNumber.readLong(in);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.RAW_STORE_XACT_ID;
	}

	/**
		TransactionId method
	*/
	  
	public int getMaxStoredSize()
	{
		return FormatIdUtil.getFormatIdByteLength(StoredFormatIds.RAW_STORE_XACT_ID) +
			CompressedNumber.MAX_LONG_STORED_SIZE; 
	}

	public boolean equals(Object other) {
		if (other == this)
			return true;

		// assume cast will be successful rather than waste time doing an
		// instanceof first.  Catch the exception if it failed.
		try
		{
			XactId oxid = (XactId)other;
			return (id == oxid.id);
		}
		catch (ClassCastException cce)
		{
			return false;
		}
	}

	public int hashCode()
	{
		return (int)id;
	}

	/**
		Methods specific to this class
	*/

	
	/**
		Return	0 if a == b, 
				+ve number if a > b
				-ve number if a < b
	*/
	public static long compare(TransactionId a, TransactionId b)
	{
		if (a == null || b == null)
		{
			if (a == null)
				return -1;
			else if (b == null)
				return 1;
			else
				return 0;
		}

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(a instanceof XactId);
			SanityManager.ASSERT(b instanceof XactId);
		}
		XactId A = (XactId)a;
		XactId B = (XactId)b;

		return A.id - B.id;
	}

	protected long getId() 
	{
		return id;
	}


	public String toString()
	{
		// needed for virtual lock table
		return Long.toString(id);
	}


}


