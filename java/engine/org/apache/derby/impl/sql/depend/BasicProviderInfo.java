/*

   Derby - Class org.apache.derby.impl.sql.depend.BasicProviderInfo

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

package org.apache.derby.impl.sql.depend;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.io.FormatableHashtable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 *	This is the implementation of ProviderInfo in the DependencyManager.
 */

public class BasicProviderInfo implements ProviderInfo
{
	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	public	UUID						uuid;
	public	DependableFinder			dFinder;
	public	String						providerName;

	// CONSTRUCTORS

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	BasicProviderInfo() {}

	/**
	 *	Make one of these puppies.
	 *
	 *  @param uuid			UUID of Provider.
	 *  @param dFinder		DependableFinder for Provider.
	 *	@param providerName	Name of the Provider.
	 */
	public	BasicProviderInfo(
		               UUID				uuid,
					   DependableFinder	dFinder,
					   String			providerName)
	{
		this.uuid = uuid;
		this.dFinder = dFinder;
		this.providerName = providerName;
	}

	// ProviderInfo methods

	/** @see ProviderInfo#getDependableFinder */
	public DependableFinder getDependableFinder()
	{
		return dFinder;
	}

	/** @see ProviderInfo#getObjectId */
	public UUID getObjectId()
	{
		return uuid;
	}

	/** @see ProviderInfo#getProviderName */
	public String getProviderName()
	{
		return providerName;
	}

	// Formatable methods

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{

		FormatableHashtable fh = (FormatableHashtable)in.readObject();
		uuid = (UUID)fh.get("uuid");
		dFinder = (DependableFinder)fh.get("dFinder");
		providerName = (String) fh.get("providerName");
	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		FormatableHashtable fh = new FormatableHashtable();
		fh.put("uuid", uuid);
		fh.put("dFinder", dFinder);
		fh.put("providerName", providerName);
		out.writeObject(fh);
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.PROVIDER_INFO_V02_ID; }

	/*
	  Object methods.
	  */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String	traceUUID;
			String  traceDFinder;
			String	traceProviderName;

			if (uuid == null)
			{
				traceUUID = "uuid: null ";
			}
			else
			{
				traceUUID = "uuid: "+uuid+" ";
			}

			if (dFinder == null)
			{
				traceDFinder = "dFinder: null ";
			}
			else
			{
				traceDFinder = "dFinder: "+dFinder+" ";
			}

			if (providerName == null)
			{
				traceProviderName = "providerName: null ";
			}
			else
			{
				traceProviderName = "providerName: "+providerName+" ";
			}

			return "ProviderInfo: ("+traceUUID+traceDFinder+traceProviderName+")";
		}
		else
		{
			return "";
		}
	}
}
