/*

   Derby - Class org.apache.derby.catalog.types.DefaultInfoImpl

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

package org.apache.derby.catalog.types;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.catalog.DefaultInfo;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

public class DefaultInfoImpl implements DefaultInfo, Formatable
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

	private DataValueDescriptor	defaultValue;
	private String				defaultText;
	private ProviderInfo[]		providerInfo;

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	DefaultInfoImpl() {}

	/**
	 * Constructor for use with numeric types
	 *
	 * @param defaultText	The text of the default.
	 */
	public DefaultInfoImpl(
		String defaultText,
		DataValueDescriptor defaultValue)
	{
		this.defaultText = defaultText;
		this.defaultValue = defaultValue;
	}

	/**
	 * @see DefaultInfo#getDefaultText
	 */
	public String getDefaultText()
	{
		return defaultText;
	}

	public String	toString()
	{
		return defaultText;
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
		defaultText = (String) in.readObject();
		defaultValue = (DataValueDescriptor) in.readObject();
		int providerInfoLength = in.readInt();
		if (providerInfoLength > 0)
		{
			providerInfo = new ProviderInfo[providerInfoLength];
			for (int index = 0; index < providerInfoLength; index++)
			{
				providerInfo[index] = (ProviderInfo) in.readObject();
			}
		}
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
		out.writeObject( defaultText );
		out.writeObject( defaultValue );
		if (providerInfo != null)
		{
			out.writeInt(providerInfo.length);
			for (int index = 0; index < providerInfo.length; index++)
			{
				out.writeObject(providerInfo[index]);
			}
		}
		else
		{
			out.writeInt(0);
		}
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.DEFAULT_INFO_IMPL_V01_ID; }

	/**
	 * Get the default value.
	 * (NOTE: This returns null if 
	 * the default is not a constant.)
	 *
	 * @return The default value.
	 */
	public DataValueDescriptor getDefaultValue()
	{
		return defaultValue;
	}

	/**
	 * Set the default value.
	 *
	 * @param defaultValue The default value.
	 *
	 * @return Nothing.
	 */
	public void setDefaultValue(DataValueDescriptor defaultValue)
	{
		this.defaultValue = defaultValue;
	}

	/**
	 * Set the ProviderInfo. (Providers that default is dependent on.)
	 *
	 * @param providerInfo	Providers that default is dependent on.
	 *
	 * @return Nothing.
	 */
	public void setProviderInfo(ProviderInfo[] providerInfo)
	{
		this.providerInfo = providerInfo;
	}

	/**
	 * Get the ProviderInfo. (Providers that default is dependent on.)
	 *
	 * @return Providers that default is dependent on.
	 */
	public ProviderInfo[] getProviderInfo()
	{
		return providerInfo;
	}
}
