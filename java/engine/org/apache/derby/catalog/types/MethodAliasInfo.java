/*

   Derby - Class org.apache.derby.catalog.types.MethodAliasInfo

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

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.catalog.AliasInfo;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Describe a method alias.
 *
 * @see AliasInfo
 */
public class MethodAliasInfo
implements AliasInfo, Formatable
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

	private String methodName;

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 */
    public	MethodAliasInfo() {}

	/**
	 * Create a MethodAliasInfo
	 *
	 * @param methodName	The name of the method for the alias.
	 */
	public MethodAliasInfo(String methodName)
	{
		this.methodName = methodName;
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
		methodName = (String)in.readObject();
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
		out.writeObject( methodName );
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.METHOD_ALIAS_INFO_V01_ID; }

	// 
	// AliasInfo methods
	// 
	/**
	  @see org.apache.derby.catalog.AliasInfo#getMethodName
	  */
	public String getMethodName()
	{
		return methodName;
	}
	/**
	  @see org.apache.derby.catalog.AliasInfo#getTargetClassName
	  */
	public String getTargetClassName()
	{
		return null;
	}
	/**
	  @see org.apache.derby.catalog.AliasInfo#getTargetMethodName
	  */
	public String getTargetMethodName()
	{
		return null;
	}

	/**
	  @see java.lang.Object#toString
	  */
	public String	toString()
	{
		return methodName;
	}
}
