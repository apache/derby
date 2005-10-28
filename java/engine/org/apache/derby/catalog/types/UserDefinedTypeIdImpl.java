/*

   Derby - Class org.apache.derby.catalog.types.UserDefinedTypeIdImpl

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

package org.apache.derby.catalog.types;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import java.sql.Types;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

public class UserDefinedTypeIdImpl extends BaseTypeIdImpl
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

	protected String className;

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
	public	UserDefinedTypeIdImpl() { super(); }

	/**
	 * Constructor for a UserDefinedTypeIdImpl. The SQLTypeName of a UserDefinedType
	 * is assumed to be its className.
	 *
	 * @param className	The SQL name of the type
	 */

	public UserDefinedTypeIdImpl(String className)
	{
		super(className);
		this.className = className;
		JDBCTypeId = java.sql.Types.JAVA_OBJECT;
	}


	/** Return the java class name for this type */
	public String	getClassName()
	{
		return className;
	}

	/** Does this type id represent a system built-in type? */
	public boolean systemBuiltIn()
	{
		return false;
	}

	/** Does this type id represent a user type? */
	public boolean userType()
	{
		return true;
	}
	// Formatable interface.

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
		super.readExternal( in );
		className = in.readUTF();
		JDBCTypeId = java.sql.Types.JAVA_OBJECT;
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
		super.writeExternal( out );
		out.writeUTF( className );
	}
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.USERDEFINED_TYPE_ID_IMPL_V3; }

	/**
	 * Get the format id for the wrapper type id that corresponds to
	 * this type id.
	 */
	public int wrapperTypeFormatId() { return StoredFormatIds.USERDEFINED_TYPE_ID_V3; }
}
