/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.catalog.types
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
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
