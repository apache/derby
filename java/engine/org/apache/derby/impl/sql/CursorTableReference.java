/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.sql.execute.ExecCursorTableReference;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
/**
 * 
 * @author jamie
 */
public class CursorTableReference
	implements ExecCursorTableReference, Formatable
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

	private String		exposedName;
	private String		baseName;
	private String		schemaName;

	/**
	 * Niladic constructor for Formatable
	 */
	public CursorTableReference()
	{
	}

	/**
	 *
	 */
	public CursorTableReference
	(
		String	exposedName,
		String	baseName,
		String	schemaName
	)
	{
		this.exposedName = exposedName;
		this.baseName = baseName;
		this.schemaName = schemaName;
	}

	/**
	 * Return the base name of the table
 	 *
	 * @return the base name
	 */
	public String getBaseName()
	{
		return baseName;
	}

	/**
	 * Return the exposed name of the table.  Exposed
	 * name is another term for correlation name.  If
	 * there is no correlation, this will return the base
	 * name.
 	 *
	 * @return the base name
	 */
	public String getExposedName()
	{
		return exposedName;
	}

	/**
	 * Return the schema for the table.  
	 *
	 * @return the schema name
	 */
	public String getSchemaName()
	{
		return schemaName;
	}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this object out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeObject(baseName);
		out.writeObject(exposedName);
		out.writeObject(schemaName);
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		baseName = (String)in.readObject();
		exposedName = (String)in.readObject();
		schemaName = (String)in.readObject();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int getTypeFormatId()	{ return StoredFormatIds.CURSOR_TABLE_REFERENCE_V01_ID; }

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "CursorTableReference"+
				"\n\texposedName: "+exposedName+
				"\n\tbaseName: "+baseName+
				"\n\tschemaName: "+schemaName;
		}
		else
		{
			return "";
		}
	}
}
