/*

   Derby - Class org.apache.derby.impl.sql.catalog.DDColumnPermissionsDependableFinder

   Copyright 2001, 2006 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 *	Class for implementation of DependableFinder in the core DataDictionary 
 *	for column level privileges
 */

public class DDColumnPermissionsDependableFinder extends DDdependableFinder
{
	////////////////////////////////////////////////////////////////////////
	//
	//  STATE
	//
	////////////////////////////////////////////////////////////////////////

	// Write the column privilege type(select/update/reference)
	private String columnPrivType;

    ////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTORS
    //
    ////////////////////////////////////////////////////////////////////////

	/**
	 * Constructor same as in parent.
	 */
	public  DDColumnPermissionsDependableFinder(int formatId)
	{
		super(formatId);
	}

	/**
	 * Constructor given column privilege type
	 */
	public  DDColumnPermissionsDependableFinder(int formatId, String columnPrivType)
	{
		super(formatId);
		this.columnPrivType = columnPrivType;
	}

    ////////////////////////////////////////////////////////////////////////
    //
    //  DDColumnPermissionsDependable METHODS
    //
    ////////////////////////////////////////////////////////////////////////

	/**
	 * Get the column privilege type of the column permission
	 *
	 * @return		String privilege type(select/update/reference) of the column permission
	 */
	public 	String	getColumnPrivType()
	{
		return columnPrivType;
	}

	/**
	 * Get a dependable object, which is essentially a column permission descriptor 
	 *
	 * @param	dd data dictionary
	 * @param	dependableObjectID dependable object ID (table UUID)
	 * @return	a dependable, a column permission descriptor with columnPrivType
	 */
	protected Dependable getDependable(DataDictionary dd, UUID dependableObjectID)
		throws StandardException
	{
		LanguageConnectionContext lcc = (LanguageConnectionContext)
        ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
		return dd.getColumnPermissions(dependableObjectID, columnPrivType, false,
				lcc.getAuthorizationId());
	}

    //////////////////////////////////////////////////////////////////
    //
    //  FORMATABLE METHODS
    //
    //////////////////////////////////////////////////////////////////

	/**
	 * Read this object from a stream of stored objects.  Just read the
	 * column privilege type, besides what the parent does.
	 *
	 * @param in read this.
	 */
	public void readExternal( ObjectInput in )
			throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		columnPrivType = (String)in.readObject();
	}

	/**
	 * Write this object to a stream of stored objects.  Just write the
	 * column privilege type, besides what the parent does.
	 *
	 * @param out write string here.
	 */
	public void writeExternal( ObjectOutput out )
			throws IOException
	{
		super.writeExternal(out);
		out.writeObject(columnPrivType);
	}
}
