/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericConstantAction

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;


import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.catalog.UUID;

/**
 *	Utilities for constant actions
 */
abstract class GenericConstantAction implements ConstantAction
{
	/**
	 * Does this constant action modify the passed in table
	 * uuid?  By modify we mean add or drop things tied to
	 * this table (e.g. index, trigger, constraint).  Things
	 * like views or spses that reference this table don't
	 * count.
	 *
	 * @param tableId the other table id
	 *
	 * @exception   StandardException thrown on failure
	 */
	public boolean modifiesTableId(UUID tableId)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("modifiesTableId() should have been implemented by a super class");
		}
		return false;
	}

	public final void readExternal(java.io.ObjectInput in ) {}
	public final void writeExternal(java.io.ObjectOutput out) {}


    /**
	  *	Reports whether these constants are up-to-date. This returns true
	  *	for homogenous Cloudscape/Cloudsync. For the Plugin, this may
	  *	return false;
	  *
	  *	@return	true if these constants are up-to-date
	  *			false otherwise
	  */
	public	final boolean	upToDate()  throws StandardException
	{ return true; }

/*	protected final void writeFormatableArray(FormatableHashtable fh, String tag, Object[] array)
	{
		fh.put(tag, array == null ? null : new FormatableArrayHolder(array));
	}

	protected final Object[] readFormatableArray(FormatableHashtable fh, String tag, Class type)
	{
		FormatableArrayHolder fah = (FormatableArrayHolder)fh.get(tag);
		return (fah != null) ?
			fah.getArray(type) :
			null;
	}
	*/
}
