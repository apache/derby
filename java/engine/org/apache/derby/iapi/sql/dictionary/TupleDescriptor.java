/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.TupleDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.error.StandardException;

import	org.apache.derby.catalog.DependableFinder;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.reference.SQLState;

// is it OK to do this?
import org.apache.derby.impl.sql.catalog.DDdependableFinder;
import org.apache.derby.impl.sql.catalog.DDColumnDependableFinder;

/**
 * This is the superclass of all Descriptors. Users of DataDictionary should use
 * the specific descriptor.
 *
 * @author Rick
 * @author Manish
 */

public class TupleDescriptor
{
	//////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	//////////////////////////////////////////////////////////////////

	// list types

	public	static	final	int	COLUMN_LIST				= 1;
	public	static	final	int	CONGLOMERATE_LIST		= 2;
	public	static	final	int	TRIGGER_LIST			= 3;
	public	static	final	int	CONSTRAINT_LIST			= 4;

	// generic items
/*
	public	static	final	int	INDEX_PROPERTIES		= 1;
	public	static	final	int	TRIGGER_WHEN_TEXT		= 2;
	public	static	final	int	TRIGGER_ACTION_TEXT		= 3;
	public	static	final	int	TRIGGER_COMP_SCHEMA_ID	= 4;
	public	static	final	int	VIEW_DEPENDENCIES		= 5;
	public	static	final	int	SOURCE_COLUMN_IDS		= 6;
	public	static	final	int	UUID_ID					= 7;
	public	static	final	int	UUID_FLATNAME			= 8;
	public	static	final	int	UUID_TYPE				= 9;
	public	static	final	int	UUID_OTHER_ID			= 10;
	public	static	final	int	EXTERNAL_TYPE			= 11;
	public	static	final	int	PLUGIN_PRIMARY_KEY		= 12;
	public	static	final	int	SOURCE_JAR_FILE			= 13;
*/
	//////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	//////////////////////////////////////////////////////////////////

	private     DataDictionary      dataDictionary;

	//////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTOR
	//
	//////////////////////////////////////////////////////////////////

	public	TupleDescriptor() {}

	public TupleDescriptor(DataDictionary dataDictionary) 
	{
		this.dataDictionary = dataDictionary;
	}

	protected DataDictionary getDataDictionary() throws StandardException
	{
		return dataDictionary;
	}

	protected void setDataDictionary(DataDictionary dd) 
	{
		dataDictionary = dd;
	}

	/**
	 * Is this provider persistent?  A stored dependency will be required
	 * if both the dependent and provider are persistent.
	 *
	 * @return boolean              Whether or not this provider is persistent.
	 */
	public boolean isPersistent()
	{
		return true;
	}


	//////////////////////////////////////////////////////////////////
	//
	//	BEHAVIOR. These are only used by Replication!!
	//
	//////////////////////////////////////////////////////////////////


	public DependableFinder getDependableFinder(int formatId)
	{
		return	new DDdependableFinder(formatId);
	}

	DependableFinder getColumnDependableFinder(int formatId, byte[]
													  columnBitMap)
	{
		return new DDColumnDependableFinder(formatId, columnBitMap);
	}
	
	/** Each descriptor must identify itself with its type; i.e index, check
	 * constraint whatever.
	 */
	public String getDescriptorType()
	{
		if (SanityManager.DEBUG) {SanityManager.NOTREACHED(); }
		return null; 
	}
	/* each descriptor has a name
	 */
	public String getDescriptorName()
	{
		if (SanityManager.DEBUG) {SanityManager.NOTREACHED(); }
		return null; 
	}
}
