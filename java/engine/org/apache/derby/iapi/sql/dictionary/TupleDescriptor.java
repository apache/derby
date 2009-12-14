/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.TupleDescriptor

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

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
 */

public class TupleDescriptor
{
	//////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	//////////////////////////////////////////////////////////////////


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

	protected DataDictionary getDataDictionary()
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
