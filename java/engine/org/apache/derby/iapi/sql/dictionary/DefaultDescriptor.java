/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.DefaultDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.Dependent;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.catalog.UUID;

/**
 * This interface is used to get information from a DefaultDescriptor.
 *
 * @author Jerry
 */

public class DefaultDescriptor 
	extends TupleDescriptor
	implements UniqueTupleDescriptor, Provider, Dependent
{
	/** public interface to this class 
		<ol>
		<li>public void setDefaultUUID(UUID defaultUUID);
		<li>public UUID	getTableUUID();
		</ol>
	*/
   
	// implementation

	int			columnNumber;
	UUID		defaultUUID;
	UUID		tableUUID;

	/**
	 * Constructor for a DefaultDescriptor
	 *
	 * @param dataDictionary    the DD
	 * @param defaultUUID		The UUID of the default
	 * @param tableUUID			The UUID of the table
	 * @param columnNumber		The column number of the column that the default is for
	 */

	public DefaultDescriptor(DataDictionary dataDictionary, UUID defaultUUID, UUID tableUUID, int columnNumber)
	{
		super( dataDictionary );

		this.defaultUUID = defaultUUID;
		this.tableUUID = tableUUID;
		this.columnNumber = columnNumber;
	}

	/**
	 * Get the UUID of the default.
	 *
	 * @return	The UUID of the default.
	 */
	public UUID	getUUID()
	{
		return defaultUUID;
	}

	/**
	 * Set the UUID of the default.
	 *
	 * @param defaultUUID The new UUID for the default.
	 *
	 * @return Nothing.
	 */
	public void setDefaultUUID(UUID defaultUUID)
	{
		this.defaultUUID = defaultUUID;
	}

	/**
	 * Get the UUID of the table.
	 *
	 * @return	The UUID of the table.
	 */
	public UUID	getTableUUID()
	{
		return tableUUID;
	}

	/**
	 * Get the column number of the column.
	 *
	 * @return	The column number of the column.
	 */
	public int	getColumnNumber()
	{
		return columnNumber;
	}

	/**
	 * Convert the DefaultDescriptor to a String.
	 *
	 * @return	A String representation of this DefaultDescriptor
	 */

	public String	toString()
	{
		if (SanityManager.DEBUG)
		{
			/*
			** NOTE: This does not format table, because table.toString()
			** formats columns, leading to infinite recursion.
			*/
			return "defaultUUID: " + defaultUUID + "\n" +
				"tableUUID: " + tableUUID + "\n" +
				"columnNumber: " + columnNumber + "\n";
		}
		else
		{
			return "";
		}
	}

	////////////////////////////////////////////////////////////////////
	//
	// PROVIDER INTERFACE
	//
	////////////////////////////////////////////////////////////////////

	/**		
		@return the stored form of this provider

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder() 
	{
	    return	getDependableFinder(StoredFormatIds.DEFAULT_DESCRIPTOR_FINDER_V01_ID);
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		return "default";
	}

	/**
	 * Get the provider's UUID
	 *
	 * @return 	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return defaultUUID;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return char		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.DEFAULT;
	}

	//////////////////////////////////////////////////////
	//
	// DEPENDENT INTERFACE
	//
	//////////////////////////////////////////////////////
	/**
	 * Check that all of the dependent's dependencies are valid.
	 *
	 * @return true if the dependent is currently valid
	 */
	public synchronized boolean isValid()
	{
		return true;
	}

	/**
	 * Prepare to mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).
	 *
	 * @param action	The action causing the invalidation
	 * @param p		the provider
	 *
	 * @exception StandardException thrown if unable to make it invalid
	 */
	public void prepareToInvalidate(Provider p, int action,
					LanguageConnectionContext lcc) 
		throws StandardException
	{
		DependencyManager dm = getDataDictionary().getDependencyManager();

		switch (action)
		{
			/*
			** Currently, the only thing we are depenedent
			** on is an alias.
			*/
		    default:
				DataDictionary dd = getDataDictionary();
				ColumnDescriptor cd = dd.getColumnDescriptorByDefaultId(defaultUUID);
				TableDescriptor td = dd.getTableDescriptor(cd.getReferencingUUID());

				throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT, 
									dm.getActionString(action), 
									p.getObjectName(),
									MessageService.getTextMessage(
										SQLState.LANG_COLUMN_DEFAULT
									),
									td.getQualifiedName() + "." +
									cd.getColumnName());
		}
	}

	/**
	 * Mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).  Always an error
	 * for a constraint -- should never have gotten here.
	 *
	 * @param	action	The action causing the invalidation
	 *
	 * @exception StandardException thrown if called in sanity mode
	 */
	public void makeInvalid(int action, LanguageConnectionContext lcc) 
		throws StandardException
	{
		/* 
		** We should never get here, we should have barfed on 
		** prepareToInvalidate().
		*/
		if (SanityManager.DEBUG)
		{
			DependencyManager dm;
	
			dm = getDataDictionary().getDependencyManager();

			SanityManager.THROWASSERT("makeInvalid("+
				dm.getActionString(action)+
				") not expected to get called");
		}
	}

	/**
     * Attempt to revalidate the dependent. Meaningless
	 * for defaults.
	 */
	public void makeValid(LanguageConnectionContext lcc) 
	{
	}

}
