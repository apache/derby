/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor

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

import org.apache.derby.iapi.sql.depend.Provider;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.services.monitor.Monitor;

/**
 * The ConglomerateDescriptor class is used to get information about
 * conglomerates for the purpose of optimization.
 *
 * NOTE: The language module does not have to know much about conglomerates
 * with this architecture. To get the cost of using a conglomerate, all it
 * has to do is pass the ConglomerateDescriptor to the access methods, along
 * with the predicate. What the access methods need from a
 * ConglomerateDescriptor remains to be seen.
 *
 * @version 0.1
 * @author Jeff Lichtman
 */

public class ConglomerateDescriptor extends TupleDescriptor
	implements UniqueTupleDescriptor, Provider
{
	// Implementation
	long	conglomerateNumber;
	String	name;
	String[]	columnNames;
	boolean	indexable;
	boolean	forConstraint;
	IndexRowGenerator	indexRowGenerator;
	UUID	uuid;
	UUID	tableID;
	UUID	schemaID;

	/**
	 * Constructor for a conglomerate descriptor.
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param conglomerateNumber	The number for the conglomerate
	 *				we're interested in
	 * @param name			The name of the conglomerate, if any
	 * @param indexable		TRUE means the conglomerate is indexable,
	 *				FALSE means it isn't
	 * @param indexDescriptor	The descriptor of the index if it's not a
	 *							heap
	 * @param forConstraint		TRUE means the conglomerate is an index backing up
	 *							a constraint, FALSE means it isn't
	 * @param uuid		UUID  for this conglomerate
	 * @param tableID	UUID for the table that this conglomerate belongs to
	 * @param schemaID	UUID for the schema that this conglomerate belongs to
	 */
	ConglomerateDescriptor(DataDictionary dataDictionary,
							   long conglomerateNumber,
							   String name,
							   boolean indexable,
							   IndexRowGenerator indexRowGenerator,
							   boolean forConstraint,
							   UUID uuid,
							   UUID tableID,
							   UUID schemaID)
	{
		super( dataDictionary );

		this.conglomerateNumber = conglomerateNumber;
		this.name = name;
		this.indexable = indexable;
		this.indexRowGenerator = indexRowGenerator;
		this.forConstraint = forConstraint;
		if (uuid == null)
		{
			UUIDFactory uuidFactory = Monitor.getMonitor().getUUIDFactory();
			uuid = uuidFactory.createUUID();
		}
		this.uuid = uuid;
		this.tableID = tableID;
		this.schemaID = schemaID;
	}

	/**
	 * Gets the number for the conglomerate.
	 *
	 * @return	A long identifier for the conglomerate
	 */
	public long	getConglomerateNumber()
	{
		return conglomerateNumber;
	}

	/**
	 * Set the conglomerate number.
	 * This is useful when swapping conglomerates, like for bulkInsert.
	 *
	 * @param conglomerateNumber	The new conglomerate number.
	 *
	 * @return Nothing.
	 */
	public void setConglomerateNumber(long conglomerateNumber)
	{
		this.conglomerateNumber = conglomerateNumber;
	}

	/**
	 * Gets the UUID String for the conglomerate.
	 *
	 * @return	The UUID String for the conglomerate
	 */
	public UUID getUUID()
	{
		return uuid;
	}

	/**
	 * Gets the UUID for the table that the conglomerate belongs to.
	 *
	 * @return	The UUID String for the conglomerate
	 */
	public UUID	getTableID()
	{
		return	tableID;
	}

	/**
	 * Gets the UUID for the schema that the conglomerate belongs to.
	 *
	 * @return	The UUID String for the schema that the conglomerate belongs to
	 */
	public UUID	getSchemaID()
	{
		return schemaID;
	}

	/**
	 * Tells whether the conglomerate can be used as an index.
	 *
	 * @return	TRUE if the conglomerate can be used as an index, FALSE if not
	 */
	public boolean	isIndex()
	{
		return indexable;
	}

	/**
	 * Tells whether the conglomerate is an index backing up a constraint.
	 *
	 * @return	TRUE if the conglomerate is an index backing up a constraint, FALSE if not
	 */
	public boolean	isConstraint()
	{
		return forConstraint;
	}

	/**
	 * Gets the name of the conglomerate.  For heaps, this is null.  For
	 * indexes, it is the index name.
	 *
	 * @return	The name of the conglomerate, null if it's the heap for a table.
	 */
	public String getConglomerateName()
	{
		return name;
	}

	/**
	 * Set the name of the conglomerate.  Used only by rename index.
	 *
	 * @param	newName The new name of the conglomerate.
	 */
	public void	setConglomerateName(String newName)
	{
		name = newName;
	}

	/**
	 * Gets the index row generator for this conglomerate, null if the
	 * conglomerate is not an index.
	 *
	 * @return	The index descriptor for this conglomerate, if any.
	 */
	public IndexRowGenerator getIndexDescriptor()
	{
		return indexRowGenerator;
	}

	/**
	 * Set the column names for this conglomerate descriptor.
	 * This is useful for tracing the optimizer.
	 *
	 * @param columnNames	0-based array of column names.
	 *
	 * @return Nothing.
	 */
	public void setColumnNames(String[] columnNames)
	{
		this.columnNames = columnNames;
	}

	/**
	 * Get the column names for this conglomerate descriptor.
	 * This is useful for tracing the optimizer.
	 *
	 * @return the column names for the conglomerate descriptor.
	 */
	public String[] getColumnNames()
	{
		return columnNames;
	}

	//
	// Provider interface
	//

	/**		
		@return the stored form of this provider
	 */
	public DependableFinder getDependableFinder() 
	{
	    return	getDependableFinder(StoredFormatIds.CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID);
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(name != null,
				"ConglomerateDescriptor only expected to be provider for indexes");
		}
		return name;
	}

	/**
	 * Get the provider's UUID
	 *
	 * @return 	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return uuid;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return char		The provider's type.
	 */
	public String getClassType()
	{
		if (indexable)
		{
			return Dependable.INDEX;
		}
		else
		{
			return Dependable.HEAP;
		}
	}

	/**
	 * Convert the conglomerate descriptor to a String
	 *
	 * @return	The conglomerate descriptor as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String keyString = "";

			if (indexable && columnNames != null )
			{
				int[] keyColumns = indexRowGenerator.baseColumnPositions();

				keyString = ", key columns = {" + columnNames[keyColumns[0] - 1];
				for (int index = 1; index < keyColumns.length; index++)
				{
					keyString = keyString + ", " + columnNames[keyColumns[index] - 1];
				}
				keyString = keyString + "}";
			}

			return "ConglomerateDescriptor: conglomerateNumber = " + conglomerateNumber +
				" name = " + name +
				" uuid = " + uuid +
				" indexable = " + indexable + keyString;
		}
		else
		{
			return "";
		}
	}

	/** @see TupleDescriptor#getDescriptorType */
	public String getDescriptorType()
	{
		if (indexable)
			return "Index";
		else
			return "Table";
	}

	/** @see TupleDescriptor#getDescriptorName */
	public String getDescriptorName() { return name; }
	
}
