/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.FileInfoDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

/**
 * A Descriptor for a file that has been stored in the database.
 */
public class  FileInfoDescriptor extends TupleDescriptor 
	implements Provider, UniqueSQLObjectDescriptor
{
	/** A type tho indicate the file is a jar file **/
	public static final int JAR_FILE_TYPE = 0;

	/** external interface to this class:
		<ol>
		<li>public long	getGenerationId();
		</ol>
	*/
	UUID id;
	SchemaDescriptor sd;
	String sqlName;
	long generationId;
	
	/**
	 * Constructor for a FileInfoDescriptor.
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param id        	The id for this file
	 * @param sd			The schema for this file.
	 * @param sqlName		The SQL name of this file.
	 * @param generationId  The generation id for the
	 *                      version of the file this describes.
	 */

	public FileInfoDescriptor(DataDictionary dataDictionary,
								 UUID id,
								 SchemaDescriptor sd,
								 String sqlName,
								 long generationId)
	{
		super( dataDictionary );

		if (SanityManager.DEBUG)
		{
			if (sd.getSchemaName() == null)
			{
				SanityManager.THROWASSERT("new FileInfoDescriptor() schema "+
					"name is null for FileInfo "+sqlName);
			}
		}
		this.id = id;
		this.sd = sd;
		this.sqlName = sqlName;
		this.generationId = generationId;
	}

	public SchemaDescriptor getSchemaDescriptor()
	{
		return sd;
	}

	public String getName()
	{
		return sqlName;
	}

	/**
	 * @see UniqueTupleDescriptor#getUUID
	 */
	public UUID	getUUID()
	{
		return id;
	}

	/**
	 * Gets the generationId for the current version of this file. The
	 * triple (schemaName,SQLName,generationId) are unique for the
	 * life of this database.
	 *
	 * @return	the generationId for this file
	 */
	public long getGenerationId()
	{
		return generationId;
	}

	//
	// Provider interface
	//

	/**		
	  @see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder()
	{
	    return	getDependableFinder(StoredFormatIds.FILE_INFO_FINDER_V01_ID);
	}

	/**
	  @see Dependable#getObjectName
	 */
	public String getObjectName()
	{
		return sqlName;
	}

	/**
	  @see Dependable#getObjectID
	 */
	public UUID getObjectID()
	{
		return id;
	}

	/**
	  @see Dependable#getClassType
	 */
	public String getClassType()
	{
		return Dependable.FILE;
	}

	//
	// class interface
	//

	
	/** @see TupleDescriptor#getDescriptorType */
	public String getDescriptorType() { return "Jar file"; }

	/** @see TupleDescriptor#getDescriptorName */
	public String getDescriptorName() { return sqlName; }



}
