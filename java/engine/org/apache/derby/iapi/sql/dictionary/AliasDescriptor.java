/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.AliasDescriptor

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
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import	org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.types.RoutineAliasInfo;

import org.apache.derby.catalog.UUID;

import org.apache.derby.catalog.AliasInfo;
import	org.apache.derby.catalog.DependableFinder;
import	org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.io.StoredFormatIds;

/**
 * This class represents an Alias Descriptor. 
 * The public methods for this class are:
 * 
 * <ol>
 * <li>getUUID</li>
 * <li>getJavaClassName</li>
 * <li>getAliasType</li>
 * <li>getNameSpace</li>
 * <li>getSystemAlias</li>
 * <li>getAliasId</li>
 * </ol>
 *
 */

public final class AliasDescriptor 
	extends TupleDescriptor
	implements UniqueTupleDescriptor, Provider
{
	private final UUID		aliasID;
	private final String		aliasName;
	private final UUID		schemaID;
	private final String		javaClassName;
	private final char		aliasType;
	private final char		nameSpace;
	private final boolean		systemAlias;
	private final AliasInfo	aliasInfo;
	private final String		specificName;

	/**
	 * Constructor for a AliasDescriptor
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param aliasID				The UUID for this alias
	 * @param aliasName				The name of the method alias
	 * @param schemaID				The UUID for this alias's schema
	 * @param javaClassName			The java class name of the alias
	 * @param aliasType				The alias type
	 * @param nameSpace				The alias name space
	 * @param aliasInfo				The AliasInfo for the alias
	 *
	 */

	public	AliasDescriptor( DataDictionary dataDictionary, UUID aliasID,
							  String aliasName, UUID schemaID, String javaClassName,
							  char aliasType, char nameSpace, boolean systemAlias,
							  AliasInfo aliasInfo, String specificName)
	{
		super( dataDictionary );

		this.aliasID = aliasID;
		this.aliasName = aliasName;
		this.schemaID = schemaID;
		this.javaClassName = javaClassName;
		this.aliasType = aliasType;
		this.nameSpace = nameSpace;
		this.systemAlias = systemAlias;
		this.aliasInfo = aliasInfo;
		if (specificName == null)
			specificName = dataDictionary.getSystemSQLName();
		this.specificName = specificName;
	}

	// Interface methods

	/**
	 * Gets the UUID  of the method alias.
	 *
	 * @return	The UUID String of the method alias.
	 */
	public UUID getUUID()
	{
		return aliasID;
	}

	/**
	 * Gets the UUID  of the schema for this method alias.
	 *
	 * @return	The UUID String of the schema id.
	 */
	public UUID getSchemaUUID()
	{
		return schemaID;
	}

	/**
	 * Gets the java class name of the alias.
	 *
	 * @return	The java class name of the alias.
	 */
	public String getJavaClassName()
	{
		return javaClassName;
	}


	/**
	 * Gets the type of the alias.
	 *
	 * @return The type of the alias.
	 */
	public char getAliasType()
	{
		return aliasType;
	}

	/**
	 * Gets the name space of the alias.
	 *
	 * @return The name space of the alias.
	 */
	public char getNameSpace()
	{
		return nameSpace;
	}

	/**
	 * Gets whether or not the alias is a system alias.
	 *
	 * @return Whether or not the alias is a system alias.
	 */
	public boolean getSystemAlias()
	{
		return systemAlias;
	}

	/**
	 * Gests the AliasInfo for the alias.
	 *
	 * @return	The AliasInfo for the alias.
	 */
	public AliasInfo getAliasInfo()
	{
		return aliasInfo;
	}


//  	/**
//  	 * Sets the ID of the method alias
//  	 *
//  	 * @param aliasID	The UUID of the method alias to be set in the descriptor
//  	 *
//  	 * @return	Nothing
//  	 */
//  	public void setAliasID(UUID aliasID)
//  	{
//  		this.aliasID = aliasID;
//  	}

	/**
	 * Convert the AliasDescriptor to a String.
	 *
	 * @return	A String representation of this AliasDescriptor
	 */

	public String	toString()
	{
		if (SanityManager.DEBUG)
		{
			return "aliasID: " + aliasID + "\n" +
				"aliasName: " + aliasName + "\n" +
				"schemaID: " + schemaID + "\n" +
				"javaClassName: " + javaClassName + "\n" +
				"aliasType: " + aliasType + "\n" +
				"nameSpace: " + nameSpace + "\n" +
				"systemAlias: " + systemAlias + "\n" +
				"aliasInfo: " + aliasInfo + "\n";
		}
		else
		{
			return "";
		}
	}

	//	Methods so that we can put AliasDescriptors on hashed lists

	/**
	  *	Determine if two AliasDescriptors are the same.
	  *
	  *	@param	otherObject	other descriptor
	  *
	  *	@return	true if they are the same, false otherwise
	  */

	public boolean equals(Object otherObject)
	{
		if (!(otherObject instanceof AliasDescriptor))
		{	return false; }

		AliasDescriptor other = (AliasDescriptor) otherObject;

		return aliasID.equals( other.getUUID() );
	}

	/**
	  *	Get a hashcode for this AliasDescriptor
	  *
	  *	@return	hashcode
	  */
	public int hashCode()
	{
		return	aliasID.hashCode();
	}

	//
	// Provider interface
	//

	/**		
		@return the stored form of this provider
			representation

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder()
	{
	    return	getDependableFinder(StoredFormatIds.ALIAS_DESCRIPTOR_FINDER_V01_ID);
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		return aliasName;
	}

	/**
	 * Get the provider's UUID
	 *
	 * @return String	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return aliasID;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return String		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.ALIAS;
	}
	
	/** @see TupleDescriptor#getDescriptorType */
	public String getDescriptorType()
	{
		return getAliasType(aliasType);
	}
	
	public static final String getAliasType(char nameSpace)
	{
		switch (nameSpace)
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				return "PROCEDURE";
			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				return "FUNCTION";
			case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
				return "SYNONYM";
		}
		return  null;
	}

	/** @see TupleDescriptor#getDescriptorName */
	public String getDescriptorName()
	{
		return aliasName;
	}


	/**
		Return the specific name for this object.
	*/
	public String getSpecificName()
	{
		return specificName;
	}
    
    /**
     * Functions are persistent unless they are in the SYSFUN schema.
     *
     */
    public boolean isPersistent()
    {
        return !getSchemaUUID().toString().equals(SchemaDescriptor.SYSFUN_SCHEMA_UUID);
    }
   
    /**
     * Report whether this descriptor describes a Table Function.
     *
     */
    public boolean isTableFunction()
    {
        if ( getAliasType() != AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR ) { return false; }

        RoutineAliasInfo    rai = (RoutineAliasInfo) getAliasInfo();

        return rai.getReturnType().isRowMultiSet();
    }
   
    /**
     * Drop the routine or synonym.
     * For a routine its permission descriptors will be dropped as well.
     * For a synonym its TableDescriptor will be dropped as well.
     * @param lcc
     * @throws StandardException
     */
    public void drop(LanguageConnectionContext lcc) throws StandardException {
        
        DataDictionary dd = getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        DependencyManager dm = dd.getDependencyManager();
        
        
        /* Prepare all dependents to invalidate.  (This is their chance
         * to say that they can't be invalidated.  For example, an open
         * cursor referencing a table/view that the user is attempting to
         * drop.) If no one objects, then invalidate any dependent objects.
         * We check for invalidation before we drop the descriptor
         * since the descriptor may be looked up as part of
         * decoding tuples in SYSDEPENDS.
         */
        int invalidationType = 0;
        switch (getAliasType())
        {
        case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
        case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
            invalidationType = DependencyManager.DROP_METHOD_ALIAS;
            break;
            
        case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
            invalidationType = DependencyManager.DROP_SYNONYM;
            break;
        }
        
        dm.invalidateFor(this, invalidationType, lcc);
        
        if (getAliasType() == AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR)
        {
            SchemaDescriptor sd = dd.getSchemaDescriptor(schemaID, tc);
            
            // Drop the entry from SYSTABLES as well.
            DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
            TableDescriptor td = ddg.newTableDescriptor(aliasName, sd,
                    TableDescriptor.SYNONYM_TYPE, TableDescriptor.DEFAULT_LOCK_GRANULARITY);
            dd.dropTableDescriptor(td, sd, tc);
        }
        else
            dd.dropAllRoutinePermDescriptors(getUUID(), tc);
        
        /* Drop the alias */
        dd.dropAliasDescriptor(this, tc);
    }
}
