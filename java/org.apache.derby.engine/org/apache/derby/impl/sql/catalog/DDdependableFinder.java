/*

   Derby - Class org.apache.derby.impl.sql.catalog.DDdependableFinder

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

package	org.apache.derby.impl.sql.catalog;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;

/**
 *	Class for most DependableFinders in the core DataDictionary.
 * This class is stored in SYSDEPENDS for the finders for
 * the provider and dependent. It stores no state, its functionality
 * is driven off its format identifier.
 *
 *
 */

public class DDdependableFinder implements	DependableFinder, Formatable
{
	////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	////////////////////////////////////////////////////////////////////////

	private final int formatId;

	////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	////////////////////////////////////////////////////////////////////////

	/**
	  *	Public constructor for Formatable hoo-hah.
	  */
	public	DDdependableFinder(int formatId)
	{
		this.formatId = formatId;
	}

	//////////////////////////////////////////////////////////////////
	//
	//	OBJECT SUPPORT
	//
	//////////////////////////////////////////////////////////////////

	public	String	toString()
	{
		return	getSQLObjectType();
	}

	//////////////////////////////////////////////////////////////////
	//
	//	VACUOUS FORMATABLE INTERFACE. ALL THAT A VACUOUSDEPENDABLEFINDER
	//	NEEDS TO DO IS STAMP ITS FORMAT ID ONTO THE OUTPUT STREAM.
	//
	//////////////////////////////////////////////////////////////////

	/**
	 * Read this object from a stream of stored objects. Nothing to
	 * do. Our persistent representation is just a 2-byte format id.
	 *
	 * @param in read this.
	 */
    public void readExternal( ObjectInput in )
			throws IOException, ClassNotFoundException
	{
	}

	/**
	 * Write this object to a stream of stored objects. Again, nothing
	 * to do. We just stamp the output stream with our Format id.
	 *
	 * @param out write bytes here.
	 */
    public void writeExternal( ObjectOutput out )
			throws IOException
	{
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	final int	getTypeFormatId()	
	{
		return formatId;
	}

	////////////////////////////////////////////////////////////////////////
	//
	//	DDdependable METHODS
	//
	////////////////////////////////////////////////////////////////////////

	/**
	  * @see DependableFinder#getSQLObjectType
	  */
	public	String	getSQLObjectType()
	{
		switch (formatId)
		{
			case StoredFormatIds.ALIAS_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.ALIAS;

			case StoredFormatIds.CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.CONGLOMERATE;

			case StoredFormatIds.CONSTRAINT_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.CONSTRAINT;

			case StoredFormatIds.DEFAULT_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.DEFAULT;

			case StoredFormatIds.FILE_INFO_FINDER_V01_ID:
				return Dependable.FILE;

			case StoredFormatIds.SCHEMA_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.SCHEMA;

			case StoredFormatIds.SPS_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.STORED_PREPARED_STATEMENT;

			case StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.TABLE;

			case StoredFormatIds.COLUMN_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.COLUMNS_IN_TABLE;

			case StoredFormatIds.TRIGGER_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.TRIGGER;

			case StoredFormatIds.VIEW_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.VIEW;

//IC see: https://issues.apache.org/jira/browse/DERBY-1330
			case StoredFormatIds.TABLE_PERMISSION_FINDER_V01_ID:
				return Dependable.TABLE_PERMISSION;
			
//IC see: https://issues.apache.org/jira/browse/DERBY-1330
			case StoredFormatIds.COLUMNS_PERMISSION_FINDER_V01_ID:
				return Dependable.COLUMNS_PERMISSION;

			case StoredFormatIds.ROUTINE_PERMISSION_FINDER_V01_ID:
				return Dependable.ROUTINE_PERMISSION;

//IC see: https://issues.apache.org/jira/browse/DERBY-3666
			case StoredFormatIds.ROLE_GRANT_FINDER_V01_ID:
				return Dependable.ROLE_GRANT;

			case StoredFormatIds.SEQUENCE_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.SEQUENCE;

			case StoredFormatIds.PERM_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.PERM;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"getSQLObjectType() called with unexpeced formatId = " + formatId);
				}
				return null;
		}
	}

	/**
		Get the dependable for the given UUID
		@exception StandardException thrown on error
	*/
	public final Dependable getDependable(DataDictionary dd, UUID dependableObjectID)
		throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-2138
        Dependable dependable = findDependable(dd, dependableObjectID);
        if (dependable == null)
            throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND,
                    getSQLObjectType(), dependableObjectID);
        return dependable;
    }
        
       
    /**
     * Find the dependable for getDependable.
     * Can return a null references, in which case getDependable()
     * will thrown an exception.
     */
    Dependable findDependable(DataDictionary dd, UUID dependableObjectID)
        throws StandardException
    {     
		switch (formatId)
		{
			case StoredFormatIds.ALIAS_DESCRIPTOR_FINDER_V01_ID:
                return dd.getAliasDescriptor(dependableObjectID);

			case StoredFormatIds.CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID:
                return dd.getConglomerateDescriptor(dependableObjectID);

			case StoredFormatIds.CONSTRAINT_DESCRIPTOR_FINDER_V01_ID:
                return dd.getConstraintDescriptor(dependableObjectID);

			case StoredFormatIds.DEFAULT_DESCRIPTOR_FINDER_V01_ID:
				ColumnDescriptor	cd = dd.getColumnDescriptorByDefaultId(dependableObjectID);
                if (cd != null)
                    return new DefaultDescriptor(
												dd, 
												cd.getDefaultUUID(), cd.getReferencingUUID(), 
												cd.getPosition());
                return null;

			case StoredFormatIds.FILE_INFO_FINDER_V01_ID:
                return dd.getFileInfoDescriptor(dependableObjectID);

			case StoredFormatIds.SCHEMA_DESCRIPTOR_FINDER_V01_ID:
                return dd.getSchemaDescriptor(dependableObjectID, null);

			case StoredFormatIds.SPS_DESCRIPTOR_FINDER_V01_ID:
                return dd.getSPSDescriptor(dependableObjectID);

			case StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID:
                return dd.getTableDescriptor(dependableObjectID);

			case StoredFormatIds.TRIGGER_DESCRIPTOR_FINDER_V01_ID:
                return dd.getTriggerDescriptor(dependableObjectID);
 
			case StoredFormatIds.VIEW_DESCRIPTOR_FINDER_V01_ID:
                return dd.getViewDescriptor(dependableObjectID);

//IC see: https://issues.apache.org/jira/browse/DERBY-1330
            case StoredFormatIds.COLUMNS_PERMISSION_FINDER_V01_ID:
                return dd.getColumnPermissions(dependableObjectID);

			case StoredFormatIds.TABLE_PERMISSION_FINDER_V01_ID:
                return dd.getTablePermissions(dependableObjectID);

			case StoredFormatIds.ROUTINE_PERMISSION_FINDER_V01_ID:
                return dd.getRoutinePermissions(dependableObjectID);

		    case StoredFormatIds.ROLE_GRANT_FINDER_V01_ID:
				return dd.getRoleGrantDescriptor(dependableObjectID);
//IC see: https://issues.apache.org/jira/browse/DERBY-3137

			case StoredFormatIds.SEQUENCE_DESCRIPTOR_FINDER_V01_ID:
                return dd.getSequenceDescriptor(dependableObjectID);

			case StoredFormatIds.PERM_DESCRIPTOR_FINDER_V01_ID:
                return dd.getGenericPermissions(dependableObjectID);

		default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"getDependable() called with unexpeced formatId = " + formatId);
				}
//IC see: https://issues.apache.org/jira/browse/DERBY-2138
                return null;
		}
    }
}
