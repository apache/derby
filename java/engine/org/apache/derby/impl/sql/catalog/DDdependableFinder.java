/*

   Derby - Class org.apache.derby.impl.sql.catalog.DDdependableFinder

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

package	org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.FileInfoDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.PublicAPI;

import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;

import org.apache.derby.iapi.reference.SQLState;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 *	Class for all DependableFinders in the core DataDictionary
 *
 *
 * @author Rick
 */

public class DDdependableFinder implements	DependableFinder, Formatable
{
	////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	////////////////////////////////////////////////////////////////////////

	private transient DataDictionary			dataDictionary;
	private transient UUIDFactory				uuidFactory;

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
	 * Gets the in-memory object associated with the passed-in object ID.
	 *
	 * @param	dependableObjectID the UUID of the Dependable as a String.
	 * 			Used to locate that Dependable
	 *
	 * @return	the associated Dependable
	 *
	 * @exception java.sql.SQLException		thrown on error
	 */
	public	final Dependable	getDependable(String dependableObjectID) throws java.sql.SQLException
	{
		/*
		** Call the specific implementation of getDependable
		** to do the work
		*/
		return getDependable(recreateUUID(dependableObjectID));
	}

	/**
	  *	Gets the AliasDescriptor associated with the passed-in object ID.
	  *
	  *	@param	the object ID of an Alias. Used to locate its AliasDescriptor
	  *
	  *	@return	the associated AliasDescriptor
	  * @exception java.sql.SQLException		thrown on error
	  */
	public final Dependable	getDependable(UUID dependableObjectID)
		 throws java.sql.SQLException
	{
		try 
		{
			return getDependable(getDataDictionary(),dependableObjectID);
		} 
		catch (StandardException se) 
		{
			throw PublicAPI.wrapStandardException(se);
		}
	}

	/**
	  * @see DependableFinder#getSQLObjectName
	  * @exception java.sql.SQLException		thrown on error
	  */
	public final String	getSQLObjectName(String idString) throws java.sql.SQLException
	{

		try {

			// This should really be getDependable(idString).getObjectName()
			// and then the sub-classes would not have to provide a getSQLObjectName
			// method. Currently getDependable(idString).getObjectName() would
			// not always return the same result - fix in main.

			return getSQLObjectName(getDataDictionary(), recreateUUID(idString));
		} 
		catch (StandardException se) 
		{
			throw PublicAPI.wrapStandardException( se );
		}
	}


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
	  *	Gets the datadictionary for this connection.
	  *
	  *	@return	the data dictionary for this connection
	  *
	  * @exception StandardException		Thrown on failure
	  */
	private	DataDictionary	getDataDictionary()
						throws StandardException
	{
		if ( dataDictionary == null )
	    {
			ContextManager				cm  = ContextService.getFactory().getCurrentContextManager();
			DataDictionaryContext		ddc = (DataDictionaryContext)
			                              (cm.getContext(DataDictionaryContext.CONTEXT_ID));
			dataDictionary = ddc.getDataDictionary();
		}
		return	dataDictionary;
	}

	/**
	 * Get the UUID for the given string
	 *
	 * @param the string
	 *
	 * @return the UUID
	 */
	private UUID recreateUUID(String idString)
	{
		if (uuidFactory == null)
		{
			uuidFactory = Monitor.getMonitor().getUUIDFactory();
		}
		return uuidFactory.recreateUUID(idString);
	}

	/**
		Get the dependable for the given UUID
		@exception StandardException thrown on error
	*/
	protected Dependable getDependable(DataDictionary dd, UUID dependableObjectID)
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
				DefaultDescriptor ddi = new DefaultDescriptor(
												dd, 
												cd.getDefaultUUID(), cd.getReferencingUUID(), 
												cd.getPosition());
				return ddi;

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

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"getDependable() called with unexpeced formatId = " + formatId);
				}
				return null;
		}
	}

	/**
		Get the SQL object name for the given UUID
		@exception StandardException thrown on error
	*/
	protected String getSQLObjectName(DataDictionary dd, UUID dependableObjectID)
		throws StandardException
	{
		switch (formatId)
		{
			case StoredFormatIds.ALIAS_DESCRIPTOR_FINDER_V01_ID:
				return dd.getAliasDescriptor(dependableObjectID).getDescriptorName();

			case StoredFormatIds.CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID:
				return dd.getConglomerateDescriptor(dependableObjectID).getConglomerateName();

			case StoredFormatIds.CONSTRAINT_DESCRIPTOR_FINDER_V01_ID:
				return dd.getConstraintDescriptor(dependableObjectID).getConstraintName();

			case StoredFormatIds.DEFAULT_DESCRIPTOR_FINDER_V01_ID:
				ColumnDescriptor columnDescriptor = dd.getColumnDescriptorByDefaultId( dependableObjectID );
				TableDescriptor tableDescriptor = dd.getTableDescriptor(
										columnDescriptor.getReferencingUUID());

				return	MessageService.getTextMessage(
							SQLState.LANG_COLUMN_DEFAULT,
							tableDescriptor.getQualifiedName() + "." +
							columnDescriptor.getColumnName());

			case StoredFormatIds.FILE_INFO_FINDER_V01_ID:
				return dd.getFileInfoDescriptor(dependableObjectID).getName();

			case StoredFormatIds.SCHEMA_DESCRIPTOR_FINDER_V01_ID:
				return dd.getSchemaDescriptor(dependableObjectID, null).getSchemaName();

			case StoredFormatIds.SPS_DESCRIPTOR_FINDER_V01_ID:
				return dd.getSPSDescriptor(dependableObjectID).getName();

			case StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.COLUMN_DESCRIPTOR_FINDER_V01_ID:
				return getDependable(dd, dependableObjectID).getObjectName();

			case StoredFormatIds.TRIGGER_DESCRIPTOR_FINDER_V01_ID:
				return dd.getTriggerDescriptor(dependableObjectID).getName();

			case StoredFormatIds.VIEW_DESCRIPTOR_FINDER_V01_ID:
				return dd.getTableDescriptor(dependableObjectID).getName();

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"getSQLObjectName() called with unexpeced formatId = " + formatId);
				}
				return null;
		}
	}
}
