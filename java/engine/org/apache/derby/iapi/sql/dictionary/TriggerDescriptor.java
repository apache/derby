/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.TriggerDescriptor

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

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.catalog.ReferencedColumns;
import org.apache.derby.catalog.UUID;
import java.sql.Timestamp;

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
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * A trigger.
 * <p>
 * We are dependent on TableDescriptors, SPSDescriptors (for our
 * WHEN clause and our action).  Note that we don't strictly
 * need to be dependent on out SPSes because we could just disallow
 * anyone from dropping an sps of type 'T', but to keep dependencies
 * uniform, we'll do be dependent.
 * <p>
 * We are a provider for DML (PreparedStatements or SPSes)
 *
 * The public methods for this class are:
 *
 * <ol>
 * <li>getUUID
 * <li>getName
 * <li>getSchemaDescriptor
 * <li>	public boolean listensForEvent(int event);
 * <li>	public int getTriggerEventMask();
 * <li>	public Timestamp getCreationTimestamp();
 * <li>	public boolean isBeforeTrigger();
 * <li> public boolean isRowTrigger();
 * <li> public UUID getActionId();
 * <li> public SPSDescriptor getActionSPS();
 * <li>	public UUID getWhenClauseId();
 * <li>	public SPSDescriptor getWhenClauseSPS()
 * <li>	public TableDescriptor getTableDescriptor()
 * <li> public ReferencedColumns getReferencedColumnsDescriptor()
 * <li> public int[] getReferencedCols();
 * <li> public boolean isEnabled();
 * <li> public void setEnabled();
 * <li> public void setDisabled();
 * <li> public boolean needsToFire(int stmtType, int[] modifiedCols)
 * <li> public String getTriggerDefinition();
 * <li> public boolean getReferencingOld();
 * <li> public boolean getReferencingNew();
 * <li> public String getOldReferencingName();
 * <li> public String getNewReferencingName();
 * </ol>
 * @author Jamie
 */
public class TriggerDescriptor extends TupleDescriptor
	implements UniqueSQLObjectDescriptor, Provider, Dependent, Formatable 
{
	// field that we want users to be able to know about
	public static final int SYSTRIGGERS_STATE_FIELD = 8;

	public static final int TRIGGER_EVENT_UPDATE = 1;
	public static final int TRIGGER_EVENT_DELETE = 2;
	public static final int TRIGGER_EVENT_INSERT = 4;

	
	private	UUID				id;
	private String				name;
	private String				oldReferencingName;
	private String				newReferencingName;
	private String				triggerDefinition;
	private SchemaDescriptor	sd;
	private int					eventMask;
	private boolean				isBefore;
	private boolean 			isRow;
	private boolean				referencingOld;
	private boolean				referencingNew;
	private	TableDescriptor		td;
	private	UUID				actionSPSId;
	private SPSDescriptor		actionSPS;
	private	UUID				whenSPSId;
	private SPSDescriptor		whenSPS;
	private	boolean				isEnabled;
	private	int[]				referencedCols;
	private	Timestamp			creationTimestamp;
	private UUID				triggerSchemaId;
	private UUID				triggerTableId;


	/**
	 * Niladic constructor, for formatable
	 */
	public TriggerDescriptor() {}

	/**
	 * Constructor.  Used when creating a trigger from SYS.SYSTRIGGERS
	 *
	 * @param dataDictionary 	the data dictionary
	 * @param sd	the schema descriptor for this trigger
	 * @param id	the trigger id
	 * @param name	the trigger name
	 * @param eventMask	TriggerDescriptor.TRIGGER_EVENT_XXXX
	 * @param isBefore	is this a before (as opposed to after) trigger 
	 * @param isRow		is this a row trigger or statement trigger
	 * @param isEnabled	is this trigger enabled or disabled
	 * @param td		the table upon which this trigger is defined
	 * @param whenSPSId	the sps id for the when clause (may be null)
	 * @param actionSPSId	the spsid for the trigger action (may be null)
	 * @param creationTimestamp	when was this trigger created?
	 * @param referencedCols	what columns does this trigger reference (may be null)
	 * @param triggerDefinition The original user text of the trigger action
	 * @param referencingOld whether or not OLD appears in REFERENCING clause
	 * @param referencingNew whether or not NEW appears in REFERENCING clause
	 * @param oldReferencingName old referencing table name, if any, that appears in REFERCING clause
	 * @param newReferencingName new referencing table name, if any, that appears in REFERCING clause
	 */
	public TriggerDescriptor
	(
		DataDictionary		dataDictionary,
		SchemaDescriptor	sd,
		UUID				id,
		String				name,
		int					eventMask,
		boolean				isBefore,
		boolean				isRow,
		boolean				isEnabled,
		TableDescriptor		td,
		UUID				whenSPSId,
		UUID				actionSPSId,
		Timestamp			creationTimestamp,
		int[]				referencedCols,
		String				triggerDefinition,
		boolean				referencingOld,
		boolean				referencingNew,
		String				oldReferencingName,
		String				newReferencingName
	)
	{
		super(dataDictionary);
		this.id = id;
		this.sd = sd;
		this.name = name;
		this.eventMask = eventMask;
		this.isBefore = isBefore;
		this.isRow = isRow;
		this.td = td;
		this.actionSPSId = actionSPSId; 
		this.whenSPSId = whenSPSId;
		this.isEnabled = isEnabled;
		this.referencedCols = referencedCols;
		this.creationTimestamp = creationTimestamp;
		this.triggerDefinition = triggerDefinition;
		this.referencingOld = referencingOld;
		this.referencingNew = referencingNew;
		this.oldReferencingName = oldReferencingName;
		this.newReferencingName = newReferencingName;
		triggerSchemaId = sd.getUUID();
		triggerTableId = td.getUUID();
	}	
		
		
	/**
	 * Get the trigger UUID
	 *
	 * @return the id
	 */
	public UUID getUUID()
	{
		return id;
	}

	/**
	 * Get the trigger name
	 *
	 * @return	the name
	 */
	public String getName()
	{
		return name;
	}

	public UUID getTableId() {
		return triggerTableId;
	}

	/**
	 * Get the triggers schema descriptor
	 *
	 * @return the schema descriptor
	 *
	 * @exception StandardException on error
	 */
	 public SchemaDescriptor getSchemaDescriptor()
		 throws StandardException
	{
		if (sd == null)
		{
			sd = getDataDictionary().getSchemaDescriptor(triggerSchemaId, null);
		}
		return sd;
	}

	/**
	 * Indicate whether this trigger listens for this
	 * type of event.
	 *
	 * @param event TRIGGER_EVENT_XXXX
	 *
	 * @return true if it listens to the specified event.
	 */ 
	public boolean listensForEvent(int event)
	{
		return (event & eventMask) == event;
	}


	/**
	 * Get the trigger event mask.  Currently, a trigger
	 * may only listen for a single event, though it may
	 * OR multiple events in the future.
	 *
	 * @return the trigger event mask
	 */
	public int getTriggerEventMask()
	{
		return eventMask;
	}

	/**
	 * Get the time that this trigger was created.
	 *
	 * @return the time the trigger was created
	 */
	public Timestamp getCreationTimestamp()
	{
		return creationTimestamp;
	}

	/**
	 * Is this a before trigger
	 *
	 * @return true if it is a before trigger
	 */
	public boolean isBeforeTrigger()
	{
		return isBefore;
	}

	/**
	 * Is this a row trigger
	 *
	 * @return true if it is a before trigger
	 */
	public boolean isRowTrigger()
	{
		return isRow;
	}


	/**
	 * Get the trigger action sps UUID
	 *
	 * @return the uuid of the sps action
	 */
	public UUID getActionId()
	{
		return actionSPSId;
	}

	/**
	 * Get the trigger action sps
	 *
	 * @return the trigger action sps
	 *
	 * @exception StandardException on error
	 */
	public SPSDescriptor getActionSPS(LanguageConnectionContext lcc)
		throws StandardException
	{
		if (actionSPS == null)
		{
			//bug 4821 - do the sysstatement look up in a nested readonly
			//transaction rather than in the user transaction. Because of
			//this, the nested compile transaction which is attempting to
			//compile the trigger will not run into any locking issues with
			//the user transaction for sysstatements.
			lcc.beginNestedTransaction(true);
			actionSPS = getDataDictionary().getSPSDescriptor(actionSPSId);
			lcc.commitNestedTransaction();
		}
		return actionSPS;
	}

	/**
	 * Get the trigger when clause sps UUID
	 *
	 * @return the uuid of the sps action
	 */
	public UUID getWhenClauseId()
	{
		return whenSPSId;
	}

	/**
	 * Get the trigger when clause sps 
	 *
	 * @return the sps of the when clause
	 *
	 * @exception StandardException on error
	 */
	public SPSDescriptor getWhenClauseSPS()
		throws StandardException
	{
		if (whenSPS == null)
		{
			whenSPS = getDataDictionary().getSPSDescriptor(whenSPSId);
		}
		return whenSPS;
	}

	/**
	 * Get the trigger table descriptor
	 *
	 * @return the table descripor upon which this trigger
 	 * is declared
	 *
	 * @exception StandardException on error
	 */
	public TableDescriptor getTableDescriptor()
		throws StandardException
	{
		if (td == null)
		{
			td = getDataDictionary().getTableDescriptor(triggerTableId);
		}
		return td;
	}

	/**
	 * Get the referenced table descriptor for this trigger.
	 *
	 * @return the referenced table descriptor
	 *
	 * @exception StandardException on error
	 */
	// caller converts referencedCols to referencedColsDescriptor...
//  	public ReferencedColumns getReferencedColumnsDescriptor()
//  		throws StandardException
//  	{
//  		return (referencedCols == null) ? 
//  				(ReferencedColumns)null :
//  				new ReferencedColumnsDescriptorImpl(referencedCols);
//  	}

	/**
	 * Get the referenced column array for this trigger, used in "alter table
	 * drop column", we get the handle and change it
	 *
	 * @return the referenced column array
	 */
	public int[] getReferencedCols()
	{
		return referencedCols;
	}

	/**
	 * Is this trigger enabled
	 *
	 * @return true if it is enabled
	 */
	public boolean isEnabled()
	{
		return isEnabled;
	}

	/**
	 * Mark this trigger as enabled
	 *
	 */
	public void setEnabled()
	{
		isEnabled = true;
	}

	/**
	 * Mark this trigger as disabled
	 *
	 */
	public void setDisabled()
	{
		isEnabled = false;
	}

	/**
	 * Does this trigger need to fire on this type of
	 * DML?
	 *
	 * @param dmlType	the type of DML 
	 * (StatementType.INSERT|StatementType.UPDATE|StatementType.DELETE)
	 * @param modifiedCols	the columns modified, or null for all
	 *
	 * @return true/false
	 *
	 * @exception StandardException on error
	 */
	public boolean needsToFire(int stmtType, int[] modifiedCols)
		throws StandardException
	{

		if (SanityManager.DEBUG)
		{
			if (!((stmtType == StatementType.INSERT) ||
								 (stmtType == StatementType.BULK_INSERT_REPLACE) ||
								 (stmtType == StatementType.UPDATE) ||
								 (stmtType == StatementType.DELETE)))
			{
				SanityManager.THROWASSERT("invalid statement type "+stmtType);
			}
		}

		/*
		** If we are disabled, we never fire
		*/
		if (!isEnabled)
		{
			return false;
		}

		if (stmtType == StatementType.INSERT)
		{
 			return (eventMask & TRIGGER_EVENT_INSERT) == eventMask;
		}
		if (stmtType == StatementType.DELETE) 
		{
			return (eventMask & TRIGGER_EVENT_DELETE) == eventMask;
		}

		// this is a temporary restriction, but it may not be lifted
		// anytime soon.
		if (stmtType == StatementType.BULK_INSERT_REPLACE)
		{
			throw StandardException.newException(SQLState.LANG_NO_BULK_INSERT_REPLACE_WITH_TRIGGER, 
												 getTableDescriptor().getQualifiedName(), name);
		}

		// if update, only relevant if columns intersect
		return ((eventMask & TRIGGER_EVENT_UPDATE) == eventMask) &&
				ConstraintDescriptor.doColumnsIntersect(modifiedCols, referencedCols);
	}

	/**
	 * Get the original trigger definition.
	 *
	 * @return The trigger definition.
	 */
	public String getTriggerDefinition()
	{
		return triggerDefinition;
	}

	/**
	 * Get whether or not OLD was replaced
	 * in the REFERENCING clause.
	 *
	 * @return Whether or not OLD was replaced
	 * in the REFERENCING clause.
	 */
	public boolean getReferencingOld()
	{
		return referencingOld;
	}

	/**
	 * Get whether or not NEW was replaced
	 * in the REFERENCING clause.
	 *
	 * @return Whether or not NEW was replaced
	 * in the REFERENCING clause.
	 */
	public boolean getReferencingNew()
	{
		return referencingNew;
	}

	/**
	 * Get the old Referencing name, if any,
	 * from the REFERENCING clause.
	 *
	 * @return The old Referencing name, if any,
	 * from the REFERENCING clause.
	 */
	public String getOldReferencingName()
	{
		return oldReferencingName;
	}

	/**
	 * Get the new Referencing name, if any,
	 * from the REFERENCING clause.
	 *
	 * @return The new Referencing name, if any,
	 * from the REFERENCING clause.
	 */
	public String getNewReferencingName()
	{
		return newReferencingName;
	}

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "TRIGGER: "+name;
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
	 * @return the stored form of this provider
	 *
	 * @see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder() 
	{
	    return getDependableFinder(StoredFormatIds.TRIGGER_DESCRIPTOR_FINDER_V01_ID);
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		return name;
	}

	/**
	 * Get the provider's UUID
	 *
	 * @return 	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return id;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return char		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.TRIGGER;
	}

	//////////////////////////////////////////////////////
	//
	// DEPENDENT INTERFACE
	//
	// Triggers are dependent on the underlying table,
 	// and their spses (for the trigger action and the WHEN
	// clause).
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
	 * @param p			the provider
	 * @param lcc		the language connection context
	 *
	 * @exception StandardException thrown if unable to make it invalid
	 */
	public void prepareToInvalidate
	(
		Provider 					p, 
		int							action, 
		LanguageConnectionContext	lcc
	) throws StandardException
	{
		

		switch (action)
		{
			/*
			** We are only dependent on the underlying
			** table, and our spses.  (we should be
			** dropped before our table is dropped).
			*/
		    case DependencyManager.DROP_TABLE:
		    case DependencyManager.DROP_SPS:
		    case DependencyManager.RENAME:
				DependencyManager dm = getDataDictionary().getDependencyManager();
				throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT, 
									dm.getActionString(action), 
									p.getObjectName(), "TRIGGER", name);

			/*
			** The trigger descriptor depends on the trigger table.
			** This means that we get called whenever anything happens
			** to the trigger table. There are so many cases where this
			** can happen that it doesn't make sense to have an assertion
			** here to check whether the action was expected (it makes
			** the code hard to maintain, and creates a big switch statement).
			*/
			default:
				break;
		}
	}

	/**
	 * Mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).  Always an error
	 * for a trigger -- should never have gotten here.
	 *
	 * @param 	lcc the language connection context
	 * @param	action	The action causing the invalidation
	 *
	 * @exception StandardException thrown if called in sanity mode
	 */
	public void makeInvalid(int action, LanguageConnectionContext lcc) throws StandardException
	{
		// No sanity check for valid action. Trigger descriptors depend on
		// the trigger table, so there is a very large number of actions
		// that we would have to check against. This is hard to maintain,
		// so don't bother.
	}

	/**
     * Attempt to revalidate the dependent. Meaningless
	 * for a trigger.
	 *
	 * @param 	lcc the language connection context
	 */
	public void makeValid(LanguageConnectionContext lcc) 
	{
	}


	//////////////////////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////////////////////

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		 throws IOException, ClassNotFoundException
	{
		id = (UUID)in.readObject();
		name = (String)in.readObject();
		triggerSchemaId = (UUID)in.readObject();
		triggerTableId = (UUID)in.readObject();
		eventMask = in.readInt();
		isBefore = in.readBoolean();
		isRow = in.readBoolean();
		isEnabled = in.readBoolean();
		whenSPSId = (UUID)in.readObject();
		actionSPSId = (UUID)in.readObject();
		int length = in.readInt();
		if (length != 0)
		{
			referencedCols = new int[length];
			for (int i = 0; i < length; i++)
			{
				referencedCols[i] = in.readInt();
			}
		}
		triggerDefinition = (String)in.readObject();
		referencingOld = in.readBoolean();
		referencingNew = in.readBoolean();
		oldReferencingName = (String)in.readObject();
		newReferencingName = (String)in.readObject();
		
	}

	protected DataDictionary getDataDictionary() throws StandardException
	{
		/*
 		  note: we need to do this since when this trigger is read back from
		  disk (when it is associated with a sps), the dataDictionary has not 
 		  been initialized and therefore can give a NullPointerException
 		*/
		DataDictionary dd = super.getDataDictionary();
 		if (dd == null)
 		{
  			LanguageConnectionContext lcc = (LanguageConnectionContext)
				ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
  			dd = lcc.getDataDictionary();
			setDataDictionary(dd);
  		}
		return dd;
 	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(triggerSchemaId != null,
				"triggerSchemaId expected to be non-null");
			SanityManager.ASSERT(triggerTableId != null,
				"triggerTableId expected to be non-null");
		}
		out.writeObject(id);
		out.writeObject(name);
		out.writeObject(triggerSchemaId);
		out.writeObject(triggerTableId);
		out.writeInt(eventMask);
		out.writeBoolean(isBefore);
		out.writeBoolean(isRow);
		out.writeBoolean(isEnabled);
		out.writeObject(whenSPSId);
		out.writeObject(actionSPSId);
		if (referencedCols == null)
		{
			out.writeInt(0);
		}
		else
		{
			out.writeInt(referencedCols.length);
			for (int i = 0; i < referencedCols.length; i++)
			{
				out.writeInt(referencedCols[i]);
			}
		}	
		out.writeObject(triggerDefinition);
		out.writeBoolean(referencingOld);
		out.writeBoolean(referencingNew);
		out.writeObject(oldReferencingName);
		out.writeObject(newReferencingName);
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.TRIGGER_DESCRIPTOR_V01_ID; }

	/** @see TupleDescriptor#getDescriptorType */
	public String getDescriptorType()
	{
		return "Trigger";
	}

	/** @see TupleDescriptor#getDescriptorName */
	public String getDescriptorName() { return name; }
	
}

