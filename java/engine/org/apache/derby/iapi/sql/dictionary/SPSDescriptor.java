/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.SPSDescriptor

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

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.sql.StorablePreparedStatement;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.loader.GeneratedClass;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import java.util.Enumeration;
import java.util.Vector;
import java.sql.Timestamp;

/**
 * A SPSDescriptor describes a Stored Prepared Statement.
 * It correlates to a row in SYS.SYSSTATEMENTS.
 *
 * <B>SYNCHRONIZATION</B>: Stored prepared statements
 * may be cached.  Thus they may be shared by multiple
 * threads.  It is very hard for two threads to try
 * to muck with an sps simultaeously because all ddl
 * (including sps recompilation) clears out the sps
 * cache and invalidates whatever statement held a
 * cached sps.  But it is possible for two statements
 * to do a prepare execute statment <x> at the exact
 * same time, so both try to do an sps.prepare() at the 
 * same time during code generation, so we synchronize
 * most everything except getters on immutable objects
 * just to be on the safe side.
 *
 *
 * @author jamie
 */
public class SPSDescriptor extends TupleDescriptor
	implements UniqueSQLObjectDescriptor, Dependent, Provider
{
	/**
	 * Statement types.  
	 * <UL>
	 * <LI> SPS_TYPE_TRIGGER	- trigger (<B>NOT IMPLEMENTED</B>) </LI>
	 * <LI> SPS_TYPE_EXPLAIN	- explain (<B>NOT IMPLEMENTED</B>) </LI>
	 * <LI> SPS_TYPE_REGULAR	- catchall</LI>
	 * </UL>
	 */
	public static final char SPS_TYPE_TRIGGER	= 'T';
	public static final char SPS_TYPE_REGULAR	= 'S';
	public static final char SPS_TYPE_EXPLAIN	= 'X';

	/**
	   interface to this class is:
	   <ol>
	   <li>public void prepare() throws StandardException;
	   <li>public void prepareAndRelease(LanguageConnectionContext lcc) 
	   throws StandardException;
	   <li>public void prepareAndRelease(...);
	   <li>public String	getQualifiedName();
	   <li>public char	getType();
	   <li>public String getTypeAsString();
	   <li>public boolean isValid();
	   <li>public boolean initiallyCompilable();
	   <li>public java.sql.Timestamp getCompileTime();
	   <li>public void setCompileTime();
	   <li>public String getText();
	   <li>public String getUsingText();
	   <li>public void setUsingText(String usingText);
	   <li>public void	setUUID(UUID uuid);
	   <li>public DataTypeDescriptor[] getParams() throws StandardException;
	   <li>public void setParams(DataTypeDescriptor[] params);
	   <li>Object[] getParameterDefaults()	throws StandardException;
	   <li>void setParameterDefaults(Object[] values);
	   <li>public UUID getCompSchemaId();
	   <li>public ExecPreparedStatement getPreparedStatement()
	   throws StandardException;
	   <li>public ExecPreparedStatement getPreparedStatement(boolean recompIfInvalid)
	   throws StandardException;
	   <li>public void revalidate(LanguageConnectionContext lcc)
			throws StandardException;
			</ol>
	*/

	private static final int RECOMPILE = 1;
	private static final int INVALIDATE = 0;

		
	// Class contents
	private SchemaDescriptor		sd;
	private String					name;
	private UUID					uuid;
	private UUID					compSchemaId;
	private char					type;
	private	boolean					valid;
	private	String					text;
	private	String					usingText;
	private	ExecPreparedStatement	preparedStatement;
	private	DataTypeDescriptor		params[];
	private	Timestamp				compileTime;
	private	Object					paramDefaults[];
	private	boolean					initiallyCompilable;
	private	boolean					lookedUpParams;
	
	private UUIDFactory				uuidFactory;


	// constructors
	/**
	 * Constructor for a SPS Descriptor
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param 	name 	the SPS name
	 * @param 	uuid	the UUID
	 * @param 	suuid	the schema UUID
	 * @param 	compSchemaUUID	the schema UUID at compilation time
	 * @param	char	type
	 * @param 	valid	is the sps valid
	 * @param 	text	the text for this statement
	 * @param 	usingText	the text for the USING clause supplied to
	 *					CREATE or ALTER STATEMENT
	 * @param	paramDefaults	default values for the parameters (if
	 *					any) in this statement.  Generated by a USING
	 *					clause, for use by the optimizer.
	 * @param	initiallyCompilable	is the statement initially compilable?
	 *
	 * @exception StandardException on error
	 */
	public SPSDescriptor
				(DataDictionary		dataDictionary,
				String				name,
			   	UUID				uuid,
			   	UUID				suuid,
				UUID				compSchemaUUID,
				char				type,
			   	boolean				valid,
			   	String				text,
				String				usingText,
				Object[]			paramDefaults,
				boolean				initiallyCompilable ) throws StandardException
	{
		this( dataDictionary, name, uuid, suuid, compSchemaUUID, type, valid, text, usingText, null, null, initiallyCompilable );
		this.paramDefaults = paramDefaults;
	}

	/**
	 * Constructor for a SPS Descriptor.  Used when
	 * constructing an SPS descriptor from a row
	 * in SYSSTATEMENTS.
	 *
	 * @param	dataDictionary		The data dictionary that this descriptor lives in
	 * @param 	name 	the SPS name
	 * @param 	uuid	the UUID
	 * @param 	suuid	the schema UUID
	 * @param 	compSchemaUUID	the schema UUID at compilation time
	 * @param	char	type
	 * @param 	valid	is the sps valid
	 * @param 	text	the text for this statement
	 * @param 	usingText	the text for the USING clause supplied to
	 *					CREATE or ALTER STATEMENT
	 * @param 	compileTime	the time this was compiled
	 * @param 	preparedStatement	the PreparedStatement
	 * @param	initiallyCompilable	is the statement initially compilable?
	 *
	 * @exception StandardException on error
	 */
	public SPSDescriptor
				(DataDictionary	dataDictionary,
				String		name,
			   	UUID		uuid,
				UUID		suuid,
				UUID		compSchemaUUID,
				char		type,
			   	boolean		valid,
			   	String		text,
				String 		usingText,
			   	Timestamp	compileTime,
			   	ExecPreparedStatement	preparedStatement,
				boolean		initiallyCompilable ) throws StandardException
	{
		super( dataDictionary );

		this.name = name;
		this.uuid = uuid; 
		this.type = type;
		this.text = text;
		this.usingText = usingText;
		this.valid = valid;
		this.compileTime = compileTime;
		this.sd = dataDictionary.getSchemaDescriptor(suuid, null);
		this.preparedStatement = preparedStatement;
		this.compSchemaId = compSchemaUUID;
		this.initiallyCompilable = initiallyCompilable;
	}

	/**
	 * FOR TRIGGERS ONLY
	 * <p>
	 * Generate the class for this SPS and immediately
	 * release it.  This is useful for cases where we
	 * don't want to immediately execute the statement 
	 * corresponding to this sps (e.g. CREATE STATEMENT).
 	 * <p>
	 * <I>SIDE EFFECTS</I>: will update and SYSDEPENDS 
	 * with the prepared statement dependency info.
 	 * 
	 * @param lcc the language connection context
	 * @param triggerTable the table descriptor to bind against.  Had
	 * 	better be null if this isn't a trigger sps.
	 *
	 * @exception StandardException on error
	 */
	public final synchronized void prepareAndRelease
	(
		LanguageConnectionContext	lcc, 
		TableDescriptor				triggerTable
	) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (triggerTable != null)
			{
				SanityManager.ASSERT(type == SPS_TYPE_TRIGGER, "only expect a table descriptor when we have a trigger");
			}
		}
		
		compileStatement(lcc, triggerTable);
	
		preparedStatement.makeInvalid(DependencyManager.PREPARED_STATEMENT_RELEASE, lcc);
	}

	/**
	 * Generate the class for this SPS and immediately
	 * release it.  This is useful for cases where we
	 * don't want to immediately execute the statement 
	 * corresponding to this sps (e.g. CREATE STATEMENT).
 	 * <p>
	 * <I>SIDE EFFECTS</I>: will update and SYSDEPENDS 
	 * with the prepared statement dependency info.
	 *
	 * @param lcc the language connection context
 	 * 
	 * @exception StandardException on error
	 */
	public final synchronized void prepareAndRelease(LanguageConnectionContext lcc) throws StandardException
	{
		prepareAndRelease(lcc, (TableDescriptor)null);
	}

	private void compileStatement
	(
		LanguageConnectionContext	lcc,
		TableDescriptor				triggerTable
	)
		throws StandardException
	{
		ContextManager cm = lcc.getContextManager();
		DependencyManager dm;
		ProviderInfo[] providerInfo;

		LanguageConnectionFactory	lcf = lcc.getLanguageConnectionFactory();

		DataDictionary dd = getDataDictionary();


		/*
		** If we are a trigger, then we have to go ahead
		** and locate the trigger's table descriptor and
		** push it on the lcc.  This is expensive, but
		** pretty atypical since trigger actions aren't
		** likely to be invalidated too often.  Also, when
		** possible, we already have the triggerTable.
		*/
		if (type == SPS_TYPE_TRIGGER && triggerTable == null)
		{
			String uuidStr = name.substring(49);
			triggerTable = dd.getTableDescriptor(recreateUUID(uuidStr));
			if (SanityManager.DEBUG)
			{
				if (triggerTable == null)
				{
					SanityManager.THROWASSERT("couldn't find trigger table for trigger sps "+name);
				}
			}
		}

		if (triggerTable != null)
		{
			lcc.pushTriggerTable(triggerTable);
		}

		// stored statements always stored as unicode.
		Statement 			stmt = lcf.getStatement(dd.getSchemaDescriptor(compSchemaId, null), text);

		try
		{
			preparedStatement = (ExecPreparedStatement) stmt.prepareStorable(
								lcc,
								preparedStatement, 
								getParameterDefaults(),
								getSchemaDescriptor(),
								type == SPS_TYPE_TRIGGER);
		}
		finally
		{
			if (triggerTable != null)
			{
				lcc.popTriggerTable(triggerTable);
			}
		}

		//If this references a SESSION schema table (temporary or permanent), then throw an exception
		//This is if EXECUTE STATEMENT executing a statement that was created with NOCOMPILE. Because
		//of NOCOMPILE, we could not catch SESSION schema table reference by the statement at
		//CREATE STATEMENT time. And hence need to catch such statements at EXECUTE STATEMENT time
		//when the query is getting compiled.
		if (preparedStatement.referencesSessionSchema())
			throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);
      
		setCompileTime();
		setParams(preparedStatement.getParameterTypes());

		if (!((org.apache.derby.impl.sql.catalog.DataDictionaryImpl) dd).readOnlyUpgrade) {

			/*
			** Indicate that we are going to write the data
			** dictionary.  We have probably already done this
			** but it is ok to call startWriting more than once.
			*/
			dd.startWriting(lcc);

			dm = dd.getDependencyManager();
			/*
			** Clear out all the dependencies that exist
			** before we recreate them so we don't grow
			** SYS.SYSDEPENDS forever.
			*/
			dm.clearDependencies(lcc, this);

			/*
			** Copy over all the dependencies to me
			*/
			dm.copyDependencies(preparedStatement, 	// from
											this, 	// to
											false,	// persistent only
											cm);
		}

		// mark it as valid
		valid = true;
	}

	/**
	 * Gets the name of the sps.
	 *
	 * @return	A String containing the name of the statement.
	 */
	public final String	getName()
	{
		return name;
	}

	/**
	 * Gets the full, qualified name of the statement.
	 *
	 * @return	A String containing the name of the statement.
	 */
	public final String	getQualifiedName()
	{
		return sd.getSchemaName() + "." + name;
	}

	/**
	 * Gets the SchemaDescriptor for this SPS Descriptor.
	 *
	 * @return SchemaDescriptor	The SchemaDescriptor.
	 */
	public final SchemaDescriptor getSchemaDescriptor()
	{
		return sd;
	}

	/**
	 * Gets an identifier telling what type of table this is.
	 * Types match final ints in this interface.  Currently
	 * returns SPS_TYPE_REGULAR or SPS_TYPE_TRIGGER.
	 *
	 * @return	An identifier telling what type of statement
 	 * we are.
	 */
	public final char getType()
	{
		return type;
	}	

	/**
	 * Simple little helper function to convert your type
	 * to a string, which is easier to use.
	 *
	 * @return type as a string
	 */	
	public final String getTypeAsString()
	{
		char[] charArray = new char[1];
		charArray[0] = type;
		return new String(charArray);
	}

	/**
	 * Is the statement initially compilable?  
	 *
	 * @return	false if statement was created with the NOCOMPILE flag
	 *			true otherwise
	 */
	public boolean initiallyCompilable() { return initiallyCompilable; }
	
	/**
	 * Validate the type. <B>NOTE</B>: Only SPS_TYPE_REGULAR
	 * and SPS_TYPE_TRIGGER are currently valid.
	 *
	 * @param type the type
	 *
	 * @return true/false	
	 */
	public final static boolean validType(char type)
	{
		return (type == SPSDescriptor.SPS_TYPE_REGULAR) || 
				(type == SPSDescriptor.SPS_TYPE_TRIGGER);
	}

	/**
	 * The time this prepared statement was compiled
	 *
	 * @return the time this class was last compiled
	 */
	public final synchronized Timestamp getCompileTime()
	{
		return compileTime;
	}

	/**
	 * Set the compile time to now
	 *
	 */
	public final synchronized void setCompileTime()
	{
		compileTime = new Timestamp(System.currentTimeMillis());
	}
	 
	/**
	 * Get the text used to create this statement.
	 * Returns original text in a cleartext string.
	 *
	 * @return The text
	 */
	public final String getText()
	{
		return text;
	}

	/**
	 * Get the text of the USING clause used on CREATE
	 * or ALTER statement.
	 *
	 * @return The text
	 */
	public final synchronized String getUsingText()
	{
		return usingText;
	}

	/**
	 * Set the text of the USING clause. Used by
	 * ALTER statement.
	 *
	 * @param usingText	the new value for the USING text
	 */
	public final synchronized void setUsingText(String usingText)
	{
		this.usingText = usingText;
	}

	/**
	 * Sets the UUID of the SPS.
	 *
	 * @param uuid	The UUID of the SPS to be set in the descriptor
	 */
	public final synchronized void setUUID(UUID uuid)
	{
		this.uuid = uuid;
	}

	/**
	 * Gets the UUID of the SPS.
	 *
	 * @return	the uuid
	 */
	public final UUID	getUUID()
	{
		return uuid;
	}
	
	/**
	 * Get the array of date type descriptors for
	 * this statement.  Currently, we do a lookup
	 * if we don't already have the parameters saved.
	 * When SPSes are cached, the parameters should
	 * be set up when the sps is constructed.
	 *
	 * @return the array of data type descriptors
	 *
	 * @exception StandardException on error
	 */
	public final synchronized DataTypeDescriptor[] getParams()
		throws StandardException
	{
		if (params == null && !lookedUpParams)
		{
			Vector v = new Vector();
			params = getDataDictionary().getSPSParams(this, v);
			paramDefaults = new Object[v.size()];	
			Enumeration iterator = v.elements();
			for (int i = 0; iterator.hasMoreElements(); i++)
			{
				paramDefaults[i] = iterator.nextElement();
			}

			lookedUpParams = true;
		}

		return params;
	}

	/**
	 * Set the list of parameters for this statement
	 *
	 * @param params	the parameter list
	 */
	public final synchronized void setParams(DataTypeDescriptor params[])
	{
		this.params = params;
	}

	/**
	 * Get the default parameter values for this 
	 * statement.  Default parameter values are
	 * supplied by a USING clause on either a
	 * CREATE or ALTER STATEMENT statement.
	 *
	 * @return the default parameter values
 	 *
	 * @exception StandardException on error
	 */
	public final synchronized Object[] getParameterDefaults()
		throws StandardException
	{
		if (paramDefaults == null)
			getParams();

		return paramDefaults;
	}

	/**
	 * Set the parameter defaults for this statement.
	 *
	 * @param params	the parameter defaults
	 */
	public final synchronized void setParameterDefaults(Object[] values)
	{
		this.paramDefaults = values;
	}
	
	/**
	 * Get the constant action for this statement
	 *
	 * @return the constant action
	 */
	//public final synchronized ConstantAction getConstantAction()
	//{
	//	return preparedStatement.getConstantAction();
	//}
	
	/**
	 * Get the preparedStatement for this statement.
	 * If stmt is invalid or hasn't been compiled yet,
	 * it will be recompiled.
	 *
	 * @return the preparedStatement
 	 *
	 * @exception StandardException on error
	 */
	public final ExecPreparedStatement getPreparedStatement()
		throws StandardException
	{
		return getPreparedStatement(true);
	}

	/**
	 * Get the preparedStatement for this statement.
	 * Expects the prepared statement to have already
	 * been added to SYS.SYSSTATEMENTS.
	 * <p>
	 * Side Effects: will update SYS.SYSSTATEMENTS with
	 * the new plan if it needs to be recompiled.
	 *
	 * @param recompIfInvalid if false, never recompile even
	 *	if statement is invalid
	 *
	 * @return the preparedStatement
 	 *
	 * @exception StandardException on error
	 */
	public final synchronized ExecPreparedStatement getPreparedStatement(boolean recompIfInvalid)
		throws StandardException
	{
		//System.out.println("preparedStatement = " + preparedStatement);
		/*
		** Recompile if we are invalid, we don't have
		** a prepared statement, or the statements activation
		** has been cleared and cannot be reconstituted.
		*/
		if (recompIfInvalid &&
			(!valid ||
			 (preparedStatement == null)))
		{
			ContextManager cm = ContextService.getFactory().getCurrentContextManager();

			/*
			** Find the language connection context.  Get
			** it each time in case a connection is dropped.
			*/
			LanguageConnectionContext lcc = (LanguageConnectionContext)
					cm.getContext(LanguageConnectionContext.CONTEXT_ID);
			prepareAndRelease(lcc);


			if (!((org.apache.derby.impl.sql.catalog.DataDictionaryImpl) (lcc.getDataDictionary())).readOnlyUpgrade) {

				//bug 4821 - First try compiling on a nested transaction so we can release
				//the locks after the compilation. But if we get lock time out on the
				//nested transaction, then go ahead and do the compilation on the user
				//transaction. When doing the compilation on user transaction, the locks
				//acquired for recompilation will be released at the end of the user transaction.
				TransactionController nestedTC;
				try
				{
					nestedTC = lcc.getTransactionCompile().startNestedUserTransaction(false);
				}
				catch (StandardException se)
				{
					// If I cannot start a Nested User Transaction use the parent
					// transaction to do all the work.
					nestedTC = null;
				}

				try
				{
					updateSYSSTATEMENTS(lcc, RECOMPILE, nestedTC);
				}
				catch (StandardException se)
				{
					if (se.getMessageId().equals(SQLState.LOCK_TIMEOUT))
					{
						if (nestedTC != null)
						{
						nestedTC.commit();
						nestedTC.destroy();
						nestedTC = null;
						}
						// if we couldn't do this with a nested xaction, retry with
						// parent-- we need to wait this time!
						updateSYSSTATEMENTS(lcc, RECOMPILE, null);
					}
					else throw se;
				}
				finally
				{
					// no matter what, commit the nested transaction; if something
					// bad happened in the child xaction lets not abort the parent
					// here.
					if (nestedTC != null)
					{
						nestedTC.commit();
						nestedTC.destroy();
					}
				} 
			}
		}

		return preparedStatement;
	}

	/**
	 * Get the compilation type schema id when this view
	 * was first bound.
	 *
	 * @return the schema UUID
	 */
	public final UUID getCompSchemaId()
	{
		return compSchemaId;
	}

	/**
	 * Prints the contents of the TableDescriptor
	 *
	 * @return The contents as a String
	 */
	public final String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "SPSDescriptor:\n"+
				"\tname: "+sd.getSchemaName()+"."+name+"\n"+
				"\tuuid: "+uuid+"\n"+
				"\ttext: "+text+"\n"+
				"\tvalid: "+((valid) ? "TRUE" : "FALSE")+"\n" +
				"\tpreparedStatement: "+preparedStatement+"\n";
		}
		else
		{
			return "";
		}
	}

	//////////////////////////////////////////////////////
	//
	// PROVIDER INTERFACE
	//
	//////////////////////////////////////////////////////

	/**		
	 * Return the stored form of this provider
	 *
	 * @see Dependable#getDependableFinder
	 */
	public final DependableFinder getDependableFinder()
	{
	    return	getDependableFinder(StoredFormatIds.SPS_DESCRIPTOR_FINDER_V01_ID);
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public final String getObjectName()
	{
		return name;
	}

	/**
	 * Get the provider's UUID 
	 *
	 * @return String	The provider's UUID
	 */
	public final UUID getObjectID()
	{
		return uuid;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return String The provider's type.
	 */
	public final String getClassType()
	{
		return Dependable.STORED_PREPARED_STATEMENT;
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
	public final synchronized boolean isValid()
	{
		return valid;
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
	public final synchronized void prepareToInvalidate(
									Provider p, int action,
									LanguageConnectionContext lcc) 
		throws StandardException
	{
		switch (action)
		{
			/*
			** Things that don't affect us
			*/
		    case DependencyManager.CREATE_VIEW:
	
			/*
			** Things that force a recompile, but are
			** allowed.
			*/
			case DependencyManager.CREATE_INDEX:
			case DependencyManager.CREATE_CONSTRAINT:
			case DependencyManager.DROP_CONSTRAINT:
			case DependencyManager.DROP_INDEX:
			case DependencyManager.DROP_TABLE:
			case DependencyManager.DROP_VIEW: 
			case DependencyManager.DROP_METHOD_ALIAS:
			case DependencyManager.ALTER_TABLE:
			case DependencyManager.RENAME:
			case DependencyManager.RENAME_INDEX:
			case DependencyManager.PREPARED_STATEMENT_RELEASE:
			case DependencyManager.USER_RECOMPILE_REQUEST:
			case DependencyManager.CHANGED_CURSOR:
			case DependencyManager.BULK_INSERT:
			case DependencyManager.COMPRESS_TABLE:
			case DependencyManager.SET_CONSTRAINTS_ENABLE:
			case DependencyManager.SET_CONSTRAINTS_DISABLE:
			case DependencyManager.SET_TRIGGERS_ENABLE:
			case DependencyManager.SET_TRIGGERS_DISABLE:
			case DependencyManager.ROLLBACK:
			case DependencyManager.INTERNAL_RECOMPILE_REQUEST:
			case DependencyManager.CREATE_TRIGGER:
			case DependencyManager.DROP_TRIGGER:
			case DependencyManager.DROP_TABLE_CASCADE:
			case DependencyManager.DROP_VIEW_CASCADE:
			case DependencyManager.DROP_COLUMN:
			case DependencyManager.DROP_COLUMN_CASCADE:
		    case DependencyManager.UPDATE_STATISTICS:
		    case DependencyManager.DROP_STATISTICS:
    		case DependencyManager.TRUNCATE_TABLE:
				break;

			/*
			** The rest are errors
			*/
		    default:

				DependencyManager dm;

				dm = getDataDictionary().getDependencyManager();
				throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_S_P_S, 
					dm.getActionString(action), 
					p.getObjectName(), name);

		}
	}

	/**
	 * Mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).
	 *
	 * @param	action	The action causing the invalidation
	 *
	 * @exception StandardException thrown if unable to make it invalid
	 */
	public final synchronized void makeInvalid(int action,
											   LanguageConnectionContext lcc) 
		throws StandardException
	{
		DependencyManager dm;

		dm = getDataDictionary().getDependencyManager();

		switch (action)
		{
			/*
			** Some things that don't affect stored prepared
	 		** statements.
			*/
			case DependencyManager.PREPARED_STATEMENT_RELEASE:
		    case DependencyManager.CREATE_VIEW:
				break;

			/*
		 	** Things that can invalidate a stored
			** prepared statement.
			*/
			case DependencyManager.CREATE_INDEX:
			case DependencyManager.CREATE_CONSTRAINT:
			case DependencyManager.DROP_CONSTRAINT:
			case DependencyManager.DROP_TABLE:
			case DependencyManager.DROP_INDEX:
			case DependencyManager.DROP_VIEW: 
			case DependencyManager.DROP_METHOD_ALIAS:
			case DependencyManager.ALTER_TABLE:
			case DependencyManager.RENAME:
			case DependencyManager.RENAME_INDEX:
			case DependencyManager.USER_RECOMPILE_REQUEST:
			case DependencyManager.CHANGED_CURSOR:
			case DependencyManager.BULK_INSERT:
			case DependencyManager.COMPRESS_TABLE:
			case DependencyManager.SET_CONSTRAINTS_ENABLE:
			case DependencyManager.SET_CONSTRAINTS_DISABLE:
			case DependencyManager.SET_TRIGGERS_ENABLE:
			case DependencyManager.SET_TRIGGERS_DISABLE:
			case DependencyManager.ROLLBACK:
			case DependencyManager.INTERNAL_RECOMPILE_REQUEST:
			case DependencyManager.CREATE_TRIGGER:
			case DependencyManager.DROP_TRIGGER:
			case DependencyManager.DROP_TABLE_CASCADE:
			case DependencyManager.DROP_VIEW_CASCADE:
			case DependencyManager.DROP_COLUMN:
			case DependencyManager.DROP_COLUMN_CASCADE:
		    case DependencyManager.UPDATE_STATISTICS:
		    case DependencyManager.DROP_STATISTICS:
			case DependencyManager.TRUNCATE_TABLE:
				/*
				** If we are already invalid, don't write ourselves
				** out.  Just to be safe, we'll send out an invalidate
				** to our dependents either way.
				*/
				if (valid == true)
				{
					valid = false;
					updateSYSSTATEMENTS(lcc, INVALIDATE, null);
				}
				dm.invalidateFor(this, dm.USER_RECOMPILE_REQUEST, lcc);
				break;
			case DependencyManager.DROP_SPS:
				//System.out.println("SPSD " + preparedStatement);
				dm.clearDependencies(lcc, this);
				break;
	
		    default:

				/* 
				** We should never get here, since we can't have dangling references 
				*/
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("makeInvalid("+
						dm.getActionString(action)+
						") not expected to get called; should have failed in "+
						"prepareToInvalidate()");
				}
				break;

		}

	}

	/**
     * Attempt to revalidate the dependent. For prepared statements,
	 * this could go through its dependencies and check that they
	 * are up to date; if not, it would recompile the statement.
	 * Any failure during this attempt should throw
	 * StandardException.unableToRevalidate().
	 *
	 * @exception StandardException thrown if unable to make it valid
	 */
	public final synchronized void makeValid(LanguageConnectionContext lcc) 
		throws StandardException
	{
		if (valid)
		{
			return;
		}
		prepareAndRelease(lcc);

		updateSYSSTATEMENTS(lcc, RECOMPILE, null);
		
	}

	/**
	 * Invalidate and revalidate.  The functional equivalent
	 * of calling makeInvalid() and makeValid(), except it
	 * is optimized.
	 *
	 * @exception StandardException on error
	 */
	public final synchronized void revalidate(LanguageConnectionContext lcc)
		throws StandardException
	{
		/*
		** Mark it as invalid first to ensure that
		** we don't write SYSSTATEMENTS 2x.
		*/
		valid = false;
		makeInvalid(DependencyManager.USER_RECOMPILE_REQUEST, lcc);
		prepareAndRelease(lcc);
		updateSYSSTATEMENTS(lcc, RECOMPILE, null);
	}

	/**
	 * Load the underlying generatd class.  This is not expected
	 * to be used outside of the datadictionary package.  It
	 * is used for optimizing class loading for sps
	 * cacheing.
	 *
	 * @exception StandardException on error
	 */
	public void loadGeneratedClass() throws StandardException
	{
		/*
		** On upgrade, we null out the statement body,
		** so handle that here.
		*/
		if (preparedStatement != null)
		{
			((StorablePreparedStatement)preparedStatement).loadGeneratedClass();
		}
	}

	/*
	** Update SYSSTATEMENTS with the changed the descriptor.  
	** Always done in the user XACT.
	** <p>
	** Ideally, the changes to SYSSTATEMENTS would be made 
	** in a separate xact as the current user xact, but this
	** is painful (you'ld need to get a new ContextManager
	** and then push all of the usual langauge contexts
	** onto it and THEN call AccessManager.getTransaction()),
	** and it wont work, because the xact is in a different
	** compatibility space and will self deadlock (e.g.
	** in the process of call DependencyManager.makeInvalid() 
	** we first did a DDdependableFinder.getDependable()
	** which called DataDictionaryImpl.getSPSDescriptor()
	** so we hold a lock on SYS.SYSSTATEMENTS by the
	** time we get a 2nd xact and try to drop the statement).
	*/
	private void updateSYSSTATEMENTS(LanguageConnectionContext lcc, int mode, TransactionController tc)
		throws StandardException
	{
		int[] 					colsToUpdate;
		boolean					updateSYSCOLUMNS,  recompile;
		//bug 4821 - we want to wait for locks if updating sysstatements on parent transaction
		boolean wait = false;
		boolean firstCompilation = false;
		if (mode == RECOMPILE)
		{
			recompile = true;
			updateSYSCOLUMNS = true;
			if(!initiallyCompilable)
			{
				firstCompilation = true;
				initiallyCompilable = true;
			}
		}
		else
		{
			recompile = false;
			updateSYSCOLUMNS = false;
		}

		DataDictionary dd = getDataDictionary();

		if (((org.apache.derby.impl.sql.catalog.DataDictionaryImpl) dd).readOnlyUpgrade)
			return;


		/*
		** Get busy time
		*/
		dd.startWriting(lcc);	

		if (tc == null) { //bug 4821 - tc will passed null if we want to use the user transaction
			tc = lcc.getTransactionExecute();
			wait = true;
		}

		dd.updateSPS(this,
					 tc, 
					 recompile,
					 updateSYSCOLUMNS,
					 wait,
					 firstCompilation);
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

	/** @see TupleDescriptor#getDescriptorType */
	public String getDescriptorType() { return "Statement"; }

	/** @see TupleDescriptor#getDescriptorName */
	// RESOLVE: some descriptors have getName.  some descriptors have
	// getTableName, getColumnName whatever! try and unify all of this to one
	// getDescriptorName! 
	public String getDescriptorName() { return name; }
	
}

