/*

   Derby - Class org.apache.derby.impl.sql.execute.AlterSPSConstantAction

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;

import org.apache.derby.iapi.sql.depend.Dependency;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;

import java.util.List;
import java.util.Enumeration;


/**
 * This class  describes actions that are performed for an
 * ALTER STATEMENT Statement at Execution time.  Currently,
 * all you can do is RECOMPILE a statement (or all statements).
 * The syntax is: <UL><I>
 *	<LI>ALTER STATEMENT <stmt> RECOMPILE ALL	</LI>
 *	<LI>ALTER STATEMENT <stmt> RECOMPILE [ USING ...] </LI> </I></UL>
 *
 * @author jamie
 */
public class AlterSPSConstantAction extends GenericSPSConstantAction
{

	private SchemaDescriptor		sd;
	private String					spsName;
	private UUID					schemaId;
	private String					usingText; 
	private boolean					invalidOnly; 

	///////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	///////////////////////////////////////////////

	/**
	 * Perform an ALTER STATEMENT statement. Currently,
	 * this is only RECOMPILE.
	 * <p>
	 * Note: if parameters are NULL, then all statements
	 * are recompiled.
	 *
	 *  @param sd			descriptor of the schema in which
	 *						our beloved stmt resides.  Null
	 *						if all statements (if spsName is null)
	 *  @param spsName		Name of sps.  if null, all statements
	 *						are recompiled
	 *	@param usingText	the text of the USING clause	
	 *	@param invalidOnly	only recompile invalid spses.  Only relevant
	 *						when name is null.
	 */
	AlterSPSConstantAction
	(
		SchemaDescriptor	sd,
		String				spsName,
		String				usingText,
		boolean				invalidOnly
	)
	{
		this.sd = sd;
		this.spsName = spsName;
		this.usingText = usingText;
		this.invalidOnly = invalidOnly;

		if (SanityManager.DEBUG)
		{
			if (spsName != null)
			{
				SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
			}
		}
	}

	///////////////////////////////////////////////
	//
	// OBJECT SHADOWS
	//
	///////////////////////////////////////////////

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		String				schemaName = "???";

		if ( sd != null ) { schemaName = sd.getSchemaName(); }

		return "ALTER STATEMENT " + schemaName + "." + spsName;
	}

	///////////////////////////////////////////////
	//
	// INTERFACE METHODS
	//
	///////////////////////////////////////////////

	/**
	 *	This is the guts of the Execution-time logic for ALTER STATEMENT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		SPSDescriptor 				spsd;
		SchemaDescriptor			schemaDescriptor;
		List						spsList;
		boolean						onlyRecompInvalid = false;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		/*
		** Do startWriting before we attempt to 
		** read in any spses.  Otherwise, we may
		** get a cached sps.  When the sps is revalidated,
		** it also does a dd.startWriting() but we
		** should avoid getting a cached copy to be	
		** safe.
		*/
		dd.startWriting(lcc);

		/*
		** If we have an individual sps, then find
		** it now.
		*/
		if (spsName != null)
		{
			/*
			** If the schema descriptor is null, then
			** we must have just read ourselves in.  
			** So we will get the corresponding schema
			** descriptor from the data dictionary.
			*/
			if (sd == null)
			{
				sd = getAndCheckSchemaDescriptor(dd, schemaId, "ALTER STATEMENT");
			}
			spsd = dd.getSPSDescriptor(spsName, sd);
			if (spsd == null)
			{
				throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION, "STATEMENT",
						(sd.getSchemaName() + "." + spsName));
			}

			if (usingText != null)
			{
				spsd.setParameterDefaults(getUsingResults(usingText));
				spsd.setUsingText(usingText);
			}

			spsList = new java.util.ArrayList();
			spsList.add(spsd);
		}
		/*
		** If we have a null spsName, get all SPSes
		*/
		else
		{
			onlyRecompInvalid = invalidOnly;
			spsList = dd.getAllSPSDescriptors();
		}

		for (java.util.Iterator it = spsList.iterator(); it.hasNext(); )
		{
			spsd = (SPSDescriptor) it.next();
			if ((onlyRecompInvalid && !spsd.isValid()) ||
				 !onlyRecompInvalid)
			{
				spsd.revalidate(lcc);
			}
		}

	}



}
