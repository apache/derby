/*

   Derby - Class org.apache.derby.impl.sql.execute.DropAliasConstantAction

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.AliasInfo;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP ALIAS Statement at Execution time.
 *
 *	@author Jerry Brenner.
 */

class DropAliasConstantAction extends DDLConstantAction
{

	private SchemaDescriptor	sd;
	private final String schemaName;
	private final String				aliasName;
	private final char				nameSpace;

	// CONSTRUCTORS


	/**
	 *	Make the ConstantAction for a DROP  ALIAS statement.
	 *
	 *
	 *	@param	aliasName			Alias name.
	 *	@param	nameSpace			Alias name space.
	 *
	 */
	DropAliasConstantAction(SchemaDescriptor sd, String aliasName, char nameSpace)
	{
		this.sd = sd;
		this.schemaName = sd.getSchemaName();
		this.aliasName = aliasName;
		this.nameSpace = nameSpace;
	}

	// OBJECT SHADOWS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return	"DROP ALIAS " + aliasName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP ALIAS.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();


		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

		if (sd == null) {
			sd = dd.getSchemaDescriptor(schemaName, lcc.getTransactionExecute(), true);
		}


		/* Get the alias descriptor.  We're responsible for raising
		 * the error if it isn't found 
		 */
		AliasDescriptor ad = dd.getAliasDescriptor(sd.getUUID().toString(), aliasName, nameSpace);

		// RESOLVE - fix error message
		if (ad == null)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "Method alias",  aliasName);
		}

		/* Prepare all dependents to invalidate.  (This is their chance
		 * to say that they can't be invalidated.  For example, an open
		 * cursor referencing a table/view that the user is attempting to
		 * drop.) If no one objects, then invalidate any dependent objects.
		 * We check for invalidation before we drop the descriptor
		 * since the descriptor may be looked up as part of
		 * decoding tuples in SYSDEPENDS.
		 */
		int invalidationType = 0;
		switch (ad.getAliasType())
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				invalidationType = DependencyManager.DROP_METHOD_ALIAS;
				break;
		}

		dm.invalidateFor(ad, invalidationType, lcc);

		/* Drop the alias */
		dd.dropAliasDescriptor(ad, lcc.getTransactionExecute());

	}
}
