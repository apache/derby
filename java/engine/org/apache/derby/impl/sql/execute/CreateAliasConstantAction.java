/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateAliasConstantAction

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

import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.types.RoutineAliasInfo;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	CREATE ALIAS Statement at Execution time.
 *
 *	@author Jerry Brenner.
 */
class CreateAliasConstantAction extends DDLConstantAction
{

	private final String					aliasName;
	private final String					schemaName;
	private final String					javaClassName;
	private final char					aliasType;
	private final char					nameSpace;
	private final AliasInfo				aliasInfo;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a CREATE ALIAS statement.
	 *
	 *  @param aliasName		Name of alias.
	 *  @param schemaName		Name of alias's schema.
	 *  @param javaClassName	Name of java class.
	 *  @param methodName		Name of method.
	 *  @param targetClassName	Name of java class at Target database.
	 *  @param targetMethodName	Name of method at Target database.
	 *  @param aliasType		The type of the alias
	 */
	CreateAliasConstantAction(
								String	aliasName,
								String	schemaName,
								String	javaClassName,
								AliasInfo	aliasInfo,
								char	aliasType)
	{
		this.aliasName = aliasName;
		this.schemaName = schemaName;
		this.javaClassName = javaClassName;
		this.aliasInfo = aliasInfo;
		this.aliasType = aliasType;
		switch (aliasType)
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR;
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Unexpected value for aliasType (" + aliasType + ")");
				}
				nameSpace = '\0';
				break;
		}
	}

	// OBJECT SHADOWS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		String type = null;

		switch (aliasType)
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				type = "CREATE PROCEDURE ";
				break;

			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				type = "CREATE FUNCTION ";
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Unexpected value for aliasType (" + aliasType + ")");
				}
		}

		return	type + aliasName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for CREATE ALIAS.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		LanguageConnectionContext lcc;
		if (activation != null)
		{
			lcc = activation.getLanguageConnectionContext();
		}
		else // only for direct executions by the database meta data
		{
			lcc = (LanguageConnectionContext) ContextService.getContext
          				(LanguageConnectionContext.CONTEXT_ID);
		}
		DataDictionary dd = lcc.getDataDictionary();

		/* Verify the method alias:
		**		Aggregates - just verify the class
		**		Method alias - verify the class and method
		**		Work units - verify the class and method 
		**				(depends on whether we're at a source or target)
		*/
		String checkMethodName = null;

			
		String checkClassName = javaClassName;

		if (aliasInfo != null)
			checkMethodName = aliasInfo.getMethodName();

		// Procedures do not check class or method validity until runtime execution of the procedure.
		// This matches DB2 behaviour
		switch (aliasType)
		{
		case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
		case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
			break;
		default:
		{

			ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();

			Class realClass = null;
			try
			{
				// Does the class exist?
				realClass = cf.loadApplicationClass(checkClassName);
			}
			catch (Throwable t)
			{
				throw StandardException.newException(SQLState.LANG_TYPE_DOESNT_EXIST2, t, checkClassName);
			}

			if (! Modifier.isPublic(realClass.getModifiers()))
			{
				throw StandardException.newException(SQLState.LANG_TYPE_DOESNT_EXIST2, checkClassName);
			}

			if (checkMethodName != null)
			{
				// Is the method public and static
				Method[] methods = realClass.getMethods();
				
				int index = 0;
				for ( ; index < methods.length; index++) 
				{
					if (!Modifier.isStatic(methods[index].getModifiers()))
					{
						continue;
					}

					if (checkMethodName.equals(methods[index].getName())) 
					{
						break;
					}
				}

				if (index == methods.length)
				{
					throw StandardException.newException(SQLState.LANG_NO_METHOD_MATCHING_ALIAS, 
									checkMethodName, checkClassName);
				}
			}
		}
		}
			

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

		
		SchemaDescriptor sd = null;
		if (activation == null)
			sd = dd.getSysIBMSchemaDescriptor();
		else if (schemaName != null)
			sd = DDLConstantAction.getSchemaDescriptorForCreate(dd, activation, schemaName);

		//
		// Create a new method alias descriptor with aliasID filled in.
		// 
		UUID aliasID = dd.getUUIDFactory().createUUID();

		AliasDescriptor ads = new AliasDescriptor(dd, aliasID,
									 aliasName,
									 sd != null ? sd.getUUID() : null,
									 javaClassName,
									 aliasType,
									 nameSpace,
									 false,
									 aliasInfo, null);

		// perform duplicate rule checking for routine
		switch (aliasType) {
		case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
		case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
		{

			java.util.List list = dd.getRoutineList(
				sd.getUUID().toString(), aliasName, aliasType);
			for (int i = list.size() - 1; i >= 0; i--) {

				AliasDescriptor proc = (AliasDescriptor) list.get(i);

				RoutineAliasInfo procedureInfo = (RoutineAliasInfo) proc.getAliasInfo();
				int parameterCount = procedureInfo.getParameterCount();
				if (parameterCount != ((RoutineAliasInfo) aliasInfo).getParameterCount())
					continue;

				// procedure duplicate checking is simple, only
				// one procedure with a given number of parameters.
				throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
												ads.getDescriptorType(),
												aliasName);
			}
		}
		break;
		default:
			break;
		}

		dd.addDescriptor(ads, null, DataDictionary.SYSALIASES_CATALOG_NUM,
						 false, lcc.getTransactionExecute());
	}

}
