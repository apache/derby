/*

   Derby - Class org.apache.derby.impl.sql.execute.SetConstraintsConstantAction

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.sql.dictionary.CheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;


import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.util.Hashtable;
import java.util.Enumeration;

/**
 * This class describes actions that are performed for a
 * set constraint at Execution time.  
 * <p>
 * Note that the dependency action we send is SET_CONSTRAINTS
 * rather than ALTER_TABLE.  We do this because we want
 * to distinguish SET_CONSTRAINTS from ALTER_TABLE for
 * error messages.
 *
 *	@author jamie
 */
class SetConstraintsConstantAction extends DDLConstantAction
{

	private boolean 					enable;
	private	boolean						unconditionallyEnforce;

	/*
	** For the following fields, never access directly, always
	** get the constraint descript list via the private
	** method getConstraintDescriptorList() defined herein.
	*/
	private ConstraintDescriptorList	cdl;
	private UUID[]						cuuids;
	private UUID[]						tuuids;

	// CONSTRUCTORS
	/**
	 *Boilerplate
	 *
	 * @param cdl						ConstraintDescriptorList
	 * @param enable					true == turn them on, false == turn them off
	 * @param unconditionallyEnforce	Replication sets this to true at
	 *									the end of REFRESH. This forces us
	 *									to run the included foreign key constraints even
	 *									if they're already marked ENABLED.
	 */
	SetConstraintsConstantAction
	(
		ConstraintDescriptorList	cdl,
		boolean						enable,
		boolean						unconditionallyEnforce
	)
	{
		this.cdl = cdl;
		this.enable = enable;
		this.unconditionallyEnforce = unconditionallyEnforce;
	}

	//////////////////////////////////////////////////////////////
	//
	// OBJECT SHADOWS
	//
	//////////////////////////////////////////////////////////////

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "SET CONSTRAINTS";
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP CONSTRAINT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		ConstraintDescriptor		cd;
		TableDescriptor				td;
		ConstraintDescriptorList	tmpCdl;
		boolean						enforceThisConstraint;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		tmpCdl = getConstraintDescriptorList(dd);

		int[] enabledCol = new int[1];
		enabledCol[0] = ConstraintDescriptor.SYSCONSTRAINTS_STATE_FIELD;
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

		/*
		** Callback to rep subclass
		*/
		publishToTargets(activation);

		boolean skipFKs = false;

		/*
		** If the constraint list is empty, then we are getting
		** all constraints.  In this case, don't bother going
		** after referencing keys (foreign keys) when we are 
		** disabling a referenced key (pk or unique key) since
		** we know we'll hit it eventually.	
		*/
		if (tmpCdl == null)
		{
			skipFKs = true;
			tmpCdl = dd.getConstraintDescriptors((TableDescriptor)null);
		}
	
		Hashtable checkConstraintTables = null;
		int cdlSize = tmpCdl.size();
		for (int index = 0; index < cdlSize; index++)
		{
			cd = tmpCdl.elementAt(index);

			/*	
			** We are careful to enable this constraint before trying
			** to enable constraints that reference it.  Similarly,
			** we disabled constraints that reference us before we
			** disable ourselves, to make sure everything works ok.
			*/
			if (unconditionallyEnforce) 
			{ 
				enforceThisConstraint = true; 
			}
			else 
			{ 
				enforceThisConstraint = (enable && !cd.isEnabled()); 
			}

			if (enforceThisConstraint)
			{
				if (cd instanceof ForeignKeyConstraintDescriptor)
				{
					validateFKConstraint((ForeignKeyConstraintDescriptor)cd, dd, tc, lcc.getContextManager());
				}
				/*
				** For check constraints, we build up a list of check constriants
				** by table descriptor.  Once we have collected them all, we
				** execute them in a single query per table descriptor.
				*/
				else if (cd instanceof CheckConstraintDescriptor)
				{
					td = cd.getTableDescriptor();

					if (checkConstraintTables == null)
					{
						checkConstraintTables = new Hashtable(10);
					}

					ConstraintDescriptorList tabCdl = (ConstraintDescriptorList)
												checkConstraintTables.get(td.getUUID());
					if (tabCdl == null)
					{
						tabCdl = new ConstraintDescriptorList();
						checkConstraintTables.put(td.getUUID(), tabCdl);
					}
					tabCdl.add(cd);
				}
				/*
				** If we are enabling a constraint, we need to issue
				** the invalidation on the underlying table rather than
				** the constraint we are enabling.  This is because
				** stmts that were compiled against a disabled constraint
				** have no depedency on that disabled constriant.
				*/
				dm.invalidateFor(cd.getTableDescriptor(), 
									DependencyManager.SET_CONSTRAINTS_ENABLE, lcc);
				cd.setEnabled();
				dd.updateConstraintDescriptor(cd, 
											cd.getUUID(), 
											enabledCol, 
											tc);
			}
	
			/*
			** If we are dealing with a referenced constraint, then
			** we find all of the constraints that reference this constraint.
			** Turn them on/off based on what we are doing to this
			** constraint.
			*/
			if (!skipFKs &&
				(cd instanceof ReferencedKeyConstraintDescriptor))
			{
				ForeignKeyConstraintDescriptor fkcd;
				ReferencedKeyConstraintDescriptor refcd;
				ConstraintDescriptorList fkcdl;
	
				refcd = (ReferencedKeyConstraintDescriptor)cd;
				fkcdl = refcd.getForeignKeyConstraints(ReferencedKeyConstraintDescriptor.ALL);

				int fkcdlSize = fkcdl.size();
				for (int inner = 0; inner < fkcdlSize; inner++)
				{
					fkcd = (ForeignKeyConstraintDescriptor) fkcdl.elementAt(inner);	
					if (enable && !fkcd.isEnabled())
					{
						dm.invalidateFor(fkcd.getTableDescriptor(), 
									DependencyManager.SET_CONSTRAINTS_ENABLE, lcc);
						validateFKConstraint(fkcd, dd, tc, lcc.getContextManager());
						fkcd.setEnabled();
						dd.updateConstraintDescriptor(fkcd, 
								fkcd.getUUID(), 
								enabledCol, 
								tc);
					}
					else if (!enable && fkcd.isEnabled())
					{
						dm.invalidateFor(fkcd, DependencyManager.SET_CONSTRAINTS_DISABLE,
										 lcc);
						fkcd.setDisabled();
						dd.updateConstraintDescriptor(fkcd, 
								fkcd.getUUID(), 
								enabledCol, 
								tc);
					}
				}
			}
	
			if (!enable && cd.isEnabled())
			{
				dm.invalidateFor(cd, DependencyManager.SET_CONSTRAINTS_DISABLE,
								 lcc);
				cd.setDisabled();
				dd.updateConstraintDescriptor(cd, 
												cd.getUUID(), 
												enabledCol, 
												tc);
			}
		}

		validateAllCheckConstraints(lcc, checkConstraintTables);
	}

	private void validateAllCheckConstraints(LanguageConnectionContext lcc, Hashtable ht)
		throws StandardException
	{
		ConstraintDescriptorList	cdl;
		ConstraintDescriptor		cd = null;
		TableDescriptor				td;
		StringBuffer				text;
		StringBuffer				constraintNames;

		if (ht == null)
		{
			return;
		}

		for (Enumeration e = ht.elements(); e.hasMoreElements(); )
		{
		
			cdl = (ConstraintDescriptorList) e.nextElement();
			text = null;
			constraintNames = null;

			/*
			** Build up the text of all the constraints into one big
			** predicate.  Also, we unfortunately have to build up a big
			** comma separated list of constraint names in case
			** there is an error (we are favoring speed over a very
			** explicit check constraint xxxx failed error message).
			*/
			int cdlSize = cdl.size();
			for (int index = 0; index < cdlSize; index++)
			{
				cd = (CheckConstraintDescriptor) cdl.elementAt(index);
				if (text == null)
				{
					text = new StringBuffer("(").append(cd.getConstraintText()).append(") ");
					constraintNames = new StringBuffer(cd.getConstraintName());
				}
				else
				{
					text.append(" AND (").append(cd.getConstraintText()).append(") ");
					constraintNames.append(", ").append(cd.getConstraintName());
				}
			}

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(text != null, "internal error, badly built hastable");
			}

			ConstraintConstantAction.validateConstraint(
												constraintNames.toString(),
												text.toString(),
												cd.getTableDescriptor(),
												lcc, true);
		}
	}

	/*
	**
	*/
	private void validateFKConstraint
	(
		ForeignKeyConstraintDescriptor	fk,
		DataDictionary					dd,
		TransactionController			tc,
		ContextManager					cm
	)
		throws StandardException
	{
		/*
		** Construct a template row 
		*/
		IndexRowGenerator irg = fk.getIndexConglomerateDescriptor(dd).getIndexDescriptor();	
		ExecIndexRow indexTemplateRow = irg.getIndexRowTemplate();
		TableDescriptor td = fk.getTableDescriptor();
		ExecRow baseRow = td.getEmptyExecRow(cm);
		irg.getIndexRow(baseRow, getRowLocation(dd, td, tc), 
								indexTemplateRow, (FormatableBitSet)null);

		/*
		** The moment of truth
		*/
		ConstraintConstantAction.validateFKConstraint(tc, dd, fk, 
							fk.getReferencedConstraint(), indexTemplateRow);
	}
			
	/*
	** Get a row location template.  Note that we are assuming that
	** the same row location can be used for all tables participating
	** in the fk.  For example, if there are multiple foreign keys,
	** we are using the row location of one of the tables and assuming
	** that it is the right shape for all tables.  Currently, this
	** is a legitimate assumption.
	*/
	private RowLocation getRowLocation
	(
		DataDictionary			dd, 
		TableDescriptor			td,
		TransactionController	tc
	) 
		throws StandardException
	{
		RowLocation 			rl; 
		ConglomerateController 	heapCC = null;

		long tableId = td.getHeapConglomerateId();
		heapCC = 
            tc.openConglomerate(
                tableId, false, 0, tc.MODE_RECORD, tc.ISOLATION_READ_COMMITTED);
		try
		{
			rl = heapCC.newRowLocationTemplate();
		}
		finally
		{
			heapCC.close();
		}

		return rl;
	}
		
	/*
	** Wrapper for constraint descriptor list -- always use
	** this to get the constriant descriptor list.  It is
	** used to hide serialization.
	*/
	private ConstraintDescriptorList getConstraintDescriptorList(DataDictionary dd)
		throws StandardException
	{
		if (cdl != null)
		{
			return cdl;
		}
		if (tuuids == null)
		{
			return null;
		}

		/*
		** Reconstitute the cdl from the uuids
		*/
		cdl = new ConstraintDescriptorList();

		for (int i = 0; i < tuuids.length; i++)
		{
			TableDescriptor td = dd.getTableDescriptor(tuuids[i]);
			if (SanityManager.DEBUG)
			{
				if (td == null)
				{
					SanityManager.THROWASSERT("couldn't locate table descriptor "+
						"in SET CONSTRAINTS for uuid "+tuuids[i]);
				}
			}

			ConstraintDescriptor cd = dd.getConstraintDescriptorById(td, cuuids[i]);

			if (SanityManager.DEBUG)
			{
				if (cd == null)
				{
					SanityManager.THROWASSERT("couldn't locate constraint descriptor "+
						" in SET CONSTRAINTS for uuid "+cuuids[i]);
				}
			}

			cdl.add(cd);
		}
		return cdl;
	}
		
	/**
	 * Does this constant action modify the passed in table
	 * uuid?  By modify we mean add or drop things tied to
	 * this table (e.g. index, trigger, constraint).  Things
	 * like views or spses that reference this table don't
	 * count.
	 * <p>
	 * For SET CONSTRAINTS return true if it is SET CONSTRAINTS
	 * ALL otherwise, compare the table ids.
	 *
	 * @param tableId the table id
	 *
	 * @exception StandardException on error
	 */
	public boolean modifiesTableId(UUID tableId) throws StandardException
	{
		if (tuuids != null) {
			for (int i = 0; i < tuuids.length; i++) {
				if (tableId.equals(tuuids[i]))
					return true;
			}
			return false;
		}

		// assume SET CONSTRAINTS ALL touches this table
		if ((cdl == null) || (cdl.size() == 0))
		{
			return true;
		}

		int cdlSize = cdl.size();
		for (int index = 0; index < cdlSize; index++)
		{
			ConstraintDescriptor cd = cdl.elementAt(index);
			if (cd.getTableId().equals(tableId))
			{
				return true;
			}
		}
		
		return false;
	}

	///////////////////////////////////////////////
	//
	// MISC
	//
	///////////////////////////////////////////////

	/**
	 * Do the work of publishing any this action to any
	 * replication targets.  On a non-replicated source,
	 * this is a no-op.
	 *
	 * @param activation the activation
	 *
	 * @exception StandardException on error
	 */
	protected void publishToTargets(Activation activation)
		throws StandardException
	{
	}
}
