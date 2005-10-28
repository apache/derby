/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.Dependent;

import org.apache.derby.catalog.UUID;

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
import	org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

/**
 * This class is used to get information from a ConstraintDescriptor.
 * A ConstraintDescriptor can represent a constraint on a table or on a
 * column.
 *
 * @version 0.1
 * @author Jeff Lichtman
 */

public abstract class ConstraintDescriptor 
	extends TupleDescriptor
	implements UniqueTupleDescriptor, Provider, Dependent
{
	// used to indicate what type of constraints we 
	// are interested in
	public static final int ENABLED		= 1;
	public static final int DISABLED	= 2;
	public static final int ALL			= 3;

	// field that we want users to be able to know about
	public static final int SYSCONSTRAINTS_STATE_FIELD = 6;

	TableDescriptor		table;
	String				constraintName;
	boolean				deferrable;
	boolean				initiallyDeferred;
	boolean				isEnabled;
	int[]				referencedColumns;
	UUID					constraintId;
	SchemaDescriptor	schemaDesc;
	ColumnDescriptorList	colDL;

	/**
	 * Constructor for a ConstraintDescriptor
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param table		The descriptor of the table the constraint is on
	 * @param constraintName	The name of the constraint.
	 * @param deferrable		If the constraint can be deferred.
	 * @param initiallyDeferred If the constraint starts life deferred.
	 * @param referencedColumns columns that the constraint references
	 * @param checkConstraint	the expression for a check constraint.
	 * @param constraintId		UUID of constraint
	 * @param schemaDesc		SchemaDescriptor
	 */

	ConstraintDescriptor(
		    DataDictionary dataDictionary,
			TableDescriptor table,
			String constraintName,
			boolean deferrable,
			boolean initiallyDeferred,
			int[] referencedColumns,
			UUID constraintId,
			SchemaDescriptor schemaDesc,
			boolean isEnabled
			)
	{
		super( dataDictionary );

		this.table = table;
		this.constraintName = constraintName;
		this.deferrable = deferrable;
		this.initiallyDeferred = initiallyDeferred;
		this.referencedColumns = referencedColumns;
		this.constraintId = constraintId;
		this.schemaDesc = schemaDesc;
		this.isEnabled = isEnabled;
	}


	/**
	 * Gets the UUID of the table the constraint is on.
	 *
	 * @return	The UUID of the table the constraint is on.
	 */
	public UUID	getTableId()
	{
		return table.getUUID();
	}

	/**
	 * Sets the UUID of the constraint.
	 *
	 * @param constraintId	The constraint Id.
	 * @return	Nothing.
	 */
	public void	setConstraintId(UUID constraintId)
	{
		this.constraintId = constraintId;
	}

	/**
	 * Gets the UUID of the constraint.
	 *
	 * @return	The UUID of the constraint.
	 */
	public UUID	getUUID()
	{
		return constraintId;
	}

	/**
	 * Gets the name of the constraint.
	 *
	 * @return	A String containing the name of the constraint.
	 */
	public String	getConstraintName()
	{
		return constraintName;
	}

	/**
	 * Gets an identifier telling what type of descriptor it is
	 * (UNIQUE, PRIMARY KEY, FOREIGN KEY, CHECK).
	 *
	 * @return	An identifier telling what type of descriptor it is
	 *		(UNIQUE, PRIMARY KEY, FOREIGN KEY, CHECK).
	 */
	public abstract int	getConstraintType();

	public abstract UUID getConglomerateId();

	/**
	 * Get the text of the constraint. (Only non-null/meaningful for check
	 * constraints.)
	 * @return	The constraint text.
	 */
	public String getConstraintText()
	{
		return null;
	}

	/**
	 * Returns TRUE if the constraint is deferrable
	 * (we will probably not do deferrable constraints in the
	 * initial release, but I want this to be part of the interface).
	 *
	 * @return	TRUE if the constraint is deferrable, FALSE if not
	 */
	public boolean	deferrable()
	{
		return deferrable;
	}

	/**
	 * Returns TRUE if the constraint is initially deferred
	 * (we will probably not do initially deferred constraints
	 * in the initial release, but I want this to be part of the interface).
	 *
	 * @return	TRUE if the constraint is initially deferred,
	 *		FALSE if not
	 */
	public boolean	initiallyDeferred()
	{
		return initiallyDeferred;
	}

	/**
	 * Returns an array of column ids (i.e. ordinal positions) for
	 * the columns referenced in this table for a primary key, unique
	 * key, referential, or check constraint.
	 *
	 * @return	An array of column ids for those constraints that can
	 *		be on columns (primary, unique key, referential
	 *		constraints, and check constraints).  For check and
	 *		unique constraints, it returns an array of columns ids
	 *		that are referenced in the constraint.  For primary key
	 *		and referential constraints, it returns an array of
	 *		column ids for the columns in this table (i.e. the
	 *		primary key columns for a primary key constraint,
	 *		and the foreign key columns for a foreign key
	 *		constraint.
	 */
	public int[]	getReferencedColumns()
	{
		return referencedColumns;
	}

	/**
	 * Does this constraint have a backing index?
	 *
	 * @return boolean	Whether or not there is a backing index for this constraint.
	 */
	public abstract boolean hasBackingIndex();

	/**
	 * Get the SchemaDescriptor for the schema that this constraint
	 * belongs to.
	 *
	 * @return SchemaDescriptor The SchemaDescriptor for this constraint.
	 */
	public SchemaDescriptor getSchemaDescriptor()
	{
		return schemaDesc;
	}

	/**
	  RESOLVE: For now the ConstraintDescriptor code stores the array of key
	  columns in the field 'otherColumns'. Jerry plans to re-organize things.
	  For now to minimize his rototill I've implemented this function on the
	  old structures. All new code should use getKeyColumns to get a constraint's
	  key columns.
	  
	  @see org.apache.derby.iapi.sql.dictionary.KeyConstraintDescriptor#getKeyColumns
	  */
	public int[]	getKeyColumns()
	{
		return getReferencedColumns();
	}

	/**
	 * Is this constraint active?
	 *
	 * @return true/false
	 */
	public boolean isEnabled()
	{
		return isEnabled;
	}

	/**
	 * Set the constraint to enabled.
	 * Does not update the data dictionary
	 */
	public void setEnabled()
	{
		isEnabled = true;
	}

	/**
	 * Set the constraint to disabled.
	 * Does not update the data dictionary
	 */
	public void setDisabled()
	{
		isEnabled = false;
	}

	/**
	 * Is this constraint referenced?  Return
	 * false.  Overridden by ReferencedKeyConstraints.
	 *
	 * @return false
	 */
	public boolean isReferenced()
	{
		return false;
	}

	/**
	 * Get the number of enabled fks that
	 * reference this key.  Overriden by
	 * ReferencedKeyConstraints.
	 *
	 * @return the number of fks
	 */
	public int getReferenceCount()
	{
		return 0;
	}

	/**
	 * Does this constraint need to fire on this type of
	 * DML?
	 *
	 * @param dmlType	the type of DML 
	 * (StatementType.INSERT|StatementType.UPDATE|StatementType.DELETE)
	 * @param modifiedCols	the columns modified, or null for all
	 *
	 * @return true/false
	 */
	public abstract boolean needsToFire(int stmtType, int[] modifiedCols);

	/**
	 * Get the table descriptor upon which this constraint
	 * is declared.
	 *
	 * @return the table descriptor
	 */
	public TableDescriptor getTableDescriptor()
	{
		return table;
	}

	/**
	 * Get the column descriptors for all the columns
	 * referenced by this constraint.
	 *
	 * @return the column descriptor list
	 *
	 * @exception StandardException on error
	 */
	public ColumnDescriptorList getColumnDescriptors()
		throws StandardException
	{
		if (colDL == null)
		{
			DataDictionary dd = getDataDictionary();
			colDL = new ColumnDescriptorList();
	
			int[]	refCols = getReferencedColumns();
			for (int i = 0; i < refCols.length; i++)
			{
				colDL.add(table.getColumnDescriptor(refCols[i]));
			}
		}
		return colDL;
	}

	/**
	 * Indicates whether the column descriptor list is
	 * type comparable with the constraints columns.  The
	 * types have to be identical AND in the same order 
	 * to succeed.
	 *
	 * @param otherColumns the columns to compare
	 *
	 * @return true/false
	 *
	 * @exception StandardException on error
	 */
	public boolean areColumnsComparable(ColumnDescriptorList otherColumns)
		throws StandardException
	{
		ColumnDescriptor		myColumn;
		ColumnDescriptor		otherColumn;

		ColumnDescriptorList	myColDl = getColumnDescriptors();

		/*
		** Check the lenghts of the lists
		*/
		if (otherColumns.size() != myColDl.size())
		{
			return false;
		}

		int mySize = myColDl.size();
		int otherSize = otherColumns.size();
		int index;
		for (index = 0; index < mySize && index < otherSize; index++)
		{
			myColumn = (ColumnDescriptor) myColDl.elementAt(index);	
			otherColumn = (ColumnDescriptor) otherColumns.elementAt(index);	

			/*
			** Just compare the types.  Note that this will
	 		** say a decimal(x,y) != numeric(x,y) even though
			** it does.
			*/
			if (!(myColumn.getType()).isExactTypeAndLengthMatch(
					(otherColumn.getType())))
			{
				break;
			}
		}

		return (index == mySize && index == otherSize);
	}

	/**
	 * Does a column intersect with our referenced columns
	 * @param int ColumnNumber
	 * 
	 * Note-- this is not a static method.
	 */
	public boolean columnIntersects(int columnArray[])
	{
		// call static method.
		return doColumnsIntersect(getReferencedColumns(), columnArray);
	}

	/**
	 * Does a column in the input set intersect with
	 * our referenced columns?
	 *
	 * @param otherColumns the columns to compare. If
	 *	null, asssumed to mean all columns
	 *
	 * @param the columns referenced by the caller
	 *
	 * @return true/false
	 */
	static boolean doColumnsIntersect(int[] otherColumns, int[] referencedColumns)
	{
		/*
		** It is assumed that if otherColumns is null, then
		** all other columns are modified.  In this case,
		** it is assumed that it intersects with some column
	 	** of ours, so just return true.
		*/
		if ((otherColumns == null) || (referencedColumns == null))
		{
			return true;
		}

		for (int outer = 0; outer < referencedColumns.length; outer++)
		{	
			for (int inner = 0; inner < otherColumns.length; inner++)
			{
				if (referencedColumns[outer] == otherColumns[inner])
				{
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Convert the ColumnDescriptor to a String.
	 *
	 * @return	A String representation of this ColumnDescriptor
	 */

	public String	toString()
	{
		if (SanityManager.DEBUG)
		{
			String tableDesc =
				"table: " +
				table.getQualifiedName() + "(" +
				table.getUUID()+","+
				table.getTableType()+")";

			return tableDesc + "\n"+
				"constraintName: " + constraintName + "\n" +
				"constraintId: " + constraintId + "\n" +
				"deferrable: " + deferrable + "\n" +
				"initiallyDeferred: " + initiallyDeferred + "\n" +
				"referencedColumns: " + referencedColumns + "\n" +
				"schemaDesc: " + schemaDesc + "\n"
				;
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
		@return the stored form of this provider

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder() 
	{
	    return	getDependableFinder(StoredFormatIds.CONSTRAINT_DESCRIPTOR_FINDER_V01_ID);
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		return constraintName;
	}

	/**
	 * Get the provider's UUID
	 *
	 * @return 	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return constraintId;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return char		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.CONSTRAINT;
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
	public synchronized boolean isValid()
	{
		return true;
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
	public void prepareToInvalidate(Provider p, int action,
					LanguageConnectionContext lcc) 
		throws StandardException
	{
		DependencyManager dm = getDataDictionary().getDependencyManager();

		switch (action)
		{
			/*
			** A SET CONSTRAINT stmt will throw an SET_CONSTRAINTS action
			** when enabling/disabling constraints.  We'll ignore it.
			** Same for SET TRIGGERS
			*/
		    case DependencyManager.SET_CONSTRAINTS_ENABLE:
		    case DependencyManager.SET_CONSTRAINTS_DISABLE:
		    case DependencyManager.SET_TRIGGERS_ENABLE:
		    case DependencyManager.SET_TRIGGERS_DISABLE:
				break;

			/*
			** Currently, the only thing we are depenedent
			** on is another constraint or an alias..
			*/
		    default:
				throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT, 
									dm.getActionString(action), 
									p.getObjectName(), "CONSTRAINT", constraintName);
		}
	}

	/**
	 * Mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).  Always an error
	 * for a constraint -- should never have gotten here.
	 *
	 * @param	action	The action causing the invalidation
	 *
	 * @exception StandardException thrown if called in sanity mode
	 */
	public void makeInvalid(int action, LanguageConnectionContext lcc) 
		throws StandardException
	{
		/*
		** SET_CONSTRAINTS/TRIGGERS is the only valid action
		*/
		if ((action != DependencyManager.SET_CONSTRAINTS_DISABLE) &&
			(action != DependencyManager.SET_CONSTRAINTS_ENABLE) &&
			(action != DependencyManager.SET_TRIGGERS_ENABLE) &&
			(action != DependencyManager.SET_TRIGGERS_DISABLE)
		   )
		{
			/* 
			** We should never get here, we should have barfed on 
			** prepareToInvalidate().
			*/
			if (SanityManager.DEBUG)
			{
				DependencyManager dm;
		
				dm = getDataDictionary().getDependencyManager();
	
				SanityManager.THROWASSERT("makeInvalid("+
					dm.getActionString(action)+
					") not expected to get called");
			}
		}
	}

	/**
     * Attempt to revalidate the dependent. Meaningless
	 * for constraints.
	 */
	public void makeValid(LanguageConnectionContext lcc) 
	{
	}
	
	/** @see TupleDescriptor#getDescriptorName */
	public String getDescriptorName() { return constraintName; }
	
	public String getDescriptorType() { return "Constraint"; }
}
