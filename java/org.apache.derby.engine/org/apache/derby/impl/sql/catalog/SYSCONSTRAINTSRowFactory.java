/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSCONSTRAINTSRowFactory

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

package org.apache.derby.impl.sql.catalog;

import java.sql.Types;

import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubCheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.compile.ConstraintDefinitionNode;

/**
 * Factory for creating a SYSCONTRAINTS row.
 *
 */

public class SYSCONSTRAINTSRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSCONSTRAINTS";

	protected static final int		SYSCONSTRAINTS_COLUMN_COUNT = 7;
	protected static final int		SYSCONSTRAINTS_CONSTRAINTID = 1;
	protected static final int		SYSCONSTRAINTS_TABLEID = 2;
	protected static final int		SYSCONSTRAINTS_CONSTRAINTNAME = 3;
	protected static final int		SYSCONSTRAINTS_TYPE = 4;
	protected static final int		SYSCONSTRAINTS_SCHEMAID = 5;
    public static final int     SYSCONSTRAINTS_STATE = ConstraintDescriptor.SYSCONSTRAINTS_STATE_FIELD;
	protected static final int		SYSCONSTRAINTS_REFERENCECOUNT = 7;

	protected static final int		SYSCONSTRAINTS_INDEX1_ID = 0;
	protected static final int		SYSCONSTRAINTS_INDEX2_ID = 1;
	protected static final int		SYSCONSTRAINTS_INDEX3_ID = 2;

    private	static	final	boolean[]	uniqueness = {
		                                               true,
													   true,
													   false
	                                                 };

	private static final int[][] indexColumnPositions =
	{
		{SYSCONSTRAINTS_CONSTRAINTID},
		{SYSCONSTRAINTS_CONSTRAINTNAME, SYSCONSTRAINTS_SCHEMAID},
		{SYSCONSTRAINTS_TABLEID}
	};

	private	static	final	String[]	uuids =
	{
		 "8000002f-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000036-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000031-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONSTRAINTS_INDEX1
		,"80000033-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONSTRAINTS_INDEX2
		,"80000035-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONSTRAINTS_INDEX3
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

//IC see: https://issues.apache.org/jira/browse/DERBY-3147
    SYSCONSTRAINTSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSCONSTRAINTS_COLUMN_COUNT, TABLENAME_STRING, 
//IC see: https://issues.apache.org/jira/browse/DERBY-1739
				 indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSCONTRAINTS row
	 *
	 * @return	Row suitable for inserting into SYSCONTRAINTS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecRow    				row;
		int						constraintIType;
		UUID					oid;
		String					constraintSType = null;
		String					constraintID = null;
		String					tableID = null;
		String					constraintName = null;
		String					schemaID = null;

        boolean                 deferrable =
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            ConstraintDefinitionNode.DEFERRABLE_DEFAULT;
        boolean                 initiallyDeferred =
            ConstraintDefinitionNode.INITIALLY_DEFERRED_DEFAULT;
        boolean                 enforced =
            ConstraintDefinitionNode.ENFORCED_DEFAULT;

		int						referenceCount = 0;

		if (td != null)
		{
			ConstraintDescriptor constraint = (ConstraintDescriptor)td;
			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			oid = constraint.getUUID();
			constraintID = oid.toString();

			oid = constraint.getTableId();
			tableID = oid.toString();

			constraintName = constraint.getConstraintName();

			constraintIType = constraint.getConstraintType();
			switch (constraintIType)
			{
			    case DataDictionary.PRIMARYKEY_CONSTRAINT:
				    constraintSType = "P";
					break;

			    case DataDictionary.UNIQUE_CONSTRAINT:
					constraintSType = "U";
					break;

			    case DataDictionary.CHECK_CONSTRAINT:
					constraintSType = "C";
					break;

			    case DataDictionary.FOREIGNKEY_CONSTRAINT:
					constraintSType = "F";
					break;

			    default:
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT("invalid constraint type");
					}
			}

			schemaID = constraint.getSchemaDescriptor().getUUID().toString();

            // constraint characteristics
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            deferrable = constraint.deferrable();
            initiallyDeferred   = constraint.initiallyDeferred();
            enforced   = constraint.enforced();

            referenceCount = constraint.getReferenceCount();
		}

		/* Insert info into sysconstraints */

		/* RESOLVE - It would be nice to require less knowledge about sysconstraints
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSCONSTRAINTS_COLUMN_COUNT);

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		row.setColumn(SYSCONSTRAINTS_CONSTRAINTID, new SQLChar(constraintID));

		/* 2nd column is TABLEID (UUID - char(36)) */
		row.setColumn(SYSCONSTRAINTS_TABLEID, new SQLChar(tableID));

		/* 3rd column is NAME (varchar(128)) */
		row.setColumn(SYSCONSTRAINTS_CONSTRAINTNAME, new SQLVarchar(constraintName));

		/* 4th column is TYPE (char(1)) */
		row.setColumn(SYSCONSTRAINTS_TYPE, new SQLChar(constraintSType));

		/* 5th column is SCHEMAID (UUID - char(36)) */
		row.setColumn(SYSCONSTRAINTS_SCHEMAID, new SQLChar(schemaID));

		/* 6th column is STATE (char(1)) */
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        row.setColumn(SYSCONSTRAINTS_STATE,
            new SQLChar(encodeCharacteristics(deferrable, initiallyDeferred, enforced)));

		/* 7th column is REFERENCED */
		row.setColumn(SYSCONSTRAINTS_REFERENCECOUNT, new SQLInteger(referenceCount));
//IC see: https://issues.apache.org/jira/browse/DERBY-4062

		return row;
	}

    /*
     * Encode the characteristics of the constraints into a single character.
     *
     * {deferrable, initiallyDeferred, enforced}     -> 'e'
     * {deferrable, initiallyDeferred, not enforced} -> 'd'
     * {deferrable, immediate, enforced}             -> 'i'
     * {deferrable, immediate, not enforced}         -> 'j'
     * {not deferrable, immediate, enforced}         -> 'E'
     * {not deferrable, immediate, not enforced      -> 'D'
     *
     * Other combinations are prohibited and not used. Note that the
     * value 'E' is only value used prior to version 10.11, and as
     * such upward compatibily since by default, constraints are {not
     * deferrable, immediate, enforced}.
     */
    private String encodeCharacteristics(
            boolean deferrable, boolean initiallyDeferred, boolean enforced) {
        char c;
//IC see: https://issues.apache.org/jira/browse/DERBY-532

        if (deferrable) {
            if (initiallyDeferred) {
                if (enforced) {
                    c = 'e'; // deferrable initially deferred enforced
                } else {
                    c = 'd'; // deferrable initially deferred not enforced
                }
            } else {
                if (enforced) {
                    c = 'i'; // deferrable initially immediate enforced
                } else {
                    c = 'j'; // deferrable initially immediate not enforced
                }
            }
        } else {
            if (initiallyDeferred) {
                if (SanityManager.DEBUG) {
                    SanityManager.NOTREACHED();
                }
                c = 'E';
            } else {
                if (enforced) {
                    c = 'E'; // not deferrable initially immediate enforced
                } else {
                    c = 'D'; // not deferrable initially immediate not enforced
                }
            }
        }

        return String.valueOf(c);
    }

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a ConstraintDescriptor out of a SYSCONSTRAINTS row
	 *
	 * @param row a SYSCONSTRAINTS row
	 * @param parentTupleDescriptor	Subconstraint descriptor with auxiliary info.
	 * @param dd dataDictionary
	 *
	 * @exception   StandardException thrown on failure
	 */
	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
		ConstraintDescriptor constraintDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSCONSTRAINTS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSCONSTRAINTS row");
		}

		DataValueDescriptor	col;
		ConglomerateDescriptor conglomDesc;
		DataDescriptorGenerator ddg;
		TableDescriptor		td = null;
		int					constraintIType = -1;
		int[]				keyColumns = null;
		UUID				constraintUUID;
		UUID				schemaUUID;
		UUID				tableUUID;
		UUID				referencedConstraintId = null; 
		SchemaDescriptor	schema;
		String				tableUUIDString;
		String				constraintName;
		String				constraintSType;
		String				constraintStateStr;
        boolean             deferrable =
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                ConstraintDefinitionNode.DEFERRABLE_DEFAULT;
        boolean             initiallyDeferred =
                ConstraintDefinitionNode.INITIALLY_DEFERRED_DEFAULT;
        boolean             enforced =
                ConstraintDefinitionNode.ENFORCED_DEFAULT;
		int					referenceCount;
		String				constraintUUIDString;
		String				schemaUUIDString;
		SubConstraintDescriptor scd;

		if (SanityManager.DEBUG)
		{
			if (!(parentTupleDescriptor instanceof SubConstraintDescriptor))
			{
				SanityManager.THROWASSERT(
					"parentTupleDescriptor expected to be instanceof " +
					"SubConstraintDescriptor, not " +
					parentTupleDescriptor.getClass().getName());
			}
		}

		scd = (SubConstraintDescriptor) parentTupleDescriptor;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		constraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);

		/* 2nd column is TABLEID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_TABLEID);
		tableUUIDString = col.getString();
		tableUUID = getUUIDFactory().recreateUUID(tableUUIDString);

		/* Get the TableDescriptor.  
		 * It may be cached in the SCD, 
		 * otherwise we need to go to the
		 * DD.
		 */
		if (scd != null)
		{
			td = scd.getTableDescriptor();
		}
		if (td == null)
		{
			td = dd.getTableDescriptor(tableUUID);
		}

		/* 3rd column is NAME (varchar(128)) */
		col = row.getColumn(SYSCONSTRAINTS_CONSTRAINTNAME);
		constraintName = col.getString();

		/* 4th column is TYPE (char(1)) */
		col = row.getColumn(SYSCONSTRAINTS_TYPE);
		constraintSType = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(constraintSType.length() == 1, 
				"Fourth column type incorrect");
		}

		boolean typeSet = false;
		switch (constraintSType.charAt(0))
		{
			case 'P' : 
				constraintIType = DataDictionary.PRIMARYKEY_CONSTRAINT;
				typeSet = true;
				// fall through

			case 'U' :
				if (! typeSet)
				{
					constraintIType = DataDictionary.UNIQUE_CONSTRAINT;
					typeSet = true;
				}
				// fall through

			case 'F' :
				if (! typeSet)
					constraintIType = DataDictionary.FOREIGNKEY_CONSTRAINT;
				if (SanityManager.DEBUG)
				{
					if (!(parentTupleDescriptor instanceof SubKeyConstraintDescriptor))
					{
						SanityManager.THROWASSERT(
						"parentTupleDescriptor expected to be instanceof " +
						"SubKeyConstraintDescriptor, not " +
						parentTupleDescriptor.getClass().getName());
					}
				}
				conglomDesc = td.getConglomerateDescriptor( 
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getIndexId());
				/* Take care the rare case of conglomDesc being null.  The
				 * reason is that our "td" is out of date.  Another thread
				 * which was adding a constraint committed between the moment
				 * we got the table descriptor (conglomerate list) and the
				 * moment we scanned and got the constraint desc list.  Since
				 * that thread just added a new row to SYSCONGLOMERATES, 
				 * SYSCONSTRAINTS, etc.  We wouldn't have wanted to lock the
				 * system tables just to prevent other threads from adding new
				 * rows.
				 */
				if (conglomDesc == null)
				{
					// we can't be getting td from cache because if we are
					// here, we must have been in dd's ddl mode (that's why
					// the ddl thread went through), we are not done yet, the
					// dd ref count is not 0, hence it couldn't have turned
					// into COMPILE_ONLY mode
					td = dd.getTableDescriptor(tableUUID);
					if (scd != null)
						scd.setTableDescriptor(td);
					// try again now
					conglomDesc = td.getConglomerateDescriptor( 
									((SubKeyConstraintDescriptor) 
										parentTupleDescriptor).getIndexId());
				}

				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(conglomDesc != null,
					"conglomDesc is expected to be non-null for backing index");
				}
				keyColumns = conglomDesc.getIndexDescriptor().baseColumnPositions();
				referencedConstraintId = ((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getKeyConstraintId();
				keyColumns = conglomDesc.getIndexDescriptor().baseColumnPositions();
				break;

			case 'C' :
				constraintIType = DataDictionary.CHECK_CONSTRAINT;
				if (SanityManager.DEBUG)
				{
					if (!(parentTupleDescriptor instanceof SubCheckConstraintDescriptor))
					{
						SanityManager.THROWASSERT("parentTupleDescriptor expected to be instanceof " +
						"SubCheckConstraintDescriptor, not " +
						parentTupleDescriptor.getClass().getName());
					}
				}
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Fourth column value invalid");
				}
		}

		/* 5th column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_SCHEMAID);
		schemaUUIDString = col.getString();
		schemaUUID = getUUIDFactory().recreateUUID(schemaUUIDString);

		schema = dd.getSchemaDescriptor(schemaUUID, null);

		/* 6th column is STATE (char(1)) */
		col = row.getColumn(SYSCONSTRAINTS_STATE);
		constraintStateStr = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(constraintStateStr.length() == 1, 
				"Sixth column (state) type incorrect");
		}

        // Cf. the encoding description in javadoc for
        // #encodeCharacteristics.
        //
        // This code is cloned by DB_Key.makeDeferredClauses().
        //
		switch (constraintStateStr.charAt(0))
		{
			case 'E': 
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                deferrable = false;
                initiallyDeferred = false;
                enforced = true;
				break;
			case 'D':
                deferrable = false;
                initiallyDeferred = false;
                enforced = false;
				break;
            case 'e':
                deferrable = true;
                initiallyDeferred = true;
                enforced = true;
                break;
            case 'd':
                deferrable = true;
                initiallyDeferred = true;
                enforced = false;
                break;
            case 'i':
                deferrable = true;
                initiallyDeferred = false;
                enforced = true;
                break;
            case 'j':
                deferrable = true;
                initiallyDeferred = false;
                enforced = false;
                break;
			default: 
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Invalidate state value '"
							+constraintStateStr+ "' for constraint");
				}
		}

		/* 7th column is REFERENCECOUNT, boolean */
		col = row.getColumn(SYSCONSTRAINTS_REFERENCECOUNT);
		referenceCount = col.getInt();
		
		/* now build and return the descriptor */

		switch (constraintIType)
		{
			case DataDictionary.PRIMARYKEY_CONSTRAINT : 
				constraintDesc = ddg.newPrimaryKeyConstraintDescriptor(
										td, 
										constraintName, 
                                        deferrable,
                                        initiallyDeferred,
										keyColumns,//genReferencedColumns(dd, td), //int referencedColumns[],
										constraintUUID,
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getIndexId(),
										schema,
                                        enforced,
										referenceCount);
				break;

			case DataDictionary.UNIQUE_CONSTRAINT :
				constraintDesc = ddg.newUniqueConstraintDescriptor(
										td, 
										constraintName, 
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                                        deferrable,
                                        initiallyDeferred,
										keyColumns,//genReferencedColumns(dd, td), //int referencedColumns[],
										constraintUUID,
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getIndexId(),
										schema,
                                        enforced,
										referenceCount);
				break;

			case DataDictionary.FOREIGNKEY_CONSTRAINT : 
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(referenceCount == 0, 
						"REFERENCECOUNT column is nonzero for fk constraint");
				}
					
				constraintDesc = ddg.newForeignKeyConstraintDescriptor(
										td, 
										constraintName, 
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                                        deferrable,
                                        initiallyDeferred,
										keyColumns,//genReferencedColumns(dd, td), //int referencedColumns[],
										constraintUUID,
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getIndexId(),
										schema,
										referencedConstraintId,
                                        enforced,
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getRaDeleteRule(),
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getRaUpdateRule()
										);
				break;

			case DataDictionary.CHECK_CONSTRAINT :
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(referenceCount == 0, 
						"REFERENCECOUNT column is nonzero for check constraint");
				}
					
				constraintDesc = ddg.newCheckConstraintDescriptor(
										td, 
										constraintName, 
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                                        deferrable,
                                        initiallyDeferred,
										constraintUUID,
										((SubCheckConstraintDescriptor) 
											parentTupleDescriptor).getConstraintText(),
										((SubCheckConstraintDescriptor) 
											parentTupleDescriptor).getReferencedColumnsDescriptor(),
										schema,
                                        enforced);
				break;
		}
		return constraintDesc;
	}

	/**
	 * Get the constraint ID of the row.
	 * 
	 * @param row	The row from sysconstraints
	 *
	 * @return UUID	The constraint id
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getConstraintId(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				constraintUUIDString;

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		return getUUIDFactory().recreateUUID(constraintUUIDString);
	 }

	/**
	 * Get the constraint name of the row.
	 * 
	 * @param row	The row from sysconstraints
	 *
	 * @return UUID	The constraint name
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected String getConstraintName(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				constraintName;

		/* 3rd column is CONSTRAINTNAME (char(128)) */
		col = row.getColumn(SYSCONSTRAINTS_CONSTRAINTNAME);
		constraintName = col.getString();
		return constraintName;
	 }

	/**
	 * Get the schema ID of the row.
	 * 
	 * @param row	The row from sysconstraints
	 *
	 * @return UUID	The schema
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getSchemaId(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				schemaUUIDString;

		/* 5th column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_SCHEMAID);
		schemaUUIDString =col.getString();
		return getUUIDFactory().recreateUUID(schemaUUIDString);
	 }

	/**
	 * Get the table ID of the row.
	 * 
	 * @param row	The row from sysconstraints
	 *
	 * @return UUID	The table id
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getTableId(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				tableUUIDString;

		/* 2nd column is TABLEID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_TABLEID);
		tableUUIDString = col.getString();
		return getUUIDFactory().recreateUUID(tableUUIDString);
	 }

	/**
	 * Get the constraint type out of the row.
	 * 
	 * @param row	The row from sysconstraints
	 *
	 * @return int	The constraint type	as an int
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected int getConstraintType(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		int					constraintIType;
		String				constraintSType;

		/* 4th column is TYPE (char(1)) */
		col = row.getColumn(SYSCONSTRAINTS_TYPE);
		constraintSType = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(constraintSType.length() == 1, 
				"Fourth column type incorrect");
		}

		switch (constraintSType.charAt(0))
		{
			case 'P' : 
				constraintIType = DataDictionary.PRIMARYKEY_CONSTRAINT;
				break;

			case 'U' :
				constraintIType = DataDictionary.UNIQUE_CONSTRAINT;
				break;

			case 'C' :
				constraintIType = DataDictionary.CHECK_CONSTRAINT;
				break;

			case 'F' :
				constraintIType = DataDictionary.FOREIGNKEY_CONSTRAINT;
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Fourth column value invalid");
				}
				constraintIType = -1;
		}

		return constraintIType;
	 }

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[]	buildColumnList()
	{
            return new SystemColumn[] {
               SystemColumnImpl.getUUIDColumn("CONSTRAINTID", false),
               SystemColumnImpl.getUUIDColumn("TABLEID", false),
               SystemColumnImpl.getIdentifierColumn("CONSTRAINTNAME", false),
               SystemColumnImpl.getIndicatorColumn("TYPE"),
               SystemColumnImpl.getUUIDColumn("SCHEMAID", false),
               SystemColumnImpl.getIndicatorColumn("STATE"),
               SystemColumnImpl.getColumn("REFERENCECOUNT", Types.INTEGER, false) 
            };
	}

}
