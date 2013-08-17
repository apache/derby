/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSUSERSRowFactory

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

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;

import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.UserDescriptor;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * Factory for creating a SYSUSERS row.
 */

public class SYSUSERSRowFactory extends CatalogRowFactory
{
	public static final String	TABLE_NAME = "SYSUSERS";
    public  static  final   String  SYSUSERS_UUID = "9810800c-0134-14a5-40c1-000004f61f90";
    public  static  final   String  PASSWORD_COL_NAME = "PASSWORD";
    
    private static final int		SYSUSERS_COLUMN_COUNT = 4;

	/* Column #s (1 based) */
    public static final int		USERNAME_COL_NUM = 1;
    public static final int		HASHINGSCHEME_COL_NUM = 2;
    public static final int		PASSWORD_COL_NUM = 3;
    public static final int		LASTMODIFIED_COL_NUM = 4;

    static final int		SYSUSERS_INDEX1_ID = 0;

	private static final int[][] indexColumnPositions =
	{
		{USERNAME_COL_NUM},
	};

    private	static	final	boolean[]	uniqueness = null;

	private	static	final	String[]	uuids =
	{
		SYSUSERS_UUID,	// catalog UUID
		"9810800c-0134-14a5-a609-000004f61f90",	// heap UUID
		"9810800c-0134-14a5-f1cd-000004f61f90",	// SYSUSERS_INDEX1
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    SYSUSERSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) 
	{
		super( uuidf, ef, dvf );
		initInfo( SYSUSERS_COLUMN_COUNT, TABLE_NAME, indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

	/**
	 * Make a SYSUSERS row. The password in the UserDescriptor will be zeroed by
     * this method.
	 *
	 * @return	Row suitable for inserting into SYSUSERS
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow( TupleDescriptor td, TupleDescriptor parent )
        throws StandardException
	{
		String  userName = null;
		String  hashingScheme = null;
		char[]  password = null;
		Timestamp   lastModified = null;
		
		ExecRow        			row;

        try {
            if ( td != null )	
            {
                UserDescriptor descriptor = (UserDescriptor) td;
                userName = descriptor.getUserName();
                hashingScheme = descriptor.getHashingScheme();
                password = descriptor.getAndZeroPassword();
                lastModified = descriptor.getLastModified();
            }
	
            /* Build the row to insert  */
            row = getExecutionFactory().getValueRow( SYSUSERS_COLUMN_COUNT );

            /* 1st column is USERNAME (varchar(128)) */
            row.setColumn( USERNAME_COL_NUM, new SQLVarchar( userName ) );

            /* 2nd column is HASHINGSCHEME (varchar(32672)) */
            row.setColumn( HASHINGSCHEME_COL_NUM, new SQLVarchar( hashingScheme ) );

            /* 3rd column is PASSWORD (varchar(32672)) */
            row.setColumn( PASSWORD_COL_NUM, new SQLVarchar( password ) );

            /* 4th column is LASTMODIFIED (timestamp) */
            row.setColumn( LASTMODIFIED_COL_NUM, new SQLTimestamp( lastModified ) );
        }
        finally
        {
            // zero out the password to prevent it from being memory-sniffed
            if ( password != null ) { Arrays.fill( password, (char) 0 ); }
        }

		return row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a descriptor out of a SYSUSERS row. The password column in the
     * row will be zeroed out.
	 *
	 * @param row a row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 *
	 * @return	a descriptor equivalent to a row
	 *
	 * @exception   StandardException thrown on failure
	 */
	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (row.nColumns() != SYSUSERS_COLUMN_COUNT)
			{
				SanityManager.THROWASSERT("Wrong number of columns for a SYSUSERS row: "+
							 row.nColumns());
			}
		}

		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		String	userName;
		String	hashingScheme;
		char[]  password = null;
		Timestamp   lastModified;
		DataValueDescriptor	col;
		SQLVarchar	passwordCol = null;

		UserDescriptor	result;

        try {
            /* 1st column is USERNAME */
            col = row.getColumn( USERNAME_COL_NUM );
            userName = col.getString();

            /* 2nd column is HASHINGSCHEME */
            col = row.getColumn( HASHINGSCHEME_COL_NUM );
            hashingScheme = col.getString();
		
            /* 3nd column is PASSWORD */
            passwordCol = (SQLVarchar) row.getColumn( PASSWORD_COL_NUM );
            password = passwordCol.getRawDataAndZeroIt();

            /* 4th column is LASTMODIFIED */
            col = row.getColumn( LASTMODIFIED_COL_NUM );
            lastModified = col.getTimestamp( new java.util.GregorianCalendar() );

            result = ddg.newUserDescriptor( userName, hashingScheme, password, lastModified );
        }
        finally
        {
            // zero out the password so that it can't be memory-sniffed
            if ( password != null ) { Arrays.fill( password, (char) 0 ); }
            if ( passwordCol != null ) { passwordCol.zeroRawData(); }
        }
        
		return result;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
    public SystemColumn[]   buildColumnList()
        throws StandardException
    {
        return new SystemColumn[]
        {
            SystemColumnImpl.getIdentifierColumn( "USERNAME", false ),
            SystemColumnImpl.getColumn( "HASHINGSCHEME", Types.VARCHAR, false, TypeId.VARCHAR_MAXWIDTH ),
            SystemColumnImpl.getColumn( PASSWORD_COL_NAME, Types.VARCHAR, false, TypeId.VARCHAR_MAXWIDTH ),
            SystemColumnImpl.getColumn( "LASTMODIFIED", Types.TIMESTAMP, false ),
        };
    }
}
