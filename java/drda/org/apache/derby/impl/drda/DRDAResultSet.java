/*

   Derby - Class org.apache.derby.impl.drda.DRDAResultSet

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.drda;

import java.lang.reflect.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.impl.jdbc.EmbedResultSet;
import org.apache.derby.impl.jdbc.EmbedPreparedStatement;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.impl.jdbc.EmbedSQLException;
import org.apache.derby.iapi.reference.SQLState;

/**
	DRDAResultSet holds result set information
*/
class DRDAResultSet
{

	// resultSet states are NOT_OPENED and SUSPENDED
	protected static final int NOT_OPENED = 1;
	protected static final int SUSPENDED = 2;

	boolean explicitlyClosed = false;

	int state;
	protected boolean hasdata = true;
	protected int[] rsLens;				// result length for each column
	private int[] rsDRDATypes;			// DRDA Types of the result set columns
	private int[] rsPrecision;         // result precision for Decimal types
	private int[] rsScale;              // result sale for Decimal types

	protected int [] outovr_drdaType;	// Output override DRDA type and length

	protected int withHoldCursor;			// hold cursor after commit attribute
	protected int scrollType;			// Sensitive or Insensitive scroll attribute
	protected int concurType;			// Concurency type
	protected long rowCount;			// Number of rows we have processed
	private ResultSet rs;              // Current ResultSet

	protected int blksize;				// Query block size
	protected int maxblkext;			// Maximum number of extra blocks
	protected int outovropt;			// Output Override option
	protected int qryclsimp;            // Implicit Query Close Setting
	protected boolean qryrelscr;		// Query relative scrolling
	protected long qryrownbr;			// Query row number
	protected boolean qryrfrtbl;		// Query refresh answer set table
	protected int qryscrorn;			// Query scroll orientation
	protected boolean qryrowsns;		// Query row sensitivity
	protected boolean qryblkrst;		// Query block reset
	protected boolean qryrtndta;		// Query returns data
	protected int qryrowset;			// Query row set
	private   int qryprctyp;			// Protocol type
	private   boolean gotPrctyp;		// save the result, for performance
	protected int rtnextdta;			// Return of EXTDTA option
	protected int nbrrow;			   // number of fetch or insert rows
	protected byte [] rslsetflg;		// Result Set Flags

	private ArrayList  extDtaObjects;  // Arraylist of Blobs and Clobs 
	                                   // Return Values to 
		                               // send with extdta objects.
	
	private ArrayList rsExtPositions;

	protected String pkgcnstknStr;               // Unique consistency token for ResultSet 0



	protected DRDAResultSet(ResultSet rs) throws SQLException
	{
		setResultSet(rs);
		state = NOT_OPENED;
	}

	protected DRDAResultSet()
	{
		state = NOT_OPENED;
	}

	/**
 	 * Set result set and initialize type array.
	 *
	 * @param value
	 * 
	 */

	protected void setResultSet(ResultSet value) throws SQLException
	{
		int numCols;
		rs = value;
		gotPrctyp = false;
		if (value != null)
		{
		    numCols= rs.getMetaData().getColumnCount();
			rsDRDATypes = new int[numCols];
		}
		explicitlyClosed = false;
	}


	/**
	 * set consistency token for this resultSet
	 *
	 */
	protected void setPkgcnstknStr(String pkgcnstknStr)
	{
		this.pkgcnstknStr = pkgcnstknStr;
	}


	/**
	 * 
	 *  @return the underlying java.sql.ResultSet
	 */
	protected ResultSet getResultSet()
	{
		return rs;
	}

	/** 
	 * Set ResultSet DRDA DataTypes
	 * @param drddaTypes for columns.
	 **/
	protected void setRsDRDATypes(int [] value)
	{
		rsDRDATypes = value;

	}

	/**
	 *@return ResultSet DRDA DataTypes
	 **/

	protected int[] getRsDRDATypes()
	{
		// use the given override if it is present
		if (outovr_drdaType != null)
			return outovr_drdaType;
		return rsDRDATypes;
	}

	/**
	 * set resultset/out parameter precision
	 *
	 * @param index - starting with 1
	 * @param precision
	 */
	protected void setRsPrecision(int index, int precision)
	{
		if (rsPrecision == null)
			rsPrecision = new int[rsDRDATypes.length];
		rsPrecision[index -1] = precision;
	}

	/**
	 * get resultset /out paramter precision
	 * @param index -starting with 1
	 * @return precision of column
	 */
	protected int getRsPrecision(int index)
	{
		if (rsPrecision == null)
			return 0;
		return rsPrecision[index-1];
	}

	/**
	 * set resultset/out parameter scale
	 *
	 * @param index - starting with 1
	 * @param scale
	 */
	protected void setRsScale(int index, int scale)
	{
		if (rsScale == null)
			rsScale = new int[rsDRDATypes.length];
		rsScale[index-1] = scale;
	}

	/**
	 * get resultset /out paramter scale
	 * @param index -starting with 1
	 * @return scale of column
	 */
	protected int  getRsScale(int index)
	{
		if (rsScale == null)
			return 0;
		
		return rsScale[index -1];
	}
	
	
	/**
	 * set resultset/out parameter DRDAType
	 *
	 * @param index - starting with 1
	 * @param type
	 */
	protected  void setRsDRDAType(int index, int type)
	{
		rsDRDATypes[index -1] =  type;
		
	}
	
	/**
	 * get  resultset/out parameter DRDAType
	 *
	 * @param index - starting with 1
	 * @return  DRDA Type of column
	 */
	protected int getRsDRDAType(int index)
	{
		return rsDRDATypes[index -1];
	}
	

	/**
	 * set resultset DRDA Len
	 *
	 * @param index - starting with 1
	 * @param value
	 */
	protected  void setRsLen(int index, int value)
	{
		if (rsLens == null)
			rsLens = new int[rsDRDATypes.length];
		rsLens[index -1] = value;
		
	}
	
	/**
	 * get  resultset  DRDALen
	 * @param index - starting with 1
	 * @return  length of column value
	 */
	protected int getRsLen(int index)
	{
		return rsLens[index -1];
	}
	

	/**
	 * Add extDtaObject
	 * @param o - object to  add
	 */
	protected void  addExtDtaObject (Object o, int jdbcIndex )
	{
		if (extDtaObjects == null)
			extDtaObjects = new java.util.ArrayList();
		extDtaObjects.add (o);

		if (rsExtPositions == null)
			rsExtPositions = new java.util.ArrayList();
		
		// need to record the 0 based position so subtract 1
		rsExtPositions.add (new Integer(jdbcIndex -1 ));

	}


	/**
	 * Clear externalized lob objects in current result set
	 */
	protected void  clearExtDtaObjects ()
	{
		if (extDtaObjects != null)
			extDtaObjects.clear();
		if (rsExtPositions != null)
			rsExtPositions.clear();
		
	}
	
	/*
	 * Is lob object nullable
	 * @param index - offset starting with 0
	 * @return true if object is nullable
	 */
	protected boolean isExtDtaValueNullable(int index)
	{
		if ((rsExtPositions == null) || 
			rsExtPositions.get(index) == null)
			return false;
		

		int colnum = ((Integer) rsExtPositions.get(index)).intValue();
		
		if (FdocaConstants.isNullable((getRsDRDATypes())[colnum]))
			return true;
		else 
			return false;
	}
	

	/**
	 * Get the extData Objects
	 *
	 *  @return ArrayList with extdta
	 */
	protected ArrayList getExtDtaObjects()
	{
		return extDtaObjects;
	}

	/**
	 * Set the extData Objects
	 *
	 *  @return ArrayList with extdta
	 */
	protected void  setExtDtaObjects(ArrayList a)
	{
		extDtaObjects =a;
	}
	
	
	/** Clean up statements and resultSet
	 * 
	 */
	protected void close()  throws SQLException
	{
		if (rs != null)
			rs.close();
		rs = null;
		gotPrctyp = false;
		outovr_drdaType = null;
		scrollType = 0;
		concurType = 0;
		rowCount = 0;
		rsLens = null;
		rsDRDATypes = null;
		rsPrecision = null;
		rsScale = null;
		extDtaObjects = null;
		rsExtPositions = null;
		state=NOT_OPENED;
		hasdata = true;
	}


	/**
	 * Explicitly close the result set by CLSQRY
	 * needed to check for double close.
	 */
	protected void CLSQRY()
	{
		explicitlyClosed = true;
	}

	/* 
	 * @return whether CLSQRY has been called on the
	 *         current result set.
	 */
	protected boolean wasExplicitlyClosed()
	{
		return explicitlyClosed;
	}


	/****
	 * Check to see if the result set for this statement
	 * has at least one column that is BLOB/CLOB.
	 * @return True if the result has at least one blob/clob
	 *  column; false otherwise.
	 ****/
 
	private boolean hasLobColumns()	throws SQLException
	{
		ResultSetMetaData rsmd = rs.getMetaData();
		int ncols = rsmd.getColumnCount();
		for (int i = 1; i <= ncols; i++)
		{
			int type = rsmd.getColumnType(i);
			if (type == Types.BLOB || type == Types.CLOB)
				return true;
		}
		return false;
	}

	/**
	 * Get the cursor name for the ResultSet
	 */
	public String getResultSetCursorName() throws SQLException
	{

		if (rs != null)
			return rs.getCursorName();
		else 
			return null;
	}

	protected int getQryprctyp()
		throws SQLException
	{
		if (!gotPrctyp && qryprctyp == CodePoint.LMTBLKPRC)
		{
			gotPrctyp = true;
			if (rs == null || ((EmbedResultSet) rs).isForUpdate() ||
				/* for now we are not supporting LOB under LMTBLKPRC.  drda spec only
				 * disallows LOB under LMTBLKPRC if OUTOVR is also for ANY CNTQRY reply.
				 * To support LOB, QRYDTA protocols for LOB will need to be changed.
				 */
				hasLobColumns())
			{
				qryprctyp = CodePoint.FIXROWPRC;
			}
		}
		return qryprctyp;
	}

	protected void setQryprctyp(int qryprctyp)
	{
		this.qryprctyp = qryprctyp;
	}

	/**
	 * is ResultSet closed
	 * @return whether the resultSet  is closed
	 */
	protected boolean isClosed()
	{
		return (state == NOT_OPENED);
	}

	/**
	 * Set state to SUSPENDED (result set is opened)
	 */
	protected void suspend()
	{
		state = SUSPENDED;
	}


	protected String toDebugString(String indent)
	{
		String s = indent + "***** DRDASResultSet toDebugString ******\n";
		s += indent + "State:" + getStateString(state)+ "\n";
		s += indent + "pkgcnstknStr: {" +pkgcnstknStr  + "}\n"; 
		s += indent + "cursor Name: ";
		String cursorName = null;
		try {
			if (rs != null)
				cursorName = rs.getCursorName();
		}
		catch (SQLException se )
		{
			cursorName = "invalid rs";
		}
		s += indent + cursorName + "\n";
		   
		return s;
	}


	private String getStateString( int i )
	{
		switch (i)
		{
			case NOT_OPENED:
				return "NOT_OPENED";
			case SUSPENDED:
				return "SUSPENDED";
			default:
				return "UNKNOWN_STATE";
		}

	}
}
	

