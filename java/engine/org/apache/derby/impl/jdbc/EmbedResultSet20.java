/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedResultSet20

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

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.reference.JDBC20Translation;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;

/* ---- New jdbc 2.0 types ----- */
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Ref;

import java.math.BigDecimal;
import java.net.URL;

/**
 * This class extends the EmbedResultSet class in order to support new
 * methods and classes that come with JDBC 2.0.
 *
 *      @see org.apache.derby.impl.jdbc.EmbedResultSet
 *
 *      @author francois
 */

public class EmbedResultSet20
        extends org.apache.derby.impl.jdbc.EmbedResultSet {

        private int fetchDirection ;
        private int fetchSize  ;

        //////////////////////////////////////////////////////////////
        //
        // CONSTRUCTORS
        //
        //////////////////////////////////////////////////////////////

        /**
         * This class provides the glue between the Cloudscape
         * resultset and the JDBC resultset, mapping calls-to-calls.
         */
        public EmbedResultSet20(org.apache.derby.impl.jdbc.EmbedConnection conn, 
                                                         ResultSet resultsToWrap,  
                                                         boolean forMetaData,
                                                         org.apache.derby.impl.jdbc.EmbedStatement stmt,
                                                         boolean isAtomic)  
        {
                super(conn, resultsToWrap, forMetaData, stmt, isAtomic);
        }


		/*
		** Methods using java.math.BigDecimal, not supported in JSR169
		*/
		/**
		 * Get the value of a column in the current row as a java.lang.BigDecimal object.
		 *
		 * @param columnIndex the first column is 1, the second is 2, ...
		 * @param scale the number of digits to the right of the decimal
		 * @return the column value; if the value is SQL NULL, the result is null
		 * @exception SQLException thrown on failure.
		 */
		public final BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {

			BigDecimal ret = getBigDecimal(columnIndex);
			if (ret != null) {
				return ret.setScale(scale, BigDecimal.ROUND_HALF_DOWN);
			}
			return null;
		}

		public final BigDecimal getBigDecimal(int columnIndex)
			throws SQLException {

			try {

				DataValueDescriptor dvd = getColumn(columnIndex);

				if (wasNull = dvd.isNull())
					return null;

				return dvd.getBigDecimal();

			} catch (StandardException t) {
				throw noStateChangeException(t);
			}
		}

		/**
		 * Get the value of a column in the current row as a java.lang.BigDecimal object.
		 *
		 * @param columnName is the SQL name of the column
		 * @param scale the number of digits to the right of the decimal
		 * @return the column value; if the value is SQL NULL, the result is null
		 * @exception SQLException thrown on failure.
		 */
		public final BigDecimal getBigDecimal(String columnName, int scale)
			throws SQLException {
			return (getBigDecimal(findColumnName(columnName), scale));
		}

        /////////////////////////////////////////////////////////////////////////
        //
        //      JDBC 2.0        -       New public methods
        //
        /////////////////////////////////////////////////////////////////////////


    //---------------------------------------------------------------------
    // Getter's and Setter's
    //---------------------------------------------------------------------

    /**
     * JDBC 2.0
     *
     * Return the Statement that produced the ResultSet.
     *
     * @return the Statment that produced the result set, or
     * null if the result was produced some other way.
     */
    public final Statement getStatement()
        {
                return stmt;
        }

    /**
     * JDBC 2.0
     *
		Deprecated in JDBC 2.0, not supported by JCC.
	 * @exception SQLException thrown on failure.
     */
    public final java.io.InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw Util.notImplemented("getUnicodeStream");
	}
    /**
		Deprecated in JDBC 2.0, not supported by JCC.
	 * @exception SQLException thrown on failure.
     */
    public final java.io.InputStream getUnicodeStream(String columnName) throws SQLException {
		throw Util.notImplemented("getUnicodeStream");
	}	
	
    /**
     * JDBC 2.0
     *
     * Get the value of a column in the current row as a java.math.BigDecimal 
     * object.
     *
         * @exception SQLException Feature not implemented for now.
     */
    public BigDecimal getBigDecimal(String columnName) throws SQLException {
                        return getBigDecimal(findColumnName(columnName));
        }

    //---------------------------------------------------------------------
    // Traversal/Positioning
    //---------------------------------------------------------------------

    /**
     * JDBC 2.0
     *
     * <p>Determine if the cursor is before the first row in the result 
     * set.   
     *
     * @return true if before the first row, false otherwise. Returns
     * false when the result set contains no rows.
         * @exception SQLException Thrown on error.
     */
    public boolean isBeforeFirst() throws SQLException 
        {
                return checkRowPosition(ResultSet.ISBEFOREFIRST, "isBeforeFirst");
        }
      
    /**
     * JDBC 2.0
     *
     * <p>Determine if the cursor is after the last row in the result 
     * set.   
     *
     * @return true if after the last row, false otherwise.  Returns
     * false when the result set contains no rows.
         * @exception SQLException Thrown on error.
     */
    public boolean isAfterLast() throws SQLException 
        {
                return checkRowPosition(ResultSet.ISAFTERLAST, "isAfterLast");
        }
 
    /**
     * JDBC 2.0
     *
     * <p>Determine if the cursor is on the first row of the result set.   
     *
     * @return true if on the first row, false otherwise.   
         * @exception SQLException Thrown on error.
     */
    public boolean isFirst() throws SQLException 
        {
                return checkRowPosition(ResultSet.ISFIRST, "isFirst");
        }
 
    /**
     * JDBC 2.0
     *
     * <p>Determine if the cursor is on the last row of the result set.   
     * Note: Calling isLast() may be expensive since the JDBC driver
     * might need to fetch ahead one row in order to determine 
     * whether the current row is the last row in the result set.
     *
     * @return true if on the last row, false otherwise. 
         * @exception SQLException Thrown on error.
     */
    public boolean isLast() throws SQLException 
        {
                return checkRowPosition(ResultSet.ISLAST, "isLast");
        }

    /**
     * JDBC 2.0
     *
     * <p>Moves to the front of the result set, just before the
     * first row. Has no effect if the result set contains no rows.
     *
     * @exception SQLException if a database-access error occurs, or
     * result set type is TYPE_FORWARD_ONLY
     */
    public void beforeFirst() throws SQLException {
                // beforeFirst is only allowed on scroll cursors
                checkScrollCursor("beforeFirst()");
                movePosition(BEFOREFIRST, "beforeFirst");
        }

    /**
     * JDBC 2.0
     *
     * <p>Moves to the end of the result set, just after the last
     * row.  Has no effect if the result set contains no rows.
     *
     * @exception SQLException if a database-access error occurs, or
     * result set type is TYPE_FORWARD_ONLY.
     */
    public void afterLast() throws SQLException {
                // afterLast is only allowed on scroll cursors
                checkScrollCursor("afterLast()");
                movePosition(AFTERLAST, "afterLast");
        }

    /**
     * JDBC 2.0
     *
     * <p>Moves to the first row in the result set.  
     *
     * @return true if on a valid row, false if no rows in the result set.
     * @exception SQLException if a database-access error occurs, or
     * result set type is TYPE_FORWARD_ONLY.
     */
    public boolean first() throws SQLException 
        {
                // first is only allowed on scroll cursors
                checkScrollCursor("first()");
                return movePosition(FIRST, "first");
        }

    /**
     * JDBC 2.0
     *
     * <p>Moves to the last row in the result set.  
     *
     * @return true if on a valid row, false if no rows in the result set.
     * @exception SQLException if a database-access error occurs, or
     * result set type is TYPE_FORWARD_ONLY.
     */
    public boolean last() throws SQLException 
        {
                // last is only allowed on scroll cursors
                checkScrollCursor("last()");
                return movePosition(LAST, "last");
        }

    /**
     * JDBC 2.0
     *
     * <p>Determine the current row number.  The first row is number 1, the
     * second number 2, etc.  
     *
     * @return the current row number, else return 0 if there is no 
     * current row
     * @exception SQLException if a database-access error occurs.
     */
    public int getRow() throws SQLException 
        {
                // getRow() is only allowed on scroll cursors
                checkScrollCursor("getRow()");

                /* 
                ** We probably needn't bother getting the text of
                ** the underlying statement but it is better to be 
                ** consistent and we aren't particularly worried 
                ** about performance of getRow().  
                */
                return theResults.getRowNumber();
        }

    /**
     * JDBC 2.0
     *
     * <p>Move to an absolute row number in the result set.
     *
     * <p>If row is positive, moves to an absolute row with respect to the
     * beginning of the result set.  The first row is row 1, the second
     * is row 2, etc. 
     *
     * <p>If row is negative, moves to an absolute row position with respect to
     * the end of result set.  For example, calling absolute(-1) positions the 
     * cursor on the last row, absolute(-2) indicates the next-to-last
     * row, etc.
     *
     * <p>An attempt to position the cursor beyond the first/last row in
     * the result set, leaves the cursor before/after the first/last
     * row, respectively.
     *
     * <p>Note: Calling absolute(1) is the same as calling first().
     * Calling absolute(-1) is the same as calling last().
     *
     * @return true if on the result set, false if off.
     * @exception SQLException if a database-access error occurs, or 
     * row is 0, or result set type is TYPE_FORWARD_ONLY.
     */
    public boolean absolute( int row ) throws SQLException 
        {
                // absolute is only allowed on scroll cursors
                checkScrollCursor("absolute()");
                return movePosition(ABSOLUTE, row, "absolute");
        }

    /**
     * JDBC 2.0
     *
     * <p>Moves a relative number of rows, either positive or negative.
     * Attempting to move beyond the first/last row in the
     * result set positions the cursor before/after the
     * the first/last row. Calling relative(0) is valid, but does
     * not change the cursor position.
     *
     * <p>Note: Calling relative(1) is different than calling next()
     * since is makes sense to call next() when there is no current row,
     * for example, when the cursor is positioned before the first row
     * or after the last row of the result set.
     *
     * @return true if on a row, false otherwise.
     * @exception SQLException if a database-access error occurs, or there
     * is no current row, or result set type is TYPE_FORWARD_ONLY.
     */
    public boolean relative( int row ) throws SQLException
        {
                // absolute is only allowed on scroll cursors
                checkScrollCursor("relative()");
                return movePosition(RELATIVE, row, "relative");
        }

    /**
     * JDBC 2.0
     *
     * <p>Moves to the previous row in the result set.  
     *
     * <p>Note: previous() is not the same as relative(-1) since it
     * makes sense to call previous() when there is no current row.
     *
     * @return true if on a valid row, false if off the result set.
     * @exception SQLException if a database-access error occurs, or
     * result set type is TYPE_FORWAR_DONLY.
     */
    public boolean previous() throws SQLException 
        {
                // previous is only allowed on scroll cursors
                checkScrollCursor("previous()");
                return movePosition(PREVIOUS, "previous");
        }

    //---------------------------------------------------------------------
    // Properties
    //---------------------------------------------------------------------

    /**
     * JDBC 2.0
     *
     * Give a hint as to the direction in which the rows in this result set
     * will be processed.  The initial value is determined by the statement
     * that produced the result set.  The fetch direction may be changed
     * at any time.
     *
     * @exception SQLException if a database-access error occurs, or
     * the result set type is TYPE_FORWARD_ONLY and direction is not 
     * FETCH_FORWARD.
     */
    public void setFetchDirection(int direction) throws SQLException 
        {
                checkScrollCursor("setFetchDirection()");
                /* FetchDirection is meaningless to us.  We
                 * just save it off and return the current
                 * value if asked.
                 */
                    fetchDirection = direction;
        }

    /**
     * JDBC 2.0
     *
     * Return the fetch direction for this result set.
     *
     * @exception SQLException if a database-access error occurs 
     */
    public int getFetchDirection() throws SQLException 
        {
                if (fetchDirection == 0){
                    // value is not set at the result set level
                    // get it from the statement level
                    return stmt.getFetchDirection();
                }
                else
                    return fetchDirection;
        }

    /**
     * JDBC 2.0
     *
     * Give the JDBC driver a hint as to the number of rows that should 
     * be fetched from the database when more rows are needed for this result
     * set.  If the fetch size specified is zero, then the JDBC driver 
     * ignores the value, and is free to make its own best guess as to what
     * the fetch size should be.  The default value is set by the statement 
     * that creates the result set.  The fetch size may be changed at any 
     * time.
     *
     * @param rows the number of rows to fetch
     * @exception SQLException if a database-access error occurs, or the
     * condition 0 <= rows <= this.getMaxRows() is not satisfied.
     */
    public void setFetchSize(int rows) throws SQLException 
        {
                if (rows < 0 || (stmt.getMaxRows()!=0 &&
                                rows > stmt.getMaxRows())) 
                {
                        throw Util.generateCsSQLException(
                  SQLState.INVALID_FETCH_SIZE, new Integer(rows));
                }
                else  
                    if (rows > 0)  // if it is zero ignore the call
                    {
                        fetchSize = rows;
                    }
        }

    /**
     * JDBC 2.0
     *
     * Return the fetch size for this result set.
     *
     * @exception SQLException if a database-access error occurs 
     */
    public int getFetchSize() throws SQLException 
        {
                if (fetchSize == 0) 
                {
                 // value is not set at the result set level
                //  get the default value from the statement 
                    return stmt.getFetchSize();
                }else
                    return fetchSize;
        }

    /**
     * JDBC 2.0
     *
     * Return the type of this result set.  The type is determined based
     * on the statement that created the result set.
     *
     * @return TYPE_FORWARD_ONLY, TYPE_SCROLL_INSENSITIVE, or
         * TYPE_SCROLL_SENSITIVE
     * @exception SQLException if a database-access error occurs
     */
    public int getType() throws SQLException 
        {
                return stmt.getResultSetType();
        }

    /**
     * JDBC 2.0
     *
     * Return the concurrency of this result set.  The concurrency
     * used is determined by the statement that created the result set.
     *
     * @return the concurrency type, CONCUR_READ_ONLY, etc.
     * @exception SQLException if a database-access error occurs
     */
    public int getConcurrency() throws SQLException 
        {
                return JDBC20Translation.CONCUR_READ_ONLY;
        }

    //---------------------------------------------------------------------
    // Updates
    //---------------------------------------------------------------------

    /**
     * JDBC 2.0
     *
     * Determine if the current row has been updated.  The value returned 
     * depends on whether or not the result set can detect updates.
     *
     * @return true if the row has been visibly updated by the owner or
     * another, and updates are detected
     * @exception SQLException if a database-access error occurs
     * 
     * @see EmbedDatabaseMetaData#updatesAreDetected
     */
    public boolean rowUpdated() throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * Determine if the current row has been inserted.  The value returned 
     * depends on whether or not the result set can detect visible inserts.
     *
     * @return true if inserted and inserts are detected
     * @exception SQLException if a database-access error occurs
     * 
     * @see EmbedDatabaseMetaData#insertsAreDetected
     */
    public boolean rowInserted() throws SQLException {
                throw Util.notImplemented();
        }
   
    /**
     * JDBC 2.0
     *
     * Determine if this row has been deleted.  A deleted row may leave
     * a visible "hole" in a result set.  This method can be used to
     * detect holes in a result set.  The value returned depends on whether 
     * or not the result set can detect deletions.
     *
     * @return true if deleted and deletes are detected
     * @exception SQLException if a database-access error occurs
     * 
     * @see EmbedDatabaseMetaData#deletesAreDetected
     */
    public boolean rowDeleted() throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     * 
     * Give a nullable column a null value.
     * 
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @exception SQLException if a database-access error occurs
     */
    public void updateNull(int columnIndex) throws SQLException {  
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     * 
     * Update a column with a boolean value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *   
     * Update a column with a byte value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateByte(int columnIndex, byte x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *   
     * Update a column with a short value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateShort(int columnIndex, short x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *   
     * Update a column with an integer value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateInt(int columnIndex, int x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *   
     * Update a column with a long value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateLong(int columnIndex, long x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a float value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateFloat(int columnIndex, float x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Double value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateDouble(int columnIndex, double x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a BigDecimal value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateBigDecimal(int columnIndex, BigDecimal x)
    throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a String value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateString(int columnIndex, String x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a byte array value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateBytes(int columnIndex, byte x[]) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Date value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateDate(int columnIndex, java.sql.Date x)
    throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Time value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateTime(int columnIndex, java.sql.Time x)
    throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Timestamp value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateTimestamp(int columnIndex, java.sql.Timestamp x)
      throws SQLException {
                throw Util.notImplemented();
        }

    /** 
     * JDBC 2.0
     *  
     * Update a column with an ascii stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @param length the length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public void updateAsciiStream(int columnIndex, 
                           java.io.InputStream x, 
                           int length) throws SQLException {
                throw Util.notImplemented();
        }

    /** 
     * JDBC 2.0
     *  
     * Update a column with a binary stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value     
     * @param length the length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public void updateBinaryStream(int columnIndex, 
                            java.io.InputStream x,
                            int length) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a character stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @param length the length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public void updateCharacterStream(int columnIndex,
                             java.io.Reader x,
                             int length) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with an Object value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @param scale For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
     *  this is the number of digits after the decimal.  For all other
     *  types this value will be ignored.
     * @exception SQLException if a database-access error occurs
     */
    public void updateObject(int columnIndex, Object x, int scale)
      throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with an Object value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateObject(int columnIndex, Object x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a null value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @exception SQLException if a database-access error occurs
     */
    public void updateNull(String columnName) throws SQLException {  
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a boolean value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateBoolean(String columnName, boolean x)
    throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a byte value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateByte(String columnName, byte x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a short value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateShort(String columnName, short x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with an integer value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateInt(String columnName, int x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a long value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateLong(String columnName, long x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a float value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateFloat(String columnName, float x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a double value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateDouble(String columnName, double x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a BigDecimal value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateBigDecimal(String columnName, BigDecimal x)
    throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a String value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateString(String columnName, String x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a byte array value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateBytes(String columnName, byte x[]) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Date value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateDate(String columnName, java.sql.Date x)
    throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Time value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateTime(String columnName, java.sql.Time x)
    throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Timestamp value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateTimestamp(String columnName, java.sql.Timestamp x)
      throws SQLException {
                throw Util.notImplemented();
        }

    /** 
     * JDBC 2.0
     *  
     * Update a column with an ascii stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public void updateAsciiStream(String columnName, 
                           java.io.InputStream x, 
                           int length) throws SQLException {
                throw Util.notImplemented();
        }

    /** 
     * JDBC 2.0
     *  
     * Update a column with a binary stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public void updateBinaryStream(String columnName, 
                            java.io.InputStream x,
                            int length) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with a character stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public void updateCharacterStream(String columnName,
                             java.io.Reader reader,
                             int length) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with an Object value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param scale For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
     *  this is the number of digits after the decimal.  For all other
     *  types this value will be ignored.
     * @exception SQLException if a database-access error occurs
     */
    public void updateObject(String columnName, Object x, int scale)
      throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *  
     * Update a column with an Object value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public void updateObject(String columnName, Object x) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * Insert the contents of the insert row into the result set and
     * the database.  Must be on the insert row when this method is called.
     *
     * @exception SQLException if a database-access error occurs,
     * if called when not on the insert row, or if all non-nullable columns in
     * the insert row have not been given a value
     */
    public void insertRow() throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * Update the underlying database with the new contents of the
     * current row.  Cannot be called when on the insert row.
     *
     * @exception SQLException if a database-access error occurs, or
     * if called when on the insert row
     */
    public void updateRow() throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * Delete the current row from the result set and the underlying
     * database.  Cannot be called when on the insert row.
     *
     * @exception SQLException if a database-access error occurs, or if
     * called when on the insert row.
     */
    public void deleteRow() throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * Refresh the value of the current row with its current value in 
     * the database.  Cannot be called when on the insert row.
     *
     * The refreshRow() method provides a way for an application to 
     * explicitly tell the JDBC driver to refetch a row(s) from the
     * database.  An application may want to call refreshRow() when 
     * caching or prefetching is being done by the JDBC driver to
     * fetch the latest value of a row from the database.  The JDBC driver 
     * may actually refresh multiple rows at once if the fetch size is 
     * greater than one.
     * 
     * All values are refetched subject to the transaction isolation 
     * level and cursor sensitivity.  If refreshRow() is called after
     * calling updateXXX(), but before calling updateRow() then the
     * updates made to the row are lost.  Calling refreshRow() frequently
     * will likely slow performance.
     *
     * @exception SQLException if a database-access error occurs, or if
     * called when on the insert row.
     */
    public void refreshRow() throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * The cancelRowUpdates() method may be called after calling an
     * updateXXX() method(s) and before calling updateRow() to rollback 
     * the updates made to a row.  If no updates have been made or 
     * updateRow() has already been called, then this method has no 
     * effect.
     *
     * @exception SQLException if a database-access error occurs, or if
     * called when on the insert row.
     *
     */
    public void cancelRowUpdates () throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * Move to the insert row.  The current cursor position is 
     * remembered while the cursor is positioned on the insert row.
     *
     * The insert row is a special row associated with an updatable
     * result set.  It is essentially a buffer where a new row may
     * be constructed by calling the updateXXX() methods prior to 
     * inserting the row into the result set.  
     *
     * Only the updateXXX(), getXXX(), and insertRow() methods may be 
     * called when the cursor is on the insert row.  All of the columns in 
     * a result set must be given a value each time this method is
     * called before calling insertRow().  UpdateXXX()must be called before
     * getXXX() on a column.
     *
     * @exception SQLException if a database-access error occurs,
     * or the result set is not updatable
     */
    public void moveToInsertRow() throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * Move the cursor to the remembered cursor position, usually the
     * current row.  Has no effect unless the cursor is on the insert 
     * row. 
     *
     * @exception SQLException if a database-access error occurs,
     * or the result set is not updatable
     */
    public void moveToCurrentRow() throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * Returns the value of column @i as a Java object.  Use the 
     * param map to determine the class from which to construct data of 
     * SQL structured and distinct types.
     *
     * @param i the first column is 1, the second is 2, ...
     * @param map the mapping from SQL type names to Java classes
     * @return an object representing the SQL value
         * @exception SQLException Feature not implemented for now.
     */
    public Object getObject(int columnIndex, java.util.Map map) throws SQLException {
        if( map == null)
            throw Util.generateCsSQLException(SQLState.INVALID_API_PARAMETER,map,"map",
                                              "java.sql.ResultSet.getObject");
        if(!(map.isEmpty()))
            throw Util.notImplemented();
        // Map is empty call the normal getObject method.
        return getObject(columnIndex);
        }

    /**
     * JDBC 2.0
     *
     * Get a REF(&lt;structured-type&gt;) column.
     *
     * @param i the first column is 1, the second is 2, ...
     * @return an object representing data of an SQL REF type
         * @exception SQLException Feature not implemented for now.
     */
    public Ref getRef(int i) throws SQLException {
                throw Util.notImplemented();
        }


    /**
     * JDBC 2.0
     *
     * Get a BLOB column.
     *
     * @param i the first column is 1, the second is 2, ...
     * @return an object representing a BLOB
     */
    public Blob getBlob(int columnIndex)
        throws SQLException
    {
        
        closeCurrentStream();   // closing currentStream does not depend on the
        // underlying connection.  Do this outside of
        // the connection synchronization.
        
        checkIfClosed("getBlob");       // checking result set closure does not depend
        // on the underlying connection.  Do this
        // outside of the connection synchronization.
        
        synchronized (getConnectionSynchronization())
        {
            int colType = getColumnType(columnIndex);

			// DB2, only allow getBlob on a BLOB column.
			if (colType != Types.BLOB)
				throw dataTypeConversion("java.sql.Blob", columnIndex);

            boolean pushStack = false;
            try
            {
                DataValueDescriptor dvd = currentRow.getColumn(columnIndex);

                if (wasNull = dvd.isNull())
                    return null;
                                
                // should set up a context stack if we have a long column,
                // since a blob may keep a pointer to a long column in the database
                if (dvd.getStream() != null)
                    pushStack = true;

                if (pushStack)
                    setupContextStack();

                return new EmbedBlob(dvd,getEmbedConnection());
            }
            catch (Throwable t)
            {
                throw handleException(t);
            }
            finally
            {
                if (pushStack)
                    restoreContextStack();
            }
        }
    }


    /**
     * JDBC 2.0
     *
     * Get a CLOB column.
     *
     * @param i the first column is 1, the second is 2, ...
     * @return an object representing a CLOB
     */
    public final Clob getClob(int columnIndex) throws SQLException
    {

        closeCurrentStream();   // closing currentStream does not depend on the
                                                        // underlying connection.  Do this outside of
                                                        // the connection synchronization.

        checkIfClosed("getClob");       // checking result set closure does not depend
                                                        // on the underlying connection.  Do this
                                                        // outside of the connection synchronization.


        synchronized (getConnectionSynchronization())
		{
            int colType = getColumnType(columnIndex);

			// DB2:, only allow getClob on a CLOB column.
			if (colType != Types.CLOB)
                throw dataTypeConversion("java.sql.Clob", columnIndex);


            boolean pushStack = false;
            try
            {

				DataValueDescriptor dvd = currentRow.getColumn(columnIndex);

                if (wasNull = dvd.isNull())
                        return null;


                // should set up a context stack if we have a long column,
                // since a blob may keep a pointer to a long column in the database
                if (dvd.getStream() != null)
                    pushStack = true;

                if (pushStack)
                    setupContextStack();

                return new EmbedClob(dvd,getEmbedConnection());
            }
            catch (Throwable t)
            {
                  throw handleException(t);
            }
            finally
            {
               if (pushStack)
                    restoreContextStack();
             }
         }
     }



    /**
     * JDBC 2.0
     *
     * Get an array column.
     *
     * @param i the first column is 1, the second is 2, ...
     * @return an object representing an SQL array
         * @exception SQLException Feature not implemented for now.
     */
    public Array getArray(int i) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * Returns the value of column @i as a Java object.  Use the 
     * param map to determine the class from which to construct data of 
     * SQL structured and distinct types.
     *
     * @param colName the column name
     * @param map the mapping from SQL type names to Java classes
     * @return an object representing the SQL value
         * @exception SQLException Feature not implemented for now.
     */
    public Object getObject(String colName, java.util.Map map)
    throws SQLException {
        return getObject(findColumn(colName),map);
        }

    /**
     * JDBC 2.0
     *
     * Get a REF(&lt;structured-type&gt;) column.
     *
     * @param colName the column name
     * @return an object representing data of an SQL REF type
         * @exception SQLException Feature not implemented for now.
     */
    public Ref getRef(String colName) throws SQLException {
                throw Util.notImplemented();
        }

    /**
     * JDBC 2.0
     *
     * Get a BLOB column.
     *
     * @param colName the column name
     * @return an object representing a BLOB
     */
    public final Blob getBlob(String columnName)
        throws SQLException
    {
            return (getBlob(findColumnName(columnName)));
        }


    /**
     * JDBC 2.0
     *
     * Get a CLOB column.
     *
     * @param colName the column name
     * @return an object representing a CLOB
         * @exception SQLException Feature not implemented for now.
     */
    public final Clob getClob(String columnName) throws SQLException
    {
            return (getClob(findColumnName(columnName)));
        }


    /**
     * JDBC 2.0
     *
     * Get an array column.
     *
     * @param colName the column name
     * @return an object representing an SQL array
         * @exception SQLException Feature not implemented for now.
     */
    public Array getArray(String colName) throws SQLException {
                throw Util.notImplemented();
        }
        private void checkScrollCursor(String methodName) throws SQLException {

                if (stmt.getResultSetType() == JDBC20Translation.TYPE_FORWARD_ONLY)
                        throw  Util.newEmbedSQLException(SQLState.NOT_ON_FORWARD_ONLY_CURSOR,
						new Object[] {methodName},
                StandardException.getSeverityFromIdentifier(SQLState.NOT_ON_FORWARD_ONLY_CURSOR));
        }

        /**
    Following methods are for the new JDBC 3.0 methods in java.sql.ResultSet
    (see the JDBC 3.0 spec). We have the JDBC 3.0 methods in Local20
    package, so we don't have to have a new class in Local30.
    The new JDBC 3.0 methods don't make use of any new JDBC3.0 classes and
    so this will work fine in jdbc2.0 configuration.
        */

        /////////////////////////////////////////////////////////////////////////
        //
        //      JDBC 3.0        -       New public methods
        //
        /////////////////////////////////////////////////////////////////////////

        /**
    * JDBC 3.0
    *
    * Retrieves the value of the designated column in the current row of this
    * ResultSet object as a java.net.URL object in the Java programming language.
    *
    * @param columnIndex - the first column is 1, the second is 2
    * @return the column value as a java.net.URL object, if the value is SQL NULL,
    * the value returned is null in the Java programming language 
    * @exception SQLException Feature not implemented for now.
        */
        public URL getURL(int columnIndex)
    throws SQLException
        {
                throw Util.notImplemented();
        }

        /**
    * JDBC 3.0
    *
    * Retrieves the value of the designated column in the current row of this
    * ResultSet object as a java.net.URL object in the Java programming language.
    *
    * @param columnName - the SQL name of the column
    * @return the column value as a java.net.URL object, if the value is SQL NULL,
    * the value returned is null in the Java programming language 
    * @exception SQLException Feature not implemented for now.
        */
        public URL getURL(String columnName)
    throws SQLException
        {
                throw Util.notImplemented();
        }

        /**
    * JDBC 3.0
    *
    * Updates the designated column with a java.sql.Ref value. The updater methods are
    * used to update column values in the current row or the insert row. The
    * updater methods do not update the underlying database; instead the updateRow
    * or insertRow methods are called to update the database.
    *
    * @param columnIndex - the first column is 1, the second is 2
    * @param x - the new column value
    * @exception SQLException Feature not implemented for now.
        */
        public void updateRef(int columnIndex, Ref x)
    throws SQLException
        {
                throw Util.notImplemented();
        }

        /**
    * JDBC 3.0
    *
    * Updates the designated column with a java.sql.Ref value. The updater methods are
    * used to update column values in the current row or the insert row. The
    * updater methods do not update the underlying database; instead the updateRow
    * or insertRow methods are called to update the database.
    *
    * @param columnName - the SQL name of the column
    * @param x - the new column value
    * @exception SQLException Feature not implemented for now.
        */
        public void updateRef(String columnName, Ref x)
    throws SQLException
        {
                throw Util.notImplemented();
        }

        /**
    * JDBC 3.0
    *
    * Updates the designated column with a java.sql.Blob value. The updater methods are
    * used to update column values in the current row or the insert row. The
    * updater methods do not update the underlying database; instead the updateRow
    * or insertRow methods are called to update the database.
    *
    * @param columnIndex - the first column is 1, the second is 2
    * @param x - the new column value
    * @exception SQLException Feature not implemented for now.
        */
        public void updateBlob(int columnIndex, Blob x)
    throws SQLException
        {
                throw Util.notImplemented();
        }

        /**
    * JDBC 3.0
    *
    * Updates the designated column with a java.sql.Blob value. The updater methods are
    * used to update column values in the current row or the insert row. The
    * updater methods do not update the underlying database; instead the updateRow
    * or insertRow methods are called to update the database.
    *
    * @param columnName - the SQL name of the column
    * @param x - the new column value
    * @exception SQLException Feature not implemented for now.
        */
        public void updateBlob(String columnName, Blob x)
    throws SQLException
        {
                throw Util.notImplemented();
        }

        /**
    * JDBC 3.0
    *
    * Updates the designated column with a java.sql.Clob value. The updater methods are
    * used to update column values in the current row or the insert row. The
    * updater methods do not update the underlying database; instead the updateRow
    * or insertRow methods are called to update the database.
    *
    * @param columnIndex - the first column is 1, the second is 2
    * @param x - the new column value
    * @exception SQLException Feature not implemented for now.
        */
        public void updateClob(int columnIndex, Clob x)
    throws SQLException
        {
                throw Util.notImplemented();
        }

        /**
    * JDBC 3.0
    *
    * Updates the designated column with a java.sql.Clob value. The updater methods are
    * used to update column values in the current row or the insert row. The
    * updater methods do not update the underlying database; instead the updateRow
    * or insertRow methods are called to update the database.
    *
    * @param columnName - the SQL name of the column
    * @param x - the new column value
    * @exception SQLException Feature not implemented for now.
        */
        public void updateClob(String columnName, Clob x)
    throws SQLException
        {
                throw Util.notImplemented();
        }

        /**
    * JDBC 3.0
    *
    * Updates the designated column with a java.sql.Array value. The updater methods are
    * used to update column values in the current row or the insert row. The
    * updater methods do not update the underlying database; instead the updateRow
    * or insertRow methods are called to update the database.
    *
    * @param columnIndex - the first column is 1, the second is 2
    * @param x - the new column value
    * @exception SQLException Feature not implemented for now.
        */
        public void updateArray(int columnIndex, Array x)
    throws SQLException
        {
                throw Util.notImplemented();
        }

        /**
    * JDBC 3.0
    *
    * Updates the designated column with a java.sql.Array value. The updater methods are
    * used to update column values in the current row or the insert row. The
    * updater methods do not update the underlying database; instead the updateRow
    * or insertRow methods are called to update the database.
    *
    * @param columnName - the SQL name of the column
    * @param x - the new column value
    * @exception SQLException Feature not implemented for now.
        */
        public void updateArray(String columnName, Array x)
    throws SQLException
        {
                throw Util.notImplemented();
        }


        private boolean checkRowPosition(int position, String positionText)
                throws SQLException
        {
                // beforeFirst is only allowed on scroll cursors
                checkScrollCursor(positionText);

                checkIfClosed(positionText);    // checking result set closure does not depend
                                                                // on the underlying connection.  Do this
                                                                // outside of the connection synchronization.


                synchronized (getConnectionSynchronization()) {
                    setupContextStack();
                    try {
                    try {

                                /* Push and pop a StatementContext around a next call
                                 * so that the ResultSet will get correctly closed down
                                 * on an error.
                                 * (Cache the LanguageConnectionContext)
                                 */
                                LanguageConnectionContext lcc = getEmbedConnection().getLanguageConnection();
                                StatementContext statementContext = lcc.pushStatementContext(isAtomic,
											getSQLText(), getParameterValueSet(), false);

                                boolean result = theResults.checkRowPosition(position);

                                lcc.popStatementContext(statementContext, null);

                                return result;
                                
                    } catch (Throwable t) {
                                /*
                                 * Need to close the result set here because the error might
                                 * cause us to lose the current connection if this is an XA
                                 * connection and we won't be able to do the close later
                                 */
                        throw closeOnTransactionError(t);
                    } 

                        } finally {
                            restoreContextStack();
                        }
                }
        }

}
