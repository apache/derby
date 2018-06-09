/*

   Derby - Class org.apache.derby.vti.RestrictedVTI

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derby.vti;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
   <p>
 * Interface for Table Functions which can be told which columns need to be fetched plus simple bounds on those columns.
 * </p>
 *
 * <p>
 * This interface can be implemented by the ResultSet returned by the public
 * static method which is bound to the Table Function. If that ResultSet
 * implements this interface, then the initScan() method of this interface will be
 * called before the scan of the ResultSet starts, that is, before calling any
 * ResultSet method.
 * </p>
 *
 * <p>
 * ResultSets which implement this interface can perform more efficiently
 * because they don't have to fetch all columns and rows. This can mean
 * performance boosts for queries which only need a subset of the Table
 * Function's columns and for queries which compare those columns to constant
 * expressions using the &lt;, &lt;=, =, &gt;, &gt;=, and != operators. This can also mean
 * performance boosts for LIKE and BETWEEN operations on Table Function
 * columns. For more information, see the commentary on
 * <a href="https://issues.apache.org/jira/browse/DERBY-4357">DERBY-4357</a>.
 * </p>
 */
public interface RestrictedVTI
{
    /**
     * <p>
     * Initialize a scan of a ResultSet. This method is called once before the scan begins. It
     * is called before any
     * ResultSet method is called. This method performs two tasks:
     * </p>
     *
     * <li><b>Column names</b> - Tells the ResultSet which columns need to be fetched.</li>
     * <li><b>Limits</b> - Gives the ResultSet simple bounds to apply in order
     * to limit which rows are returned. Note that the ResultSet does not have
     * to enforce all of these bounds. Derby will redundantly enforce these
     * limits on all rows returned by the ResultSet. That is, filtering not
     * performed inside the ResultSet will still happen outside the ResultSet.</li>
     *
     * <p>
     * The <i>columnNames</i> argument is an array of columns which need to be fetched.  This
     * is an array of the column names declared in the Table Function's
     * CREATE FUNCTION statement. Column names which were double-quoted
     * in the CREATE FUNCTION statement appear case-sensitive in this
     * array. Column names which were not double-quoted appear
     * upper-cased. Derby asks the Table Function to fetch all columns mentioned
     * in the query. This includes columns mentioned in the SELECT list as well
     * as columns mentioned in the WHERE clause. Note that a column could be
     * mentioned in the WHERE clause in a complex expression which could not be
     * passed to the Table Function via the <i>restriction</i> argument.
     * </p>
     *
     * <p>
     * The array has one slot for each column declared in the CREATE FUNCTION
     * statement. Slot 0 corresponds to the first column declared in the CREATE
     * FUNCTION statement and so on. If a column does not need to be
     * fetched, then the corresponding slot is null. If a column needs
     * to be fetched, then the corresponding slot holds the column's
     * name.
     * </p>
     *
     * <p>
     * Note that even though the array may have gaps, it is expected that
     * columns in the ResultSet will occur at the positions declared in the
     * CREATE FUNCTION statement. Consider the following declaration:
     * </p>
     *
     * <blockquote><pre>
     *  create function foreignEmployeeTable()
     *  returns table
     *  (
     *      id        int,
     *      birthDay  date,
     *      firstName varchar( 100 ),
     *      lastName  varchar( 100 )
     *  )
     * ...
     * </pre></blockquote>
     *
     * <p>
     * and the following query:
     * </p>
     *
     * <blockquote><pre>
     * select lastName from table( foreignEmployeeTable() ) s
     * </pre></blockquote>
     *
     * <p>
     * In this example, the array passed to this method will have
     * 4 slots. Slots 0, 1, and 2 will be null and slot 3 will hold
     * the String "LASTNAME". Last names will be retrieved from the
     * ResultSet by calls to getString( 4 )--remember that JDBC column
     * ids are 1-based.
     * </p>
     *
     * <p>
     * The <i>restriction</i> argument is a simple expression which should be evaluated inside the Table
     * Function in order to eliminate rows. The expression is a binary tree built out of ANDs,
     * ORs, and column qualifiers. The column qualifiers are simple comparisons
     * between constant values and columns in the Table Function. The Table
     * Function only returns rows which satisfy the expression. The
     * <i>restriction</i> is redundantly enforced by Derby on the rows returned
     * by the ResultSet--this means that <i>restriction</i> gives the Table
     * Function a hint about how to optimize its performance but the Table
     * Function is not required to enforce the entire <i>restriction</i>.
     * </p>
     */
    public void initScan( String[] columnNames, Restriction restriction ) throws SQLException;

}
