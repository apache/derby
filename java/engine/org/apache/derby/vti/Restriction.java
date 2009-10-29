/*

   Derby - Class org.apache.derby.vti.Restriction

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

import java.io.Serializable;
import java.sql.SQLException;

/**
   <p>
 * An expression to be pushed into a Table Function so that the Table Function
 * can short-circuit its processing and return fewer rows. A restriction is represented
 * as a binary tree. The non-leaf nodes are ANDs and ORs. The leaf nodes
 * are ColumnQualifiers. A ColumnQualifier
 * is a simple expression comparing a constant value to a column in
 * the Table Function.
 * </p>
 */
public abstract class Restriction implements Serializable
{
    /**
     * Turn this Restriction into a string suitable for use in a WHERE clause.
     */
    public abstract String toSQL();

    /** Utility method to double quote a string */
    protected String doubleQuote( String raw ) { return "\"" + raw + "\""; }

    /** Utility method to parenthesize an expression */
    protected String parenthesize( String raw ) { return "( " + raw + " )"; }
    
    /** An AND of two Restrictions */
    public static class AND extends Restriction
    {
        /** Derby serializes these objects in PreparedStatements */
        public static final long serialVersionUID = -8205388794606605844L;
        
        private Restriction _leftChild;
        private Restriction _rightChild;

        /** AND together two other Restrictions */
        public AND( Restriction leftChild, Restriction rightChild )
        {
            _leftChild = leftChild;
            _rightChild = rightChild;
        }

        /** Get the left Restriction */
        public Restriction getLeftChild() { return _leftChild; }

        /** Get the right Restriction */
        public Restriction getRightChild() { return _rightChild; }
        
        public String toSQL()
        {
            return parenthesize( _leftChild.toSQL() ) + " AND " + parenthesize( _rightChild.toSQL() );
        }
    }

    /** An OR of two Restrictions */
    public static class OR extends Restriction
    {
        /** Derby serializes these objects in PreparedStatements */
        public static final long serialVersionUID = -8205388794606605844L;
        
        private Restriction _leftChild;
        private Restriction _rightChild;

        /** OR together two other Restrictions */
        public OR( Restriction leftChild, Restriction rightChild )
        {
            _leftChild = leftChild;
            _rightChild = rightChild;
        }

        /** Get the left Restriction */
        public Restriction getLeftChild() { return _leftChild; }

        /** Get the right Restriction */
        public Restriction getRightChild() { return _rightChild; }

        public String toSQL()
        {
            return parenthesize( _leftChild.toSQL() ) + " OR " + parenthesize( _rightChild.toSQL() );
        }
    }

    /**
       <p>
       * A simple comparison of a column to a constant value. The comparison
       * has the form:
       * </p>
       *
       * <blockquote><pre>
       * column OP constant
       * </pre></blockquote>
       *
       * <p>
       * where OP is one of the following:
       * </p>
       *
       * <blockquote><pre>
       *  <     =     <=     >      >=    IS NULL    IS NOT NULL
       * </pre></blockquote>
       */
    public static class ColumnQualifier extends Restriction
    {
        ////////////////////////////////////////////////////////////////////////////////////////
        //
        // CONSTANTS
        //
        ////////////////////////////////////////////////////////////////////////////////////////

        /** Derby serializes these objects in PreparedStatements */
        public static final long serialVersionUID = -8205388794606605844L;
        
        /**	 Ordering operation constant representing '<' **/
        public static final int ORDER_OP_LESSTHAN = 0;

        /**	 Ordering operation constant representing '=' **/
        public static final int ORDER_OP_EQUALS = 1;

        /**	 Ordering operation constant representing '<=' **/
        public static final int ORDER_OP_LESSOREQUALS = 2;

        /**	 Ordering operation constant representing '>' **/
        public static final int ORDER_OP_GREATERTHAN = 3;

        /**	 Ordering operation constant representing '>=' **/
        public static final int ORDER_OP_GREATEROREQUALS = 4;

        /**	 Ordering operation constant representing 'IS NULL' **/
        public static final int ORDER_OP_ISNULL = 5;

        /**	 Ordering operation constant representing 'IS NOT NULL' **/
        public static final int ORDER_OP_ISNOTNULL = 6;

        // Visible forms of the constants above
        private String[] OPERATOR_SYMBOLS = new String[] {  "<", "=", "<=", ">", ">=", "IS NULL", "IS NOT NULL" };

        ////////////////////////////////////////////////////////////////////////////////////////
        //
        // STATE
        //
        ////////////////////////////////////////////////////////////////////////////////////////

        /** name of column being restricted */
        private String _columnName;

        /** comparison operator, one of the ORDER_OP constants */
        private int     _comparisonOperator;

        /** value to compare the column to */
        private Object _constantOperand;
        
        ////////////////////////////////////////////////////////////////////////////////////////
        //
        // CONSTRUCTORS
        //
        ////////////////////////////////////////////////////////////////////////////////////////

        /**
         * <p>
         * Construct from pieces.
         * </p>
         *
         * @param columnName Name of column as declared in the CREATE FUNCTION statement.
         * @param comparisonOperator One of the ORDER_OP constants.
         * @param constantOperand Constant value to which the column should be compared.
         */
        public ColumnQualifier
            (
             String columnName,
             int comparisonOperator,
             Object constantOperand
             )
        {
            _columnName = columnName;
            _comparisonOperator = comparisonOperator;
            _constantOperand = constantOperand;
        }
        
        ////////////////////////////////////////////////////////////////////////////////////////
        //
        // ACCESSORS
        //
        ////////////////////////////////////////////////////////////////////////////////////////
        
        /**
         * <p>
         * The name of the column being compared.
         * </p>
         */
        public String getColumnName() { return _columnName; }

        /**
         * <p>
         * The type of comparison to perform. This is one of the ORDER_OP constants
         * defined above.
         * </p>
         */
        public int getComparisonOperator() { return _comparisonOperator; }

        /**
         * <p>
         * Get the constant value to which the column should be compared. The
         * constant value must be an Object of the Java type which corresponds to
         * the SQL type of the column. The column's SQL type was declared in the CREATE FUNCTION statement.
         * The mapping of SQL types to Java types is defined in table 4 of chapter 14
         * of the original JDBC 1 specification (dated 1996). Bascially, these are the Java
         * wrapper values you would expect. For instance, SQL INT maps to java.lang.Integer, SQL CHAR
         * maps to java.lang.String, etc.. This object will be null if the
         * comparison operator is ORDER_OP_ISNULL or ORDER_OP_ISNOTNULL.
         * </p>
         */
        public Object getConstantOperand() { return _constantOperand; }
        
        public String toSQL()
        {
            StringBuffer buffer = new StringBuffer();

            buffer.append( doubleQuote( _columnName ) );
            buffer.append( " " + OPERATOR_SYMBOLS[ _comparisonOperator ] + " " );
            if ( _constantOperand != null ) { buffer.append( _constantOperand ); }

            return buffer.toString();
        }
    }
    
}

