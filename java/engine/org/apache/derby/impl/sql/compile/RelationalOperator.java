/*

   Derby - Class org.apache.derby.impl.sql.compile.RelationalOperator

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.Optimizable;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

/**
 * This interface is an abstraction of a relational operator.  It was created
 * for optimization, to allow binary comparison operators and IS NULL to
 * be treated the same.
 */
public interface RelationalOperator
{
	public final int EQUALS_RELOP = 1;
	public final int NOT_EQUALS_RELOP = 2;
	public final int GREATER_THAN_RELOP = 3;
	public final int GREATER_EQUALS_RELOP = 4;
	public final int LESS_THAN_RELOP = 5;
	public final int LESS_EQUALS_RELOP = 6;
	public final int IS_NULL_RELOP = 7;
	public final int IS_NOT_NULL_RELOP = 8;

	/**
	 * Check whether this RelationalOperator is a comparison of the given
	 * column with an expression.  If so, return the ColumnReference that
	 * corresponds to the given column, and that is on one side of this
	 * RelationalOperator or the other (this method copes with the
	 * column being on either side of the operator).  If the given column
	 * does not appear by itself on one side of the comparison, the
	 * method returns null.
	 *
	 * @param optTable	An Optimizable for the base table the column is in
	 * @param columnPosition	The ordinal position of the column (one-based)
	 *
	 * @return	The ColumnReference on one side of this RelationalOperator
	 *			that represents the given columnPosition.  Returns null
	 *			if no such ColumnReference exists by itself on one side of
	 *			this RelationalOperator.
	 */
	ColumnReference	getColumnOperand(Optimizable optTable, int columnPosition);

	/**
	 * Check whether this RelationalOperator is a comparison of the given
	 * column with an expression.  If so, return the ColumnReference that
	 * corresponds to the given column, and that is on one side of this
	 * RelationalOperator or the other (this method copes with the
	 * column being on either side of the operator).  If the given column
	 * does not appear by itself on one side of the comparison, the
	 * method returns null.
	 *
	 * @param tableNumber	The table number of the table in question
	 * @param columnPosition	The ordinal position of the column (one-based)
	 *
	 * @return	The ColumnReference on one side of this RelationalOperator
	 *			that represents the given columnPosition.  Returns null
	 *			if no such ColumnReference exists by itself on one side of
	 *			this RelationalOperator.
	 */
	ColumnReference getColumnOperand(int tableNumber, int columnPosition);

	/**
	 * Get the ColumnReference for the given table on one side of this
	 * RelationalOperator.  This presumes it will be found only on one
	 * side.  If not found, it will return null.
	 */
	ColumnReference getColumnOperand(Optimizable optTable);

	/**
	 * Check whether this RelationalOperator is a comparison of the given
	 * column with an expression.  If so, return the expression
	 * the column is being compared to.
	 *
	 * @param tableNumber	The table number of the base table the column is in
	 * @param columnPosition	The ordinal position of the column (one-based)
	 *
	 * @return	The ValueNode for the expression the column is being compared
	 *			to - null if the column is not being compared to anything.
	 */
	ValueNode getExpressionOperand(int tableNumber,
									int columnPosition);

	/**
	 * Check whether this RelationalOperator is a comparison of the given
	 * column with an expression.  If so, generate the Expression for
	 * the ValueNode that the column is being compared to.
	 *
	 * @param optTable	An Optimizable for the base table the column is in
	 * @param columnPosition	The ordinal position of the column (one-based)
	 * @param acb		The ExpressionClassBuilder for the class we're building
	 * @param mb		The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateExpressionOperand(Optimizable optTable,
										int columnPosition,
										ExpressionClassBuilder acb,
										MethodBuilder mb)
						throws StandardException;

	/**
	 * Check whether this RelationalOperator compares the given ColumnReference
	 * to any columns in the same table as the ColumnReference.
	 *
	 * @param cr	The ColumnReference that is being compared to some
	 *				expression.
	 *
	 * @return	true if the given ColumnReference is being compared to any
	 *			columns from the same table
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean			selfComparison(ColumnReference cr)
		throws StandardException;

	/**
	 * Tell whether this relop is a useful start key for the given table.
	 * It has already been determined that the relop has a column from
	 * the given table on one side or the other.
	 *
	 * @param optTable	The Optimizable table for which we want to know
	 *					whether this is a useful start key.
	 *
	 * @return	true if this is a useful start key
	 */
	boolean	usefulStartKey(Optimizable optTable);

	/**
	 * Tell whether this relop is a useful stop key for the given table.
	 * It has already been determined that the relop has a column from
	 * the given table on one side or the other.
	 *
	 * @param optTable	The Optimizable table for which we want to know
	 *					whether this is a useful stop key.
	 *
	 * @return	true if this is a useful stop key
	 */
	boolean	usefulStopKey(Optimizable optTable);

	/**
	 * Get the start operator for a scan (at the store level) for this
	 * RelationalOperator.
	 *
	 * @param optTable	The optimizable table we're doing the scan on.
	 *					This parameter is so we can tell which side of
	 *					the operator the table's column is on.
	 *
	 * @return	Either ScanController.GT or ScanController.GE
	 *
	 * @see TransactionController#openScan
	 */
	int getStartOperator(Optimizable optTable);

	/**
	 * Get the stop operator for a scan (at the store level) for this
	 * RelationalOperator.
	 *
	 * @param optTable	The optimizable table we're doing the scan on.
	 *					This parameter is so we can tell which side of
	 *					the operator the table's column is on.
	 *
	 * @return	Either ScanController.GT or ScanController.GE
	 *
	 * @see TransactionController#openScan
	 */
	int getStopOperator(Optimizable optTable);

	/**
	 * Generate the absolute column id for the ColumnReference that appears on one
	 * side of this RelationalOperator or the other, and that refers to
	 * the given table. (Absolute column id means column id within the 
	 * row stored on disk.)
	 *
	 * @param mb	The method the generated code is to go into
	 * @param optTable	The optimizable table we're doing the scan on.
	 *
	 */
	void generateAbsoluteColumnId(MethodBuilder mb,
										Optimizable optTable);

	/**
	 * Generate the relative column id for the ColumnReference that appears on one
	 * side of this RelationalOperator or the other, and that refers to
	 * the given table. (Relative column id means column id within the 
	 * partial row returned by the store.)
	 *
	 * @param mb	The method the generated code is to go into
	 * @param optTable	The optimizable table we're doing the scan on.
	 *
	 */
	void generateRelativeColumnId(MethodBuilder mb,
										Optimizable optTable);

	/**
	 * Generate the comparison operator for this RelationalOperator.
	 * The operator can depend on which side of this operator the
	 * optimizable column is.
	 * 
	 * @param mb	The method the generated code is to go into
	 * @param optTable	The optimizable table we're doing the scan on.
	 *
	 */
	void generateOperator(MethodBuilder mb,
								Optimizable optTable);

	/**
	 * Generate the method to evaluate a Qualifier.  The factory method for
	 * a Qualifier takes a GeneratedMethod that returns the Orderable
	 * that Qualifier.getOrderable() returns.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're building
	 * @param mb	The method the generated code is to go into
	 * @param optTable	The Optimizable table the Qualifier will qualify
	 *
	 * @return	An expression that evaluates to the GeneratedMethod that
	 *			returns the Orderable to be returned by Qualifier.getOrderable()
	 *
	 * @exception StandardException		Thrown on error.
	 */
	void generateQualMethod(ExpressionClassBuilder acb,
								MethodBuilder mb,
								Optimizable optTable)
						throws StandardException;

	/**
	 * Generate an expression that evaluates to true if this RelationalOperator
	 * uses ordered null semantics, false if it doesn't.
	 *
	 * @param mb	The method the generated code is to go into
	 *
	 */
	void generateOrderedNulls(MethodBuilder mb);

	/**
	 * Generate an expression that evaluates to true if the result of the
	 * comparison should be negated.  For example, col > 1 generates
	 * a comparison operator of <= and a negation of true, while col < 1
	 * generates a comparison operator of < and a negation of false.
	 *
	 * @param mb	The method the generated code is to go into
	 * @param optTable	The Optimizable table the Qualifier will qualify
	 */
	void generateNegate(MethodBuilder mb,
								Optimizable optTable);

	/** Return true if this operator uses ordered null semantics */
	boolean orderedNulls();

	/**
	 * Return true if this operator can be compiled into a Qualifier for
	 * the given Optimizable table.  This means that there is a column
	 * from that table on one side of this relop, and an expression that
	 * does not refer to the table on the other side of the relop.
	 *
	 * @param optTable	The Optimizable table in question.
	 *
	 * @return	true if this operator can be compiled into a Qualifier
	 *			for the given Optimizable table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean isQualifier(Optimizable optTable) throws StandardException;

	/**
	 * Return the operator (as an int) for this RelationalOperator.
	 *
	 * @return int		The operator for this RelationalOperator.
	 */
	public int getOperator();

	/**
	 * Return the variant type for the Qualifier's Orderable.
	 * (Is the Orderable invariant within a scan or within a query?)
	 *
	 * @param optTable	The Optimizable table the Qualifier will qualify
	 *
	 * @return int		The variant type for the Qualifier's Orderable.
	 * @exception StandardException	thrown on error
	 */
	public int getOrderableVariantType(Optimizable optTable)
		throws StandardException;

	/**
	 * Return whether this operator compares the given Optimizable with
	 * a constant whose value is known at compile time.
	 */
	public boolean compareWithKnownConstant(Optimizable optTable, boolean considerParameters);

	/**
	 * Return an Object representing the known value that this relational
	 * operator is comparing to a column in the given Optimizable.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor getCompareValue(Optimizable optTable)
					throws StandardException;

	/**
	 * Return whether this operator is an equality comparison of the given
	 * optimizable with a constant expression.
	 */
	public boolean equalsComparisonWithConstantExpression(Optimizable optTable);

	/**
	 * Return a relational operator which matches the current one
	 * but with the passed in ColumnReference as the (left) operand.
	 *
	 * @param otherCR	The ColumnReference for the new (left) operand.
	 *
	 * @return	A relational operator which matches the current one
	 *			but with the passed in ColumnReference as the (left) operand.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public RelationalOperator getTransitiveSearchClause(ColumnReference otherCR)
		throws StandardException;
}
