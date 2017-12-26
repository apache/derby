/*

   Derby - Class org.apache.derby.impl.sql.compile.ConditionalNode

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

package	org.apache.derby.impl.sql.compile;

import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.JBitSet;

/**
 * A ConditionalNode represents an if/then/else operator with a single
 * boolean expression on the "left" of the operator and a list of expressions on 
 * the "right". This is used to represent the java conditional (aka immediate if).
 *
 */

class ConditionalNode extends ValueNode
{
    /**
     * The case operand if this is a simple case expression. Otherwise, it
     * is {@code null}.
     */
    private CachedValueNode caseOperand;

    /** The list of test conditions in the WHEN clauses. */
    private ValueNodeList testConditions;

    /**
     * The list of corresponding THEN expressions to the test conditions in
     * {@link #testConditions}. The last element represents the ELSE clause.
     */
    private ValueNodeList thenElseList;

	/**
     * Constructor for a ConditionalNode
	 *
     * @param caseOperand       The case operand if this is a simple case
     *                          expression, or {@code null} otherwise
     * @param testConditions    The boolean test conditions
	 * @param thenElseList		ValueNodeList with then and else expressions
     * @param cm                The context manager
	 */
    ConditionalNode(CachedValueNode caseOperand,
                    ValueNodeList testConditions,
                    ValueNodeList thenElseList,
                    ContextManager cm)
	{
        super(cm);
        this.caseOperand = caseOperand;
        this.testConditions = testConditions;
        this.thenElseList = thenElseList;
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
    void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

            if (testConditions != null)
			{
                printLabel(depth, "testConditions: ");
                testConditions.treePrint(depth + 1);
			}

			if (thenElseList != null)
			{
				printLabel(depth, "thenElseList: ");
				thenElseList.treePrint(depth + 1);
			}
		}
	}

	/**
     * This method makes sure any SQL NULLs will be cast to the correct type.
	 *
	 * @param castType        The type to cast SQL parsed NULL's too.
	 * @param fromList        FromList to pass on to bindExpression if recast is performed
	 * @param subqueryList    SubqueryList to pass on to bindExpression if recast is performed
     * @param aggregates      List of aggregates to pass on to bindExpression if recast is performed
	 *
	 * @exception             StandardException Thrown on error.
	 */
    private void recastNullNodes(
	                           DataTypeDescriptor castType, FromList fromList,
                               SubqueryList subqueryList, List<AggregateNode> aggregates)
	 throws StandardException {

		// need to have nullNodes nullable
		castType = castType.getNullabilityType(true);

        for (int i = 0; i < thenElseList.size(); i++) {
            ValueNode vn = thenElseList.elementAt(i);
            if (vn instanceof UntypedNullConstantNode) {
                CastNode cast = new CastNode(vn, castType, getContextManager());
                cast.bindExpression(fromList, subqueryList, aggregates);
                thenElseList.setElementAt(cast, i);
            }
        }
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
     * @param aggregates        The aggregate list being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ValueNode bindExpression(FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
			throws StandardException
	{
        CompilerContext cc = getCompilerContext();
        
        int previousReliability = orReliability( CompilerContext.CONDITIONAL_RESTRICTION );

        ValueNodeList caseOperandParameters =
                bindCaseOperand(cc, fromList, subqueryList, aggregates);

        testConditions.bindExpression(fromList,
			subqueryList,
            aggregates);

        // If we have a simple case expression in which the case operand
        // requires type from context (typically because it's an untyped
        // parameter), find out which type best describes it.
        if (caseOperandParameters != null) {

            // Go through all the dummy parameter nodes that bindCaseOperand()
            // inserted into the synthetic test conditions. Each of them will
            // have been given the type of the corresponding when operand
            // when testConditions was bound.
            for (ValueNode vn : caseOperandParameters) {
                // Check that this parameter is comparable to all the other
                // parameters in the list. This indirectly checks whether
                // all when operands have compatible types.
                caseOperandParameters.comparable(vn);

                // Replace the dummy parameter node with the actual case
                // operand.
                testConditions.accept(new ReplaceNodeVisitor(vn, caseOperand));
            }

            // Finally, after we have determined that all the when operands
            // are compatible, and we have reinserted the case operand into
            // the tree, set the type of the case operand to the dominant
            // type of all the when operands.
            caseOperand.setType(
                    caseOperandParameters.getDominantTypeServices());
        }

        thenElseList.bindExpression(fromList, subqueryList, aggregates);

        // Find the type of the first typed value in thenElseList and cast
        // all untyped NULL values to that type. We don't need to find the
        // dominant type here, since a top-level cast to that type will be
        // added later, if necessary.
        DataTypeDescriptor nullType = thenElseList.getTypeServices();
        if (nullType == null) {
            // There are no values with a known type in the list. Raise
            // an error.
            throw StandardException.newException(
                    SQLState.LANG_ALL_RESULT_EXPRESSIONS_UNTYPED);
        } else {
            recastNullNodes(nullType, fromList, subqueryList, aggregates);
        }

        // Set the result type of this conditional to be the dominant type
        // of the result expressions.
        setType(thenElseList.getDominantTypeServices());

		/* testCondition must be a boolean expression.
		 * If it is a ? parameter on the left, then set type to boolean,
		 * otherwise verify that the result type is boolean.
		 */
        testConditions.setParameterDescriptor(
                new DataTypeDescriptor(TypeId.BOOLEAN_ID, true));

        for (ValueNode testCondition : testConditions) {
			if ( ! testCondition.getTypeServices().getTypeId().equals(
														TypeId.BOOLEAN_ID))
			{
				throw StandardException.newException(SQLState.LANG_CONDITIONAL_NON_BOOLEAN);
			}
		}

        // Set the type of the parameters.
        thenElseList.setParameterDescriptor(getTypeServices());

		/* The then and else expressions must be type compatible */
		ClassInspector cu = getClassFactory().getClassInspector();

		/*
		** If it is comparable, then we are ok.  Note that we
		** could in fact allow any expressions that are convertible()
		** since we are going to generate a cast node, but that might
		** be confusing to users...
		*/
        for (ValueNode expr : thenElseList) {
            DataTypeDescriptor dtd = expr.getTypeServices();
            String javaTypeName =
                    dtd.getTypeId().getCorrespondingJavaTypeName();
            String resultJavaTypeName =
                    getTypeId().getCorrespondingJavaTypeName();

            if (!dtd.comparable(getTypeServices(), false, getClassFactory())
                    && !cu.assignableTo(javaTypeName, resultJavaTypeName)
                    && !cu.assignableTo(resultJavaTypeName, javaTypeName)) {
                throw StandardException.newException(
                        SQLState.LANG_NOT_TYPE_COMPATIBLE,
                        dtd.getTypeId().getSQLTypeName(),
                        getTypeId().getSQLTypeName());
            }
        }

        // The result is nullable if and only if at least one of the result
        // expressions is nullable (DERBY-6567).
        setNullability(thenElseList.isNullable());

		/*
		** Generate a CastNode if necessary and
		** stick it over the original expression
		*/
		TypeId condTypeId = getTypeId();
        for (int i = 0; i < thenElseList.size(); i++) {
            ValueNode expr = thenElseList.elementAt(i);
            if (expr.getTypeId().typePrecedence()
                    != condTypeId.typePrecedence()) {
                // Cast to dominant type.
                ValueNode cast = new CastNode(
                        expr, getTypeServices(), getContextManager());
                cast = cast.bindExpression(fromList, subqueryList, aggregates);
                thenElseList.setElementAt(cast, i);
            }
        }

        cc.setReliability( previousReliability );
        
		return this;
	}

    /**
     * <p>
     * Bind the case operand, if there is one, and check that it doesn't
     * contain anything that's illegal in a case operand (such as calls to
     * routines that are non-deterministic or modify SQL).
     * </p>
     *
     * <p>
     * Also, if the type of the case operand needs to be inferred, insert
     * dummy parameter nodes into {@link #testConditions} instead of the
     * case operand, so that the type can be inferred individually for each
     * test condition. Later, {@link #bindExpression} will find a common
     * type for all of them, use that type for the case operand, and reinsert
     * the case operand into the test conditions.
     * </p>
     *
     * @return a list of dummy parameter nodes that have been inserted into
     * the tree instead of the case operand, if such a replacement has
     * happened; otherwise, {@code null} is returned
     */
    private ValueNodeList bindCaseOperand(
                    CompilerContext cc, FromList fromList,
                    SubqueryList subqueryList, List<AggregateNode> aggregates)
            throws StandardException {

        ValueNodeList replacements = null;

        if (caseOperand != null) {
            int previousReliability = orReliability(
                    CompilerContext.CASE_OPERAND_RESTRICTION);

            // If the case operand requires type from context (typically,
            // because it is an untyped parameter), we need to find a type
            // that is comparable with all the when operands.
            //
            // To find the types of the when operands, temporarily replace
            // all occurrences of the case operand with dummy parameter nodes.
            // Later, after binding testConditions, those dummy nodes will
            // have their types set to the types of the when operands. At that
            // time, we will be able to find a common type, set the type of the
            // case operand to that type, and reinsert the case operand into
            // the tree.
            if (caseOperand.requiresTypeFromContext()) {
                replacements = new ValueNodeList(getContextManager());
                testConditions.accept(
                        new ReplaceCaseOperandVisitor(replacements));
            }

            caseOperand = (CachedValueNode) caseOperand.bindExpression(
                    fromList, subqueryList, aggregates);

            cc.setReliability(previousReliability);
        }

        return replacements;
    }

    /**
     * A visitor that replaces all occurrences of the {@link #caseOperand} node
     * in a tree with dummy parameter nodes. It also fills a supplied list
     * with the parameter nodes that have been inserted into the tree.
     */
    private class ReplaceCaseOperandVisitor implements Visitor {
        private final ValueNodeList replacements;

        private ReplaceCaseOperandVisitor(ValueNodeList replacements) {
            this.replacements = replacements;
        }

        @Override
        public Visitable visit(Visitable node) throws StandardException {
            if (node == caseOperand) {
                ParameterNode pn = new ParameterNode(
                        0, null, getContextManager());
                replacements.addElement(pn);
                return pn;
            } else {
                return node;
            }
        }

        @Override
        public boolean visitChildrenFirst(Visitable node) {
            return false;
        }

        @Override
        public boolean stopTraversal() {
            return false;
        }

        @Override
        public boolean skipChildren(Visitable node) throws StandardException {
            return false;
        }
    }

	/**
	 * Preprocess an expression tree.  We do a number of transformations
	 * here (including subqueries, IN lists, LIKE and BETWEEN) plus
	 * subquery flattening.
	 * NOTE: This is done before the outer ResultSetNode is preprocessed.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
        testConditions.preprocess(numTables,
                                  outerFromList, outerSubqueryList,
                                  outerPredicateList);
 		thenElseList.preprocess(numTables,
								outerFromList, outerSubqueryList,
								outerPredicateList);
		return this;
	}

	/**
	 * Categorize this predicate.  Initially, this means
	 * building a bit map of the referenced tables for each predicate.
	 * If the source of this ColumnReference (at the next underlying level) 
	 * is not a ColumnReference or a VirtualColumnNode then this predicate
	 * will not be pushed down.
	 *
	 * For example, in:
	 *		select * from (select 1 from s) a (x) where x = 1
	 * we will not push down x = 1.
	 * NOTE: It would be easy to handle the case of a constant, but if the
	 * inner SELECT returns an arbitrary expression, then we would have to copy
	 * that tree into the pushed predicate, and that tree could contain
	 * subqueries and method calls.
	 * RESOLVE - revisit this issue once we have views.
	 *
	 * @param referencedTabs	JBitSet with bit map of referenced FromTables
	 * @param simplePredsOnly	Whether or not to consider method
	 *							calls, field references and conditional nodes
	 *							when building bit map
	 *
	 * @return boolean		Whether or not source.expression is a ColumnReference
	 *						or a VirtualColumnNode.
	 * @exception StandardException			Thrown on error
	 */
    @Override
    boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		/* We stop here when only considering simple predicates
		 *  as we don't consider conditional operators when looking
		 * for null invariant predicates.
		 */
		if (simplePredsOnly)
		{
			return false;
		}

		boolean pushable;

        pushable = testConditions.categorize(referencedTabs, simplePredsOnly);
		pushable = (thenElseList.categorize(referencedTabs, simplePredsOnly) && pushable);
		return pushable;
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return ValueNode			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
    @Override
    ValueNode remapColumnReferencesToExpressions()
		throws StandardException
	{
        testConditions = testConditions.remapColumnReferencesToExpressions();
		thenElseList = thenElseList.remapColumnReferencesToExpressions();
		return this;
	}

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
    @Override
    boolean isConstantExpression()
	{
        return (testConditions.isConstantExpression() &&
			    thenElseList.isConstantExpression());
	}

	/** @see ValueNode#constantExpression */
    @Override
    boolean constantExpression(PredicateList whereClause)
	{
        return (testConditions.constantExpression(whereClause) &&
			    thenElseList.constantExpression(whereClause));
	}

	/**
	 * Eliminate NotNodes in the current query block.  We traverse the tree, 
	 * inverting ANDs and ORs and eliminating NOTs as we go.  We stop at 
	 * ComparisonOperators and boolean expressions.  We invert 
	 * ComparisonOperators and replace boolean expressions with 
	 * boolean expression = false.
	 * NOTE: Since we do not recurse under ComparisonOperators, there
	 * still could be NotNodes left in the tree.
	 *
	 * @param	underNotNode		Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	ValueNode eliminateNots(boolean underNotNode) 
					throws StandardException
	{
        // NOT CASE WHEN a THEN b ELSE c END is equivalent to
        // CASE WHEN a THEN NOT b ELSE NOT c END, so just push the
        // NOT node down to the THEN and ELSE expressions.
        thenElseList.eliminateNots(underNotNode);

        // Eliminate NOTs in the WHEN expressions too. The NOT node above us
        // should not be pushed into the WHEN expressions, though, as that
        // would alter the meaning of the CASE expression.
        testConditions.eliminateNots(false);

		return this;
	}

	/**
	 * Do code generation for this conditional expression.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{
        if (SanityManager.DEBUG) {
            // There should be at least one test condition.
            SanityManager.ASSERT(testConditions.size() > 0);
            // Because of the ELSE clause, there should always be one
            // more element in thenElseList than in testConditions.
            SanityManager.ASSERT(
                    thenElseList.size() == testConditions.size() + 1);
        }

        // Generate code for all WHEN ... THEN clauses.
        for (int i = 0; i < testConditions.size(); i++) {
            testConditions.elementAt(i).generateExpression(acb, mb);
            mb.cast(ClassName.BooleanDataValue);
            mb.push(true);
            mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
                          "equals", "boolean", 1);
            mb.conditionalIf();
            thenElseList.elementAt(i).generateExpression(acb, mb);
            mb.startElseCode();
        }

        // Generate code for the ELSE clause.
        thenElseList.elementAt(thenElseList.size() - 1)
                    .generateExpression(acb, mb);

        for (int i = 0; i < testConditions.size(); i++) {
            mb.completeConditional();
        }

        // If we have a cached case operand, clear the field that holds
        // the cached value after the case expression has been evaluated,
        // so that the value can be garbage collected early.
        if (caseOperand != null) {
            caseOperand.generateClearField(mb);
        }
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
    @Override
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

        if (testConditions != null)
		{
            testConditions = (ValueNodeList) testConditions.accept(v);
		}

		if (thenElseList != null)
		{
			thenElseList = (ValueNodeList)thenElseList.accept(v);
		}
	}
        
	/**
	 * {@inheritDoc}
	 */
    boolean isEquivalent(ValueNode o) throws StandardException
	{
        if (isSameNodeKind(o)) {
			ConditionalNode other = (ConditionalNode)o;
            return testConditions.isEquivalent(other.testConditions) &&
                    thenElseList.isEquivalent(other.thenElseList);
		}

		return false;
	}
}
