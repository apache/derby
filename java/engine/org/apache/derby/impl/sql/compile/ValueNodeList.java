/*

   Derby - Class org.apache.derby.impl.sql.compile.ValueNodeList

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
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.JBitSet;

/**
 * A ValueNodeList represents a list of ValueNodes within a specific predicate 
 * e.g. IN list, NOT IN list or BETWEEN in a DML statement.
 */

class ValueNodeList extends QueryTreeNodeVector<ValueNode>
{
    ValueNodeList(ContextManager cm) {
        super(ValueNode.class, cm);
    }

	/**
	 * Add a ValueNode to the list.
	 *
	 * @param valueNode	A ValueNode to add to the list
	 *
	 * @exception StandardException		Thrown on error
	 */

    void addValueNode(ValueNode valueNode) throws StandardException
	{
		addElement(valueNode);
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
	 * @exception StandardException		Thrown on error
	 */
    void bindExpression(FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
			throws StandardException
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            ValueNode vn = elementAt(index);
            vn = vn.bindExpression(fromList, subqueryList, aggregates);

			setElementAt(vn, index);
		}
	}


	/**
	 * Generate a SQL-&gt;Java-&gt;SQL conversion tree any node in the list
	 * which is not a system built-in type.
	 * This is useful when doing comparisons, built-in functions, etc. on
	 * java types which have a direct mapping to system built-in types.
	 *
	 * @exception StandardException	Thrown on error
	 */
    void genSQLJavaSQLTrees()
		throws StandardException
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            ValueNode valueNode = elementAt(index);
			
			if (valueNode.getTypeId().userType())
			{
				setElementAt(valueNode.genSQLJavaSQLTree(), index);
			}
		}
	}

	/**
	 * Get the dominant DataTypeServices from the elements in the list. This
	 * method will also set the correct collation information on the dominant
	 * DataTypeService if we are dealing with character string datatypes.
	 *  
	 * Algorithm for determining collation information
	 * This method will check if it is dealing with character string datatypes.
	 * If yes, then it will check if all the character string datatypes have
	 * the same collation derivation and collation type associated with them.
	 * If not, then the resultant DTD from this method will have collation
	 * derivation of NONE. If yes, then the resultant DTD from this method will
	 * have the same collation derivation and collation type as all the 
	 * character string datatypes.
	 * 
	 * Note that this method calls DTD.getDominantType and that method returns
	 * the dominant type of the 2 DTDs involved in this method. That method 
	 * sets the collation info on the dominant type following the algorithm
	 * mentioned in the comments of 
	 * @see DataTypeDescriptor#getDominantType(DataTypeDescriptor, ClassFactory)
	 * With that algorithm, if one DTD has collation derivation of NONE and the
	 * other DTD has collation derivation of IMPLICIT, then the return DTD from
	 * DTD.getDominantType will have collation derivation of IMPLICIT. That is 
	 * not the correct algorithm for aggregate operators. SQL standards says
	 * that if EVERY type has implicit derivation AND is of the same type, then 
	 * the collation of the resultant will be of that type with derivation 
	 * IMPLICIT. To provide this behavior for aggregate operator, we basically 
	 * ignore the collation type and derivation picked by 
	 * DataTypeDescriptor.getDominantType. Instead we let 
	 * getDominantTypeServices use the simple algorithm listed at the top of
	 * this method's comments to determine the collation type and derivation 
	 * for this ValueNodeList object.
	 * 
	 * @return DataTypeServices		The dominant DataTypeServices.
	 *
	 * @exception StandardException		Thrown on error
	 */
    DataTypeDescriptor getDominantTypeServices() throws StandardException
	{
		DataTypeDescriptor	dominantDTS = null;
		//Following 2 will hold the collation derivation and type of the first 
		//string operand. This collation information will be checked against
		//the collation derivation and type of other string operands. If a 
		//mismatch is found, foundCollationMisMatch will be set to true.
		int firstCollationDerivation = -1;
		int firstCollationType = -1;
		//As soon as we find 2 strings with different collations, we set the 
		//following flag to true. At the end of the method, if this flag is set 
		//to true then it means that we have operands with different collation
		//types and hence the resultant dominant type will have to have the
		//collation derivation of NONE. 
		boolean foundCollationMisMatch = false;

		for (int index = 0; index < size(); index++)
		{
			ValueNode			valueNode;

            valueNode = elementAt(index);

            // Skip nodes that take their type from the context, if they
            // haven't already been bound to a type.
            if (valueNode.requiresTypeFromContext()
                    && valueNode.getTypeServices() == null) {
				continue;
            }

			DataTypeDescriptor valueNodeDTS = valueNode.getTypeServices();

			if (valueNodeDTS.getTypeId().isStringTypeId())
			{
				if (firstCollationDerivation == -1)
				{
					//found first string type. Initialize firstCollationDerivation
					//and firstCollationType with collation information from 
					//that first string type operand.
					firstCollationDerivation = valueNodeDTS.getCollationDerivation(); 
					firstCollationType = valueNodeDTS.getCollationType(); 
				} else if (!foundCollationMisMatch)
				{
					if (firstCollationDerivation != valueNodeDTS.getCollationDerivation())
						foundCollationMisMatch = true;//collation derivations don't match
					else if (firstCollationType != valueNodeDTS.getCollationType())
						foundCollationMisMatch = true;//collation types don't match
				}
			}
			if (dominantDTS == null)
			{
				dominantDTS = valueNodeDTS;
			}
			else
			{
				dominantDTS = dominantDTS.getDominantType(valueNodeDTS, getClassFactory());
			}
		}

		//if following if returns true, then it means that we are dealing with 
		//string operands.
		if (firstCollationDerivation != -1)
		{
			if (foundCollationMisMatch) {
				//if we come here that it means that alll the string operands
				//do not have matching collation information on them. Hence the
				//resultant dominant DTD should have collation derivation of 
				//NONE.
				dominantDTS =
                    dominantDTS.getCollatedType(
                            dominantDTS.getCollationType(),
                            StringDataValue.COLLATION_DERIVATION_NONE);
			}			
			//if we didn't find any collation mismatch, then resultant dominant
			//DTD already has the correct collation information on it and hence
			//we don't need to do anything.
		}

		return dominantDTS;
	}

	/**
	 * Get the first non-null DataTypeServices from the elements in the list.
	 *
	 * @return DataTypeServices		The first non-null DataTypeServices.
	 *
	 * @exception StandardException		Thrown on error
	 */
    DataTypeDescriptor getTypeServices() throws StandardException
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            ValueNode valueNode = elementAt(index);
			DataTypeDescriptor valueNodeDTS = valueNode.getTypeServices();

			if (valueNodeDTS != null)
			{
				return valueNodeDTS;
			}
		}

		return null;
	}

	/**
	 * Return whether or not all of the entries in the list have the same
	 * type precendence as the specified value.
	 *
	 * @param precedence	The specified precedence.
	 *
	 * @return	Whether or not all of the entries in the list have the same
	 *			type precendence as the specified value.
	 */
	boolean allSamePrecendence(int precedence)
	{
		boolean allSame = true;
		int size = size();

		for (int index = 0; index < size; index++)
		{
			ValueNode			valueNode;

            valueNode = elementAt(index);
			DataTypeDescriptor valueNodeDTS = valueNode.getTypeServices();

			if (valueNodeDTS == null)
			{
				return false;
			}

			if (precedence != valueNodeDTS.getTypeId().typePrecedence())
			{
				return false;
			}
		}

		return allSame;
	}


	/**
	 * Make sure that passed ValueNode's type is compatible with the non-parameter elements in the ValueNodeList.
	 *
	 * @param leftOperand	Check for compatibility against this parameter's type
	 *
	 */
    void compatible(ValueNode leftOperand) throws StandardException
	{
        TypeId leftType = leftOperand.getTypeId();
        TypeCompiler leftTC = leftOperand.getTypeCompiler();

        for (ValueNode valueNode : this)
		{
            if (valueNode.requiresTypeFromContext()) {
				continue;
            }

			/*
			** Are the types compatible to each other?  If not, throw an exception.
			*/
			if (! leftTC.compatible(valueNode.getTypeId()))
			{
				throw StandardException.newException(SQLState.LANG_DB2_COALESCE_DATATYPE_MISMATCH,
						leftType.getSQLTypeName(),
						valueNode.getTypeId().getSQLTypeName()
						);
			}
		}
	}

	/**
	 * Determine whether or not the leftOperand is comparable() with all of
	 * the elements in the list. Throw an exception if any of them are not 
	 * comparable.
	 *
	 * @param leftOperand	The left side of the expression
	 *
	 * @exception StandardException		Thrown on error
	 */
    void comparable(ValueNode leftOperand) throws StandardException
	{
		int			 size = size();
		ValueNode		valueNode;

		for (int index = 0; index < size; index++)
		{
            valueNode = elementAt(index);

			/*
			** Can the types be compared to each other?  If not, throw an
			** exception.
			*/
			if (! leftOperand.getTypeServices().comparable(valueNode.getTypeServices(),
									false,
									getClassFactory()))
			{
				throw StandardException.newException(SQLState.LANG_NOT_COMPARABLE, 
						leftOperand.getTypeServices().getSQLTypeNameWithCollation(),
						valueNode.getTypeServices().getSQLTypeNameWithCollation()
						);
			}
		}
	}

	/** 
	 * Determine whether or not any of the elements in the list are nullable.
	 *
	 * @return boolean	Whether or not any of the elements in the list 
	 *					are nullable.
	 */
    boolean isNullable()
	throws StandardException
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            if (elementAt(index).getTypeServices().isNullable())
			{
				return true;
			}
		}
		return false;
	}
										 
	/**
	 * Does this list contain a ParameterNode?
	 *
	 * @return boolean	Whether or not the list contains a ParameterNode
	 */
    boolean containsParameterNode()
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            if (elementAt(index).requiresTypeFromContext())
			{
				return true;
			}
		}
		return false;
	}
										 
	/**
	 * Does this list contain all ParameterNodes?
	 *
	 * @return boolean	Whether or not the list contains all ParameterNodes
	 */
    boolean containsAllParameterNodes()
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            if (! (elementAt(index).requiresTypeFromContext()))
			{
				return false;
			}
		}
		return true;
	}

	/**
	 * Does this list contain all ConstantNodes?
	 *
	 * @return boolean	Whether or not the list contains all ConstantNodes
	 */
    boolean containsAllConstantNodes()
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            if (! (elementAt(index) instanceof ConstantNode))
			{
				return false;
			}
		}
		return true;
	}

	/**
	 * Does this list *only* contain constant and/or parameter nodes?
	 *
	 * @return boolean	True if every node in this list is either a constant
	 *  node or parameter node.
	 */
    boolean containsOnlyConstantAndParamNodes()
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            ValueNode vNode = elementAt(index);
			if (!vNode.requiresTypeFromContext() &&
			    !(vNode instanceof ConstantNode))
			{
				return false;
			}
		}

		return true;
	}

	/**
	 * Sort the entries in the list in ascending order.
	 * (All values are assumed to be constants.)
	 *
	 * @param judgeODV  In case of type not exactly matching, the judging type.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void sortInAscendingOrder(DataValueDescriptor judgeODV)
		throws StandardException
	{
		int size = size();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(size > 0,
				"size() expected to be non-zero");
		}

		/* We use bubble sort to sort the list since we expect
		 * the list to be in sorted order > 90% of the time.
		 */
		boolean continueSort = true;

		while (continueSort)
		{
			continueSort = false;
			
			for (int index = 1; index < size; index++)
			{
				ConstantNode currCN = (ConstantNode) elementAt(index);
				DataValueDescriptor currODV =
					 currCN.getValue();
				ConstantNode prevCN = (ConstantNode) elementAt(index - 1);
				DataValueDescriptor prevODV =
					 prevCN.getValue();

				/* Swap curr and prev if prev > curr */
				if ((judgeODV == null && (prevODV.compare(currODV)) > 0) ||
					(judgeODV != null && judgeODV.greaterThan(prevODV, currODV).equals(true)))
				{
					setElementAt(currCN, index - 1);
					setElementAt(prevCN, index);
					continueSort = true;
				}
			}
		}
	}

    /**
     * Eliminate NotNodes in all the nodes in this list.
     *
     * @param underNotNode whether or not we are under a NotNode
     * @see ValueNode#eliminateNots(boolean)
     */
    void eliminateNots(boolean underNotNode) throws StandardException {
        for (int i = 0; i < size(); i++) {
            setElementAt(elementAt(i).eliminateNots(underNotNode), i);
        }
    }

	/**
	 * Set the descriptor for every ParameterNode in the list.
	 *
	 * @param descriptor	The DataTypeServices to set for the parameters
	 *
	 * @exception StandardException		Thrown on error
	 */
    void setParameterDescriptor(DataTypeDescriptor descriptor)
						throws StandardException
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            ValueNode valueNode = elementAt(index);
			if (valueNode.requiresTypeFromContext())
			{
				valueNode.setType(descriptor);
			}
		}
	}

	/**
	 * Preprocess a ValueNodeList.  For now, we just preprocess each ValueNode
	 * in the list.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @exception StandardException		Thrown on error
	 */
    void preprocess(int numTables,
							FromList outerFromList,
							SubqueryList outerSubqueryList,
							PredicateList outerPredicateList) 
		throws StandardException
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            ValueNode vn = elementAt(index).preprocess(numTables,
								 outerFromList, outerSubqueryList,
								 outerPredicateList);
            setElementAt(vn, index);
		}
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return ValueNodeList			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
    ValueNodeList remapColumnReferencesToExpressions()
		throws StandardException
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
            setElementAt(elementAt(index).remapColumnReferencesToExpressions(),
                         index);
		}
		return this;
	}

    /**
     * Check if all the elements in this list are equivalent to the elements
     * in another list. The two lists must have the same size, and the
     * equivalent nodes must appear in the same order in both lists, for the
     * two lists to be equivalent.
     *
     * @param other the other list
     * @return {@code true} if the two lists contain equivalent elements, or
     * {@code false} otherwise
     * @throws StandardException thrown on error
     * @see ValueNode#isEquivalent(ValueNode)
     */
    boolean isEquivalent(ValueNodeList other) throws StandardException {
        if (size() != other.size()) {
            return false;
        }

        for (int i = 0; i < size(); i++) {
            ValueNode vn1 = elementAt(i);
            ValueNode vn2 = other.elementAt(i);
            if (!vn1.isEquivalent(vn2)) {
                return false;
            }
        }

        return true;
    }

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
    boolean isConstantExpression()
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
			boolean retcode;

            retcode = elementAt(index).isConstantExpression();
			if (! retcode)
			{
				return retcode;
			}
		}

		return true;
	}

	/** @see ValueNode#constantExpression */
    boolean constantExpression(PredicateList whereClause)
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
			boolean retcode;

            retcode = elementAt(index).constantExpression(whereClause);
			if (! retcode)
			{
				return retcode;
			}
		}

		return true;
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
	 * @exception StandardException		Thrown on error
	 */
    boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		/* We stop here when only considering simple predicates
		 *  as we don't consider in lists when looking
		 * for null invariant predicates.
		 */
		boolean pushable = true;
		int size = size();

		for (int index = 0; index < size; index++)
		{
            pushable = elementAt(index).categorize(referencedTabs, simplePredsOnly) &&
					   pushable;
		}

		return pushable;
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *		CONSTANT			- constant
	 *
	 * @return	The variant type for the underlying expression.
	 * @exception StandardException	thrown on error
	 */
	protected int getOrderableVariantType() throws StandardException
	{
		int listType = Qualifier.CONSTANT;
		int size = size();

		/* If any element in the list is VARIANT then the 
		 * entire expression is variant
		 * else it is SCAN_INVARIANT if any element is SCAN_INVARIANT
		 * else it is QUERY_INVARIANT.
		 */
		for (int index = 0; index < size; index++)
		{
            int curType = elementAt(index).getOrderableVariantType();
			listType = Math.min(listType, curType);
		}

		return listType;
	}
}
