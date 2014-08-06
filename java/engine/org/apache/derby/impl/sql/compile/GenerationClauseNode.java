/*

   Derby - Class org.apache.derby.impl.sql.compile.GenerationClauseNode

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
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.depend.ProviderList;

/**
 * This node describes a Generation Clause in a column definition.
 *
 */
class GenerationClauseNode extends ValueNode
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private ValueNode _generationExpression;
    private String      _expressionText;

    private ValueNode _boundExpression;
	private ProviderList _apl;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    GenerationClauseNode( ValueNode generationExpression,
                          String expressionText,
                          ContextManager cm)
    {
        super(cm);
        _generationExpression = generationExpression;
        _expressionText = expressionText;
	}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    //  ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the defining text of this generation clause */
    public  String  getExpressionText() { return _expressionText; }
    
	/** Set the auxiliary provider list. */
	void setAuxiliaryProviderList(ProviderList apl) { _apl = apl; }

	/** Return the auxiliary provider list. */
    ProviderList getAuxiliaryProviderList() { return _apl; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // QueryTreeNode BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	/**
	 * Binding the generation clause.
	 */
    @Override
    ValueNode bindExpression
        ( FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
        throws StandardException
	{
        _boundExpression = _generationExpression.bindExpression( fromList, subqueryList, aggregates );

        return _boundExpression;
	}

	/**
	 * Generate code for this node.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{
        throw StandardException.newException( SQLState.HEAP_UNIMPLEMENTED_FEATURE );
	}

    boolean isEquivalent(ValueNode other)
		throws StandardException
    {
        if (! isSameNodeKind(other)) {
            return false;
        }

        GenerationClauseNode    that = (GenerationClauseNode) other;

        return this._generationExpression.isEquivalent( that._generationExpression );
    }
    
	/**
	 * Return a list of columns referenced in the generation expression.
	 *
	 * @exception StandardException		Thrown on error
	 */
    public List<ColumnReference> findReferencedColumns()
        throws StandardException
    {
        CollectNodesVisitor<ColumnReference> visitor =
            new CollectNodesVisitor<ColumnReference>(ColumnReference.class);

        _generationExpression.accept( visitor );

        return visitor.getList();
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     *
     * @exception StandardException on error
     */
    @Override
    void acceptChildren(Visitor v) throws StandardException {

        super.acceptChildren(v);

        if (_generationExpression != null) {
            _generationExpression = (ValueNode)_generationExpression.accept(v);
        }

        if (_boundExpression != null) {
            _boundExpression = (ValueNode)_boundExpression.accept(v);
        }
    }

    /*
		Stringify.
	 */
    @Override
	public String toString()
    {
        return
            "expressionText: GENERATED ALWAYS AS ( " +
            _expressionText + " )\n" +
            super.toString();
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

            printLabel(depth, "generationExpression: ");
            _generationExpression.treePrint(depth + 1);

            if (_boundExpression != null) {
                printLabel(depth, "boundExpression. ");
                _boundExpression.treePrint(depth + 1);
            }
		}
	}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

}
