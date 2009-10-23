/*
   Derby - Class org.apache.derby.impl.sql.compile.WindowFunctionNode

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.reference.SQLState;

import java.sql.Types;
import java.util.Vector;


/**
 * Superclass of any window function call.
 */
public abstract class WindowFunctionNode extends UnaryOperatorNode
{

    private WindowNode window; // definition or reference

    /*
    ** We wind up pushing all window function calls into a different
    ** resultColumnList.  When we do this (in replaceCallsWithColumnReference),
    ** we return a column reference and create a new result column.  This is
    ** used to store that result column.
    */
    private ResultColumn            generatedRC;
    private ColumnReference         generatedRef;

    /**
     * Initializer for a WindowFunctionNode
     * @param arg1 null (operand)
     * @param arg2 function mame (operator)
     * @param arg3 window node (definition or reference)
     * @exception StandardException
     */
    public void init(Object arg1, Object arg2, Object arg3)
    {
        super.init(arg1, arg2, null);
        this.window = (WindowNode)arg3;
    }

    /**
     * ValueNode override.
     * @see ValueNode#isConstantExpression
     */
    public boolean isConstantExpression()
    {
        return false;
    }

    /**
     * ValueNode override.
     * @see ValueNode#isConstantExpression
     */
    public boolean constantExpression(PredicateList whereClause)
    {
        // Without this, an ORDER by on ROW_NUMBER could get optimised away
        // if there is a restriction, e.g.
        //
        // SELECT -ABS(i) a, ROW_NUMBER() OVER () c FROM t
        //     WHERE i > 1 ORDER BY c DESC
        return false;
    }


    /**
     * @return window associated with this window function
     */
    public WindowNode getWindow() {
        return window;
    }


    /**
     * Set window associated with this window function call.
     * @param wdn window definition
     */
    public void setWindow(WindowDefinitionNode wdn) {
        this.window = wdn;
    }


    /**
     * ValueNode override.
     * @see ValueNode#bindExpression
     */
    public ValueNode bindExpression(
            FromList fromList,
            SubqueryList subqueryList,
            Vector  aggregateVector)
        throws StandardException
    {
        if (window instanceof WindowReferenceNode) {

            WindowDefinitionNode found =
                definedWindow(fromList.getWindows(), window.getName());

            if (found != null) {
                window = found;
            } else {
                throw StandardException.
                    newException(SQLState.LANG_NO_SUCH_WINDOW,
                                 window.getName());
            }
        }

        return this;
    }


    /**
     * @return if name matches a defined window (in windows), return the
     * definition of that window, else null.
     */
    private WindowDefinitionNode definedWindow(WindowList windows,
                                               String name) {
        for (int i=0; i < windows.size(); i++) {
            WindowDefinitionNode wdn =
                (WindowDefinitionNode)windows.elementAt(i);

            if (wdn.getName().equals(name)) {
                return wdn;
            }
        }
        return null;
    }


    /**
     * QueryTreeNode override.
     * @see QueryTreeNode#printSubNodes
     */

    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            super.printSubNodes(depth);

            printLabel(depth, "window: ");
            window.treePrint(depth + 1);
        }
    }

    /**
     * Replace window function calls in the expression tree with a
     * ColumnReference to that window function, append the aggregate to the
     * supplied RCL (assumed to be from the child ResultSetNode) and return the
     * ColumnReference.
     *
     * @param rcl   The RCL to append to.
     * @param tableNumber   The tableNumber for the new ColumnReference
     *
     * @return ValueNode    The (potentially) modified tree.
     *
     * @exception StandardException         Thrown on error
     */
    public ValueNode replaceCallsWithColumnReferences(ResultColumnList rcl,
                                                      int tableNumber)
        throws StandardException
    {
        /*
         * This call is idempotent.  Do the right thing if we have already
         * replaced ourselves.
         */
        if (generatedRef == null)
        {
            String                  generatedColName;
            CompilerContext         cc = getCompilerContext();
            generatedColName ="SQLCol" + cc.getNextColumnNumber();
            generatedRC = (ResultColumn) getNodeFactory().getNode(
                                            C_NodeTypes.RESULT_COLUMN,
                                            generatedColName,
                                            this,
                                            getContextManager());
            generatedRC.markGenerated();

            // Parse time.
            //
            generatedRef = (ColumnReference) getNodeFactory().getNode(
                                                C_NodeTypes.COLUMN_REFERENCE,
                                                generatedRC.getName(),
                                                null,
                                                getContextManager());

            // RESOLVE - unknown nesting level, but not correlated, so nesting
            // levels must be 0
            generatedRef.setSource(generatedRC);
            generatedRef.setNestingLevel(0);
            generatedRef.setSourceLevel(0);

            if (tableNumber != -1)
            {
                generatedRef.setTableNumber(tableNumber);
            }

            rcl.addResultColumn(generatedRC);


            // Mark the ColumnReference as being generated to replace a call to
            // a window function

            generatedRef.markGeneratedToReplaceWindowFunctionCall();
        }
        else
        {
            rcl.addResultColumn(generatedRC);
        }

        return generatedRef;
    }

    /**
     * Get the generated ColumnReference to this window function after the
     * parent called replaceCallsWithColumnReferences().
     * <p/>
     * There are cases where this will not have been done because the tree has
     * been re-written to eliminate the window function, e.g. for this query:
     * <p/><pre>
     *     {@code SELECT * FROM t WHERE EXISTS
     *           (SELECT ROW_NUMBER() OVER () FROM t)}
     * </pre><p/>
     * in which case the top PRN of the subquery sitting over a
     * WindowResultSetNode just contains a RC which is boolean constant {@code
     * true}. This means that the replaceCallsWithColumnReferences will not
     * have been called for {@code this}, so the returned {@code generatedRef}
     * is null.
     *
     * @return the column reference
     */
    public ColumnReference getGeneratedRef()
    {
        return generatedRef;
    }


    /**
     * Get the null result expression column.
     *
     * @return the value node
     *
     * @exception StandardException on error
     */
    public ValueNode    getNewNullResultExpression()
        throws StandardException
    {
        //
        // Create a result column with the aggregate operand
        // it.
        //
        return getNullNode(getTypeServices());
    }
}
