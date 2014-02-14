/*
   Derby - Class org.apache.derby.impl.sql.compile.WindowResultSetNode

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

import java.util.ArrayList;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;


/**
 * A WindowResultSetNode represents a result set for a window partitioning on a
 * select. Modeled on the code in GroupByNode.
 */
class WindowResultSetNode extends SingleChildResultSetNode
{
    /**
     * The parent to the WindowResultSetNode.  We generate a ProjectRestrict
     * over the windowing node and parent is set to that node.
     */
    FromTable   parent;
    List<WindowFunctionNode> windowFuncCalls;
    WindowDefinitionNode wdn;

    /**
     * Constructor for a WindowResultSetNode.
     *
     * @param bottomPR     The project restrict result set we want to wrap
     * @param windowDef    The window definition
     * @param windowFuncCalls
     *                     All window function calls in SELECT's select list
     *                     and order by list.
     * @param nestingLevel Nesting level
     * @param cm           The context manager
     *
     * @exception StandardException     Thrown on error
     */
    WindowResultSetNode(ResultSetNode            bottomPR,
                        WindowDefinitionNode     windowDef,
                        List<WindowFunctionNode> windowFuncCalls,
                        int                      nestingLevel,
                        ContextManager           cm) throws StandardException
    {
        super(bottomPR, null, cm);
        this.wdn = windowDef;
        this.windowFuncCalls = windowFuncCalls;
        setLevel(nestingLevel);

        ResultColumnList newBottomRCL;

        this.parent = this;

        /*
        ** The first thing we do is put ourselves on top of the SELECT.  The
        ** select becomes the childResult.  So our RCL becomes its RCL (so
        ** nodes above it now point to us).  Map our RCL to its columns.
        */
        newBottomRCL = childResult.getResultColumns().copyListAndObjects();
        setResultColumns( childResult.getResultColumns() );
        childResult.setResultColumns(newBottomRCL);

        // Wrao purselved int a project/restrict as per convention.
        addNewPRNode();

        // Add the extra result columns required
        addNewColumns();
    }

    /**
     * Add a new PR node.  Put the new PR under any sort.
     *
     * @exception standard exception
     */
    private void addNewPRNode()
        throws StandardException
    {
        /*
        ** Get the new PR, put above the WindowResultSetNode.
        */
        ResultColumnList rclNew = new ResultColumnList(getContextManager());

        for (ResultColumn rc : getResultColumns())
        {
            if (!rc.isGenerated()) {
                rclNew.addElement(rc);
            }
        }

        // if any columns in the source RCL were generated for an order by
        // remember it in the new RCL as well. After the sort is done it will
        // have to be projected out upstream.
        rclNew.copyOrderBySelect(getResultColumns());

        parent = new ProjectRestrictNode(this, // child
                                         rclNew,
                                         null, // havingClause,
                                         null, // restriction list
                                         null, // project subqueries
                                         null, // havingSubquerys,
                                         null, // tableProperties,
                                         getContextManager());


        /*
         * Reset the bottom RCL to be empty.
         */
        childResult.setResultColumns(new ResultColumnList(getContextManager()));

        /*
         * Set the Windowing RCL to be empty
         */
        setResultColumns( new ResultColumnList(getContextManager()) );


        // Add all referenced columns in select list to windowing node's RCL
        // and substitute references in original node to point to the Windowing
        // result set. (modelled on GroupByNode's action for addUnAggColumns)
        CollectNodesVisitor<ColumnReference> getCRVisitor =
            new CollectNodesVisitor<ColumnReference>(ColumnReference.class);

        parent.getResultColumns().accept(getCRVisitor);

        // Find all unique columns referenced and add those to windowing result
        // set.
        ArrayList<ValueNode> uniqueCols = new ArrayList<ValueNode>();
        for (ColumnReference cr : getCRVisitor.getList()) {
            if (!colRefAlreadySeen(uniqueCols, cr)) {
                uniqueCols.add(cr);
            }
        }

        // Add all virtual column select list to windowing node's RCL and
        // substitute references in original node to point to the Windowing
        // result set. Happens for example when we have a window over a group
        // by.
        CollectNodesVisitor<VirtualColumnNode> getVCVisitor =
            new CollectNodesVisitor<VirtualColumnNode>(VirtualColumnNode.class);

        parent.getResultColumns().accept(getVCVisitor);

        // Add any virtual columns to windowing result.
        uniqueCols.addAll(getVCVisitor.getList());

        ResultColumnList bottomRCL  = childResult.getResultColumns();
        ResultColumnList windowingRCL = getResultColumns();

        for (int i= 0; i< uniqueCols.size(); i++) {
            ValueNode crOrVcn = uniqueCols.get(i);

            ResultColumn newRC = new ResultColumn(
                    "##UnWindowingColumn",
                    crOrVcn,
                    getContextManager());

            // add this result column to the bottom rcl
            bottomRCL.addElement(newRC);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            newRC.setVirtualColumnId(bottomRCL.size());

            // now add this column to the windowing result column list
            ResultColumn wRC = new ResultColumn(
                    "##UnWindowingColumn",
                    crOrVcn,
                    getContextManager());
            windowingRCL.addElement(wRC);
            wRC.markGenerated();
            wRC.bindResultColumnToExpression();
            wRC.setVirtualColumnId(windowingRCL.size());

            /*
             ** Reset the original node to point to the
             ** Windowing result set.
             */
            VirtualColumnNode vc = new VirtualColumnNode(
                    this, // source result set.
                    wRC,
                    windowingRCL.size(),
                    getContextManager());

            SubstituteExpressionVisitor seVis =
                new SubstituteExpressionVisitor(crOrVcn, vc, null);
            parent.getResultColumns().accept(seVis);
        }
    }


    /**
     * @return {@code true} if an equivalent column reference to {@code cand}
     *         is already present in {@code uniqueColRefs}
     */
    private boolean colRefAlreadySeen(List<ValueNode> uniqueColRefs,
                                      ColumnReference cand)
            throws StandardException {

        for (int i= 0; i< uniqueColRefs.size(); i++) {
            ColumnReference cr = (ColumnReference)uniqueColRefs.get(i);

            if (cr.isEquivalent(cand)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Substitute new result columns for window function calls and add the
     * result columns to childResult's list of columns.
     */
    private void addNewColumns() throws StandardException {
        /*
         * Now process all of the window function calls.  Replace every
         * call with an RC.  We toss out the list of RCs, we need to get
         * each RC as we process its corresponding window function.
         */
        ResultColumnList bottomRCL  = childResult.getResultColumns();
        ResultColumnList windowingRCL = getResultColumns();

        ReplaceWindowFuncCallsWithCRVisitor replaceCallsVisitor =
            new ReplaceWindowFuncCallsWithCRVisitor(
                new ResultColumnList(getContextManager()),
                ((FromTable) childResult).getTableNumber(),
                ResultSetNode.class);
        parent.getResultColumns().accept(replaceCallsVisitor);

        for (int i=0; i < windowFuncCalls.size(); i++) {
            WindowFunctionNode winFunc = windowFuncCalls.get(i);

            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(
                    !(winFunc.getWindow() instanceof WindowReferenceNode),
                    "unresolved window-reference: " +
                    winFunc.getWindow().getName());
            }

            WindowDefinitionNode funcWindow =
                (WindowDefinitionNode)winFunc.getWindow();

            if (funcWindow == wdn) {
                ResultColumn newRC = new ResultColumn(
                    "##winFuncResult",
                    winFunc.getNewNullResultExpression(),
                    getContextManager());

                newRC.markGenerated();
                newRC.bindResultColumnToExpression();
                bottomRCL.addElement(newRC);
                newRC.setVirtualColumnId(bottomRCL.size());

                /*
                ** Set the WindowResultSetNode result column to point to this.
                ** The Windowing Node result was created when we called
                ** ReplaceWindowFuncCallsWithCRVisitor.
                */
                ColumnReference newColumnRef = new ColumnReference(
                        newRC.getName(), null, getContextManager());

                newColumnRef.setSource(newRC);
                newColumnRef.setNestingLevel(this.getLevel());
                newColumnRef.setSourceLevel(this.getLevel());
                newColumnRef.markGeneratedToReplaceWindowFunctionCall();

                ResultColumn tmpRC = new ResultColumn(
                    newRC.getColumnName(),
                    newColumnRef,
                    getContextManager());

                tmpRC.markGenerated();
                tmpRC.bindResultColumnToExpression();
                windowingRCL.addElement(tmpRC);
                tmpRC.setVirtualColumnId(windowingRCL.size());

                /*
                ** Set the column reference to point to
                ** this.
                */
                newColumnRef = winFunc.getGeneratedRef();

                if (newColumnRef != null) {
                    newColumnRef.setSource(tmpRC);
                } // Not generated, meaning it's no longer in use
            }
        }
    }


    /**
     * override
     * @see QueryTreeNode#generate
     */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
            throws StandardException
    {
        // Get the next ResultSet#, so we can number this ResultSetNode, its
        // ResultColumnList and ResultSet.

        assignResultSetNumber();

        // Get the final cost estimate from the child.
        setCostEstimate( childResult.getFinalCostEstimate() );


        acb.pushGetResultSetFactoryExpression(mb);

        int rclSize = getResultColumns().size();
        FormatableBitSet referencedCols = new FormatableBitSet(rclSize);

        /*
         * Build a FormatableBitSet for columns to copy from source.
         */

        for (int index = rclSize-1; index >= 0; index--) {
            ResultColumn rc = getResultColumns().elementAt(index);
            ValueNode expr = rc.getExpression();

            if (rc.isGenerated() &&
                    (expr instanceof ColumnReference) &&
                    ((ColumnReference)expr).
                        getGeneratedToReplaceWindowFunctionCall()) {

                // meaningless to copy these, they arise in this rs.
            } else {
                referencedCols.set(index);
            }
        }

        int erdNumber = acb.addItem(referencedCols);

        acb.pushThisAsActivation(mb); // arg 1

        childResult.generate(acb, mb);    // arg 2
        mb.upCast(ClassName.NoPutResultSet);

        /* row allocator */
        mb.push(acb.addItem(getResultColumns().buildRowTemplate())); // arg 3

        mb.push(getResultSetNumber()); //arg 4

        /* Pass in the erdNumber for the referenced column FormatableBitSet */
        mb.push(erdNumber); // arg 5

        /* There is no restriction at this level, we just want to pass null. */
        mb.pushNull(ClassName.GeneratedMethod); // arg 6

        mb.push(getCostEstimate().rowCount()); //arg 7
        mb.push(getCostEstimate().getEstimatedCost()); // arg 8

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
            "getWindowResultSet", ClassName.NoPutResultSet, 8);

    }



    /**
     * @return parent of this node, a PRN, used by SelectNode to retrieve new
     * top result set node after window result set rewrite of result set tree.
     */
    final FromTable getParent() {
        return parent;
    }


    /**
     * QueryTreeNode override
     * @see QueryTreeNode#printSubNodes
     */
    @Override
    public void printSubNodes(int depth) {
        if (SanityManager.DEBUG) {
			super.printSubNodes(depth);

            printLabel(depth, "wdn: ");
            wdn.treePrint(depth + 1);
        }
    }

}
