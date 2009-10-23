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

import java.util.Iterator;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;

import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.impl.sql.execute.AggregatorInfo;
import org.apache.derby.impl.sql.execute.AggregatorInfoList;


/**
 * A WindowResultSetNode represents a result set for a window partitioning on a
 * select. Modelled on the code in GroupByNode.
 */
public class WindowResultSetNode extends SingleChildResultSetNode
{
    /**
     * The parent to the WindowResultSetNode.  We generate a ProjectRestrict
     * over the windowing node and parent is set to that node.
     */
    FromTable   parent;
    Vector windowFuncCalls;
    WindowDefinitionNode wdn;

    /**
     * Intializer for a WindowResultSetNode.
     * @param bottomPR The project restrict result set we want to wrap
     * @param windowDef The window definition
     * @param windowFuncCalls All window function calls in SELECT's select list
     * and order by list.
     * @param nestingLevel Nesting level
     *
     * @exception StandardException     Thrown on error
     */
    public void init(
        Object bottomPR,
        Object windowDef,
        Object windowFuncCalls,
        Object nestingLevel) throws StandardException
    {
        super.init(bottomPR, null);
        this.wdn = (WindowDefinitionNode)windowDef;
        this.windowFuncCalls = (Vector)windowFuncCalls;
        setLevel(((Integer)nestingLevel).intValue());

        ResultColumnList newBottomRCL;

        this.parent = this;

        /*
        ** The first thing we do is put ourselves on top of the SELECT.  The
        ** select becomes the childResult.  So our RCL becomes its RCL (so
        ** nodes above it now point to us).  Map our RCL to its columns.
        */
        newBottomRCL = childResult.getResultColumns().copyListAndObjects();
        resultColumns = childResult.getResultColumns();
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
        ResultColumnList rclNew = (ResultColumnList)getNodeFactory().
            getNode(C_NodeTypes.RESULT_COLUMN_LIST,
                    getContextManager());

        int sz = resultColumns.size();
        for (int i = 0; i < sz; i++)
        {
            ResultColumn rc = (ResultColumn) resultColumns.elementAt(i);
            if (!rc.isGenerated()) {
                rclNew.addElement(rc);
            }
        }

        // if any columns in the source RCL were generated for an order by
        // remember it in the new RCL as well. After the sort is done it will
        // have to be projected out upstream.
        rclNew.copyOrderBySelect(resultColumns);

        parent = (FromTable) getNodeFactory().getNode(
                                        C_NodeTypes.PROJECT_RESTRICT_NODE,
                                        this, // child
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
        childResult.setResultColumns((ResultColumnList)
                                            getNodeFactory().getNode(
                                                C_NodeTypes.RESULT_COLUMN_LIST,
                                                getContextManager()));

        /*
         * Set the Windowing RCL to be empty
         */
        resultColumns = (ResultColumnList) getNodeFactory().getNode(
                                            C_NodeTypes.RESULT_COLUMN_LIST,
                                            getContextManager());


        // Add all referenced columns in select list to windowing node's RCL
        // and substitute references in original node to point to the Windowing
        // result set. (modelled on GroupByNode's action for addUnAggColumns)
        CollectNodesVisitor getCRVisitor =
            new CollectNodesVisitor(ColumnReference.class);

        ResultColumnList prcl = parent.getResultColumns();

        parent.getResultColumns().accept(getCRVisitor);

        Vector colRefs = getCRVisitor.getList();

        // Find all unique columns referenced and add those to windowing result
        // set.
        Vector uniqueCols = new Vector();
        for (int i= 0; i< colRefs.size(); i++) {
            ColumnReference cr = (ColumnReference)colRefs.elementAt(i);
            if (!colRefAlreadySeen(uniqueCols, cr)) {
                uniqueCols.add(cr);
            }
        }

        // Add all virtual column select list to windowing node's RCL and
        // substitute references in original node to point to the Windowing
        // result set. Happens for example when we have a window over a group
        // by.
        CollectNodesVisitor getVCVisitor =
            new CollectNodesVisitor(VirtualColumnNode.class);

        parent.getResultColumns().accept(getVCVisitor);
        Vector vcs = getVCVisitor.getList();

        // Add any virtual columns to windowing result.
        for (int i= 0; i< vcs.size(); i++) {
            uniqueCols.add(vcs.elementAt(i));
        }

        ResultColumnList bottomRCL  = childResult.getResultColumns();
        ResultColumnList windowingRCL = resultColumns;

        for (int i= 0; i< uniqueCols.size(); i++) {
            ValueNode crOrVcn = (ValueNode)uniqueCols.elementAt(i);

            ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    "##UnWindowingColumn",
                    crOrVcn,
                    getContextManager());

            // add this result column to the bottom rcl
            bottomRCL.addElement(newRC);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            newRC.setVirtualColumnId(bottomRCL.size());

            // now add this column to the windowing result column list
            ResultColumn wRC = (ResultColumn) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
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
            VirtualColumnNode vc = (VirtualColumnNode) getNodeFactory().getNode(
                    C_NodeTypes.VIRTUAL_COLUMN_NODE,
                    this, // source result set.
                    wRC,
                    new Integer(windowingRCL.size()),
                    getContextManager());

            SubstituteExpressionVisitor seVis =
                new SubstituteExpressionVisitor(crOrVcn, vc, null);
            parent.getResultColumns().accept(seVis);
        }
    }


    /**
     * @return true if an equivalent column reference to cand is already
     * present in uniqueColRefs
     */
    private boolean colRefAlreadySeen(Vector uniqueColRefs,
                                      ColumnReference cand)
            throws StandardException {

        for (int i= 0; i< uniqueColRefs.size(); i++) {
            ColumnReference cr = (ColumnReference)uniqueColRefs.elementAt(i);

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
        LanguageFactory lf =
            getLanguageConnectionContext().getLanguageFactory();

        ResultColumnList bottomRCL  = childResult.getResultColumns();
        ResultColumnList windowingRCL = resultColumns;

        ReplaceWindowFuncCallsWithCRVisitor replaceCallsVisitor =
            new ReplaceWindowFuncCallsWithCRVisitor(
                (ResultColumnList) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN_LIST,
                    getContextManager()),
                ((FromTable) childResult).getTableNumber(),
                ResultSetNode.class);
        parent.getResultColumns().accept(replaceCallsVisitor);

        for (int i=0; i < windowFuncCalls.size(); i++) {
            WindowFunctionNode winFunc =
                (WindowFunctionNode)windowFuncCalls.elementAt(i);

            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(
                    !(winFunc.getWindow() instanceof WindowReferenceNode),
                    "unresolved window-reference: " +
                    winFunc.getWindow().getName());
            }

            WindowDefinitionNode funcWindow =
                (WindowDefinitionNode)winFunc.getWindow();

            if (funcWindow == wdn) {
                ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    "##winFuncResult",
                    winFunc.getNewNullResultExpression(),
                    getContextManager());

                newRC.markGenerated();
                newRC.bindResultColumnToExpression();
                bottomRCL.addElement(newRC);
                newRC.setVirtualColumnId(bottomRCL.size());
                int winFuncResultVColId = newRC.getVirtualColumnId();

                /*
                ** Set the WindowResultSetNode result column to point to this.
                ** The Windowing Node result was created when we called
                ** ReplaceWindowFuncCallsWithCRVisitor.
                */
                ColumnReference newColumnRef =
                    (ColumnReference) getNodeFactory().getNode(
                        C_NodeTypes.COLUMN_REFERENCE,
                        newRC.getName(),
                        null,
                        getContextManager());

                newColumnRef.setSource(newRC);
                newColumnRef.setNestingLevel(this.getLevel());
                newColumnRef.setSourceLevel(this.getLevel());
                newColumnRef.markGeneratedToReplaceWindowFunctionCall();

                ResultColumn tmpRC = (ResultColumn) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
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
    public void generate(ActivationClassBuilder acb,
                         MethodBuilder mb)
            throws StandardException
    {
        // Get the next ResultSet#, so we can number this ResultSetNode, its
        // ResultColumnList and ResultSet.

        assignResultSetNumber();

        // Get the final cost estimate from the child.
        costEstimate = childResult.getFinalCostEstimate();


        acb.pushGetResultSetFactoryExpression(mb);

        int rclSize = resultColumns.size();
        FormatableBitSet referencedCols = new FormatableBitSet(rclSize);

        /*
         * Build a FormatableBitSet for columns to copy from source.
         */

        for (int index = rclSize-1; index >= 0; index--) {
            ResultColumn rc = (ResultColumn) resultColumns.elementAt(index);
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
        resultColumns.generateHolder(acb, mb); // arg 3

        mb.push(resultSetNumber); //arg 4

        /* Pass in the erdNumber for the referenced column FormatableBitSet */
        mb.push(erdNumber); // arg 5

        /* There is no restriction at this level, we just want to pass null. */
        mb.pushNull(ClassName.GeneratedMethod); // arg 6

        mb.push(costEstimate.rowCount()); //arg 7
        mb.push(costEstimate.getEstimatedCost()); // arg 8

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
            "getWindowResultSet", ClassName.NoPutResultSet, 8);

    }



    /**
     * @return parent of this node, a PRN, used by SelectNode to retrieve new
     * top result set node after window result set rewrite of result set tree.
     */
    public FromTable getParent() {
        return parent;
    }


    /**
     * QueryTreeNode override
     * @see QueryTreeNode#printSubNodes
     */
    public void printSubNodes(int depth) {
        if (SanityManager.DEBUG) {
			super.printSubNodes(depth);

            printLabel(depth, "wdn: ");
            wdn.treePrint(depth + 1);
        }
    }

}
