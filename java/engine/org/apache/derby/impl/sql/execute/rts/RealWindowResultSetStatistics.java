/*
   Derby - Class org.apache.derby.impl.sql.execute.rts.RealWindowResultSetStatistics

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

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.sql.execute.ResultSetStatistics;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.iapi.reference.SQLState;

/**
 * ResultSetStatistics implementation for WindowResultSet.
 */
public class RealWindowResultSetStatistics
    extends RealNoPutResultSetStatistics
{

    private ResultSetStatistics childResultSetStatistics;

    /**
     * Constructor.
     *
     */
    public  RealWindowResultSetStatistics(
                                int numOpens,
                                int rowsSeen,
                                int rowsFiltered,
                                long constructorTime,
                                long openTime,
                                long nextTime,
                                long closeTime,
                                int resultSetNumber,
                                double optimizerEstimatedRowCount,
                                double optimizerEstimatedCost,
                                ResultSetStatistics childResultSetStatistics
                                )
    {
        super(
            numOpens,
            rowsSeen,
            rowsFiltered,
            constructorTime,
            openTime,
            nextTime,
            closeTime,
            resultSetNumber,
            optimizerEstimatedRowCount,
            optimizerEstimatedCost
            );
        this.childResultSetStatistics = childResultSetStatistics;

    }

    // ResultSetStatistics interface

    /**
     * Return the statement execution plan as a String.
     *
     * @param depth Indentation level.
     *
     * @return String   The statement execution plan as a String.
     */
    public String getStatementExecutionPlanText(int depth)
    {
        initFormatInfo(depth);
        String WINDOWSPECIFICATION = "()";

        return
            indent + MessageService.getTextMessage(
                            SQLState.RTS_WINDOW_RS) +
            WINDOWSPECIFICATION + "\n" +
            indent + MessageService.getTextMessage(
                            SQLState.RTS_NUM_OPENS) +
                            " = " + numOpens + "\n" +
            indent + MessageService.getTextMessage(
                            SQLState.RTS_ROWS_SEEN) +
                            " = " + rowsSeen + "\n" +
            dumpTimeStats(indent, subIndent) + "\n" +
            dumpEstimatedCosts(subIndent) + "\n" +
            indent + MessageService.getTextMessage(
                SQLState.RTS_SOURCE_RS) + ":\n" +
            childResultSetStatistics.
                getStatementExecutionPlanText(sourceDepth) + "\n";
    }

    /**
     * Return information on the scan nodes from the statement execution
     * plan as a String.
     *
     * @param depth Indentation level.
     * @param tableName if not NULL then print information for this table only
     *
     * @return String   The information on the scan nodes from the
     *                  statement execution plan as a String.
     */
    public String getScanStatisticsText(String tableName, int depth)
    {
        return getStatementExecutionPlanText(depth);
    }


    // java.lang.Object override
    //
    public String toString()
    {
        return getStatementExecutionPlanText(0);
    }


    /**
     * RealBasicNoPutResultSetStatistics override.
     * @see RealBasicNoPutResultSetStatistics#getChildren
     */
    public java.util.Vector<ResultSetStatistics> getChildren()
    {
        java.util.Vector<ResultSetStatistics> children = new java.util.Vector<ResultSetStatistics>();
        children.addElement(childResultSetStatistics);
        return children;
    }


    /**
     * RealBasicNoPutResultSetStatistics override.
     * @see RealBasicNoPutResultSetStatistics#getNodeOn
     */
    public String getNodeOn(){
        return MessageService.getTextMessage(
                                    SQLState.RTS_FOR_TAB_NAME,
                                    "<WINDOW FUNCTION>");
    }


    /**
     * RealBasicNoPutResultSetStatistics override.
     * @see RealBasicNoPutResultSetStatistics#getNodeName
     */
    public String getNodeName(){
        return MessageService.getTextMessage(SQLState.RTS_IRTBR);
    }



    // -----------------------------------------------------
    // XPLAINable Implementation
    // -----------------------------------------------------

    public void accept(XPLAINVisitor visitor) {

        // I have only one child
        visitor.setNumberOfChildren(1);

        // pre-order, depth-first traversal me first
        visitor.visit(this);

        // then my child
        childResultSetStatistics.accept(visitor);
    }


    public String getRSXplainType() {
        return XPLAINUtil.OP_WINDOW;
    }
}
