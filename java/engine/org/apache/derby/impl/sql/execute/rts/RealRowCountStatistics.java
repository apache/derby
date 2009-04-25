/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealRowCountStatistics

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


import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.iapi.reference.SQLState;

import java.util.Vector;

public class RealRowCountStatistics
    extends RealNoPutResultSetStatistics
{

    /* Leave these fields public for object inspectors */
    public ResultSetStatistics childResultSetStatistics;

    // CONSTRUCTORS

    /**
     * Statistics for the RowCountResultSet
     */
    public  RealRowCountStatistics(
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
        ResultSetStatistics childResultSetStatistics)
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

    // ResultSetStatistics methods

    /**
     * Return the statement execution plan as a String.
     *
     * @param depth     Indentation level.
     *
     * @return String   The statement execution plan as a String.
     */
    public String getStatementExecutionPlanText(int depth) {

        String subqueryInfo = "";

        initFormatInfo(depth);

        return
            subqueryInfo +
            indent + MessageService.getTextMessage(SQLState.RTS_RC_RS) +
                " (" +  resultSetNumber + "):" + "\n" +
            indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
                " = " + numOpens + "\n" +
            indent + MessageService.getTextMessage(SQLState.RTS_ROWS_SEEN) +
                " = " + rowsSeen + "\n" +
            indent + MessageService.getTextMessage(
                                                SQLState.RTS_ROWS_FILTERED) +
                " = " + rowsFiltered + "\n" +
            dumpTimeStats(indent, subIndent) + "\n" +
            dumpEstimatedCosts(subIndent) + "\n" +
            indent + MessageService.getTextMessage(SQLState.RTS_SOURCE_RS) +
                ":" + "\n" +
            childResultSetStatistics.getStatementExecutionPlanText(sourceDepth);
    }

    /**
     * Return information on the scan nodes from the statement execution
     * plan as a String.
     *
     * @param depth     Indentation level.
     * @param tableName if not NULL then print information for this table only
     *
     * @return String   The information on the scan nodes from the
     *                  statement execution plan as a String.
     */
    public String getScanStatisticsText(String tableName, int depth) {
        return childResultSetStatistics.getScanStatisticsText(tableName, depth);
    }



    // Class implementation

    public String toString() {
        return getStatementExecutionPlanText(0);
    }


    /**
     * @see RealBasicNoPutResultSetStatistics#getChildren
     */
    public Vector getChildren() {
        Vector children = new Vector();
        children.addElement(childResultSetStatistics);

        return children;
    }


    /**
     * @see RealBasicNoPutResultSetStatistics#getNodeName
     */
    public String getNodeName() {
        return MessageService.getTextMessage(SQLState.RTS_RC);
    }

    public void accept(XPLAINVisitor visitor) {
        visitor.setNumberOfChildren(1);
        visitor.visit(this);
        childResultSetStatistics.accept(visitor);
    }
    public String getRSXplainType() { return XPLAINUtil.OP_ROW_COUNT; }
}
