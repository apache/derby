/*

   Derby - Class org.apache.derby.impl.sql.execute.xplain.XPLAINDefaultVisitor

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

package org.apache.derby.impl.sql.execute.xplain;

import java.sql.SQLException;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.stream.HeaderPrintWriter;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.RunTimeStatistics;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.iapi.sql.execute.ResultSetStatistics;
/**
 * This is the Default Visitor which produces explain information like the 
 * old getRuntimeStatistics() approach. <br/>
 * It exists to support backward-compatibility.
 * The only thing this visitor does, is to log the output of the statistics to the 
 * default log stream. (the file derby.log)
 *
 */
public class XPLAINDefaultVisitor implements XPLAINVisitor {

    public XPLAINDefaultVisitor(){
        // System.out.println("Default Style XPLAIN Visitor created");
    }

    public void visit(ResultSetStatistics statistics) {
        // default do nothing, because no traversal is done
    }

    public void reset() {
        // TODO Auto-generated method stub
        
    }

    public void doXPLAIN(RunTimeStatistics rss, Activation activation) {
        LanguageConnectionContext lcc;
        try {
            lcc = ConnectionUtil.getCurrentLCC();
            HeaderPrintWriter istream = lcc.getLogQueryPlan() ? Monitor.getStream() : null;
            if (istream != null){
                istream.printlnWithHeader(LanguageConnectionContext.xidStr + 
                      lcc.getTransactionExecute().getTransactionIdString() +
                      "), " +
                      LanguageConnectionContext.lccStr +
                      lcc.getInstanceNumber() +
                      "), " +
                      rss.getStatementText() + " ******* " +
                      rss.getStatementExecutionPlanText());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
    }

    public void setNumberOfChildren(int noChildren) {
        // do nothing
        
    }

}
