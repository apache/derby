/*

   Derby - Class org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor

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

package org.apache.derby.iapi.sql.execute.xplain;

import java.sql.SQLException;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.RunTimeStatistics;
import org.apache.derby.iapi.sql.execute.ResultSetStatistics;
/**
 * Classes, which implement this interface have the ability to explain the
 * gathered ResultSetStatistics. A Visitor pattern is used to traverse the 
 * ResultSetStatistics tree and to extract the required information. Classes 
 * implementing this interface are responsible about what they extract 
 * and what will be done with the extracted information.
 * This approach allows easy representaion extensions of the statistics, 
 * e.g. an XML representation. 
 *
 */
public interface XPLAINVisitor {

    /**
     * Call this method to reset the visitor for a new run over the 
     * statistics. A default implementation should call this method
     * automatically at first of a call of doXPLAIN(). 
     */
    public void reset();
    
    /**
     * This method is the hook method which is called from the TopResultSet.
     * It starts the explanation of the current ResultSetStatistics tree 
     * and keeps the information during one explain run.
     */
    public void doXPLAIN(RunTimeStatistics rss, Activation activation)
        throws StandardException;

    /**
     * This is the Visitor hook method, which gets called from each 
     * ResultSetStatistics. It depends on the sub-class implementation of this
     * interface, to describe the behaviour of the explanation facility. <br/>
     * To be easily extendable with new explain representation methods, 
     * just implement this interface and provide the new behaviour. 
     * @param statistics the statistics, which want to get explained.
     */
    public void visit(ResultSetStatistics statistics);
    
    /**
     * This method informs the visitor about the number of children. It has to 
     * be called first! by the different explainable nodes before the visit 
     * method of the visitor gets called. Each node knows how many children he has. 
     * The visitor can use this information to resolve the relationship of the 
     * current explained node to above nodes. Due to the top-down, pre-order, 
     * depth-first traversal of the tree, this information can directly 
     * be exploited.  
     * @param noChildren the number of children of the current explained node.
     */
    public void setNumberOfChildren(int noChildren);
    
}
