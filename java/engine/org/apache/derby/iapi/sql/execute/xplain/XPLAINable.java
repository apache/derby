/*

   Derby - Class org.apache.derby.iapi.sql.execute.xplain.XPLAINable

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
/**
 * This interface has to be implemented by object structures, which want to
 * get explained. The current implementation let the ResultSetStatistics 
 * extend this Interface to be explainable.
 *
 */
public interface XPLAINable
{

    /**
     * This method gets called to let a visitor visit this XPLAINable object.
     * The general contract is to implement pre-order, depth-first traversal 
     * to produce a predictable traversal behaviour.  
     */
    public void accept(XPLAINVisitor visitor);

    // The methods below return descriptive information about the particular
    // result set. There are a few common implementations, and the various
    // ResultSetStatistics sub-classes override these methods when they
    // have more detailed information to provide. The visitor calls these
    // methods during xplain tree visiting.

    public String getRSXplainType();
    public String getRSXplainDetails();
    public Object getResultSetDescriptor(Object rsID, Object parentID,
            Object scanID, Object sortID, Object stmtID, Object timingID);
    public Object getResultSetTimingsDescriptor(Object rstID);
    public Object getSortPropsDescriptor(Object spID);
    public Object getScanPropsDescriptor(Object spID);
}
