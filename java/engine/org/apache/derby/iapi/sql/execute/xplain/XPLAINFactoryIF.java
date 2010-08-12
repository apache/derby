/*

   Derby - Class org.apache.derby.iapi.sql.execute.xplain.XPLAINFactoryIF

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
import org.apache.derby.iapi.error.StandardException;
/**
 * This is the factory interface of the XPLAINFactory facility. It extends the 
 * possibilities and provides a convenient protocol to explain queries 
 * on basis of the query execution plan. This plan manfifests in Derby in the 
 * different ResultSets and their associated statistics. The introduction of 
 * this factory interface makes it possible to switch to another implementation 
 * or to easily extend the API.
 *  
 */
public interface XPLAINFactoryIF {

    /**
    Module name for the monitor's module locating system.
    */
    String MODULE = "org.apache.derby.iapi.sql.execute.xplain.XPLAINFactoryIF";
    
    /**
     * This method returns an appropriate visitor to traverse the 
     * ResultSetStatistics. Depending on the current configuration, 
     * the perfect visitor will be chosen, created and cached by this factory
     * method. 
     * @return a XPLAINVisitor to traverse the ResultSetStatistics
     * @see XPLAINVisitor
     */
    public XPLAINVisitor getXPLAINVisitor() throws StandardException;
    
    /**
     * This method gets called when the user switches off the explain facility.
     * The factory destroys for example the cached visitor implementation(s) or 
     * releases resources to save memory.
     */
    public void freeResources();
    
}
