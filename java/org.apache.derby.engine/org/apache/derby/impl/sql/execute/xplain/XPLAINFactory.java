/*

   Derby - Class org.apache.derby.impl.sql.execute.xplain.XPLAINFactory

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINFactoryIF;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

/**
 * This is the module implementation of the XPLAINFactoryIF. It gets lazy-loaded
 * when needed. The factory method determines which visitor to use. 
 * The visitor is cached in this factory for later reuse. 
 *
 */
public class XPLAINFactory implements XPLAINFactoryIF {
    
    /** the last instance of a visitor is cached */
    private XPLAINVisitor currentVisitor = new XPLAINDefaultVisitor();
    
    /** the current cached schema */ 
    private String       currentSchema = null;
    
    public XPLAINFactory(){
    }
    
    /**
     * the factory method, which gets called to determine 
     * and return an appropriate XPLAINVisitor instance
     */
    public XPLAINVisitor getXPLAINVisitor()
        throws StandardException
    {
        try
        {
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            String schema = lcc.getXplainSchema();
            if (schema != currentSchema)
            {
                currentSchema = schema;
                if (currentSchema == null)
                    currentVisitor = new XPLAINDefaultVisitor();
                else
                    currentVisitor = new XPLAINSystemTableVisitor();
            }
        }
        catch (SQLException e)
        {
            throw StandardException.plainWrapException(e);
        }
        return currentVisitor;
    }

    /**
     * uncache the visitor and reset the factory state
     */
    public void freeResources() {
        // let the garbage collector destroy the visitor and schema
        currentVisitor = null;
        currentSchema = null;
    }

}
