/*

   Derby - Class org.apache.derby.impl.sql.compile.WindowList

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

import org.apache.derby.iapi.services.context.ContextManager;

/**
 * A WindowList represents the list of windows (definitions) for a table
 * expression, either defined explicitly in a WINDOW clause, or inline in the
 * SELECT list or ORDER BY clause.
 *
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
class WindowList extends QueryTreeNodeVector<WindowDefinitionNode>
{
    WindowList(ContextManager cm) {
        super(WindowDefinitionNode.class, cm);
    }

    /**
     * @param window the window definition to add to the list
     */
    public void addWindow(WindowDefinitionNode window)
    {
        addElement(window);
    }
}
