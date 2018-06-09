/*

   Derby - Class org.apache.derby.impl.sql.compile.WindowNode

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;

/**
 * Superclass of window definition and window reference.
 */
public abstract class WindowNode extends QueryTreeNode
{
    /**
     * The provided name of the window if explicitly defined in a window
     * clause. If the definition is inlined, currently the definition has
     * windowName "IN_LINE".  The standard 2003 sec. 4.14.9 calls for a
     * implementation defined one.
     */
    private String windowName;


    /**
     * Constructor
     *
     * @param windowName The window name
     * @param cm         The context manager
     *
     * @exception StandardException
     */
    WindowNode(String windowName, ContextManager cm) throws StandardException
    {
        super(cm);
        this.windowName = windowName;
    }

    /**
     * @return the name of this window
     */
    public String getName() {
        return windowName;
    }
}
