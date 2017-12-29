/*

   Derby - Class org.apache.derby.impl.sql.compile.WindowReferenceNode

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
 * Represents a reference to an explicitly defined window
 */
public final class WindowReferenceNode extends WindowNode
{
    /**
     * Constructor
     *
     * @param windowName The window name referenced
     * @param cm         The context manager
     *
     * @exception StandardException
     */
    WindowReferenceNode(String windowName, ContextManager cm)
        throws StandardException
    {
        super(windowName, cm);
    }

    @Override
    public String toString() {
        return "referenced window: " + getName() + "\n" +
            super.toString();
    }
}
