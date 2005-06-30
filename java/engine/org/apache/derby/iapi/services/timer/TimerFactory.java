/*

   Derby - Class org.apache.derby.iapi.services.timer.TimerFactory

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.services.timer;

import java.util.Timer;

/**
 * This class provides access to Timer objects for various purposes.
 * The scheme for creation of Timer objects is implementation-defined.
 */
public interface TimerFactory
{
    /**
     * Returns a Timer object that can be used for adding TimerTasks
     * that cancel executing statements.
     *
     * @return a Timer object for cancelling statements.
     */
    public Timer getCancellationTimer();
}
