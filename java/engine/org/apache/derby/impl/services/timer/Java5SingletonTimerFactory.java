/*

   Derby - Class org.apache.derby.impl.services.timer.Java5SingletonTimerFactory

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

package org.apache.derby.impl.services.timer;

import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extension of {@code SingletonTimerFactory} that takes advantage of the
 * new {@code Timer.purge()} method in Java 5.
 */
public class Java5SingletonTimerFactory extends SingletonTimerFactory {
    /**
     * The number of times {@link #cancel(TimerTask)} has been called.
     * Used for determining whether it's time to purge cancelled tasks from
     * the timer.
     */
    private final AtomicInteger cancelCount = new AtomicInteger();

    @Override public void cancel(TimerTask task) {
        super.cancel(task);

        // DERBY-6114: Cancelled tasks stay in the timer's queue until they
        // are scheduled to run, unless we call the purge() method. This
        // prevents garbage collection of the tasks. Even though the tasks
        // are small objects, there could be many of them, especially when
        // both the transaction throughput and tasks' delays are high, it
        // could lead to OutOfMemoryErrors. Since purge() could be a heavy
        // operation if the queue is big, we don't call it every time a task
        // is cancelled.
        if (cancelCount.incrementAndGet() % 1000 == 0) {
            getCancellationTimer().purge();
        }
    }
}
