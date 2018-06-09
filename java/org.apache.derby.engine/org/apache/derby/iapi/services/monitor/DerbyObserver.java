/*

   Derby - Class org.apache.derby.iapi.services.monitor.DerbyObserver

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

package org.apache.derby.iapi.services.monitor;

/**
 * <p>
 * Created to provide the Observable behavior which Derby has depended
 * on since Java 1.2 but which as deprecated in JDK 9 build 118. A DerbyObserver
 * is an object which registers it interest in being notified when events occur.
 * </p>
 */
public interface DerbyObserver
{
    /**
     * This is the callback method which is invoked when a change happens
     * to the object which is being observed.
     *
     * @param observable The object which is being observed
     * @param extraInfo Extra information being passed to the callback
     */
    public void update(DerbyObservable observable, Object extraInfo);
}
