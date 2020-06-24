/*

   Derby - Class org.apache.derby.iapi.services.monitor.DerbyObservable

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

import java.util.ArrayList;

/**
 * <p>
 * Created to provide the Observable behavior which Derby has depended
 * on since Java 1.2 but which as deprecated in JDK 9 build 118. A DerbyObservable
 * is an object whose state changes are being tracked.
 * </p>
 */
public class DerbyObservable
{
    //////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    //////////////////////////////////////////////////////////////////////
  
    private boolean _hasChanged = false;
    private ArrayList<DerbyObserver> _observers;

    //////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    //////////////////////////////////////////////////////////////////////
  
    /** No-arg constructor */
    public DerbyObservable() { _observers = new ArrayList<DerbyObserver>(); }

    //////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    //////////////////////////////////////////////////////////////////////
  
    /**
     * Add another observer who wants to be told about changes to this object.
     *
     * @param observer The object which wants to be notified when this object changes
     *
     * @throws IllegalArgumentException If the argument is bad (e.g., null)
     */
    public synchronized void addObserver(DerbyObserver observer)
    {
      if (observer == null) { throw new IllegalArgumentException("Null arguments not allowed."); }

//IC see: https://issues.apache.org/jira/browse/DERBY-6891
      if (!_observers.contains(observer)) { _observers.add(observer); }
    }

    /**
     * Return the number of observers who are watching this object.
     *
     * @return The number of watchers
     */
    public synchronized int countObservers() { return _observers.size(); }

    /**
     * Remove a specific observer from the list of watchers. Null is ignored.
     *
     * @param observer The observer to remove.
     */
    public synchronized void deleteObserver(DerbyObserver observer) { _observers.remove(observer); }

    /**
     * This method is equivalent to notifyObservers(null);
     */
    public void notifyObservers() { notifyObservers(null); }
  
    /**
     * If this object has changed, then notify all observers. Pass
     * them this object and the extraInfo. This object is then marked
     * as unchanged again.
     *
     * @param extraInfo Extra information to be passed to the observer's callback method.
     */
    public void notifyObservers(Object extraInfo)
    {
        // Shield the observers from further changes to the list of watchers
        DerbyObserver[] cachedObservers;

        synchronized (this)
        {
            if (!_hasChanged) { return; }
            
            cachedObservers = new DerbyObserver[_observers.size()];
            _observers.toArray(cachedObservers);
            _hasChanged = false;
        }

        int lastIndex = cachedObservers.length - 1;
        for (int idx = lastIndex; idx >= 0; idx--)
        {
            cachedObservers[idx].update(this, extraInfo);
        }
    }
  
    //////////////////////////////////////////////////////////////////////
    //
    // PROTECTED BEHAVIOR TO BE CALLED BY SUBCLASSES
    //
    //////////////////////////////////////////////////////////////////////
  
    /**
     * When the object state changes, the object calls this method
     * in order to flag that fact. After this method has been called,
     * then the notifyObservers() will wake up the observers which are
     * watching this object.
     */
    protected synchronized void setChanged() { _hasChanged = true; }
  
}
