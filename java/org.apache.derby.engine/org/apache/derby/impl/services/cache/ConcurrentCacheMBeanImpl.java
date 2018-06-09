/*

   Derby - Class org.apache.derby.impl.services.cache.ConcurrentCacheMBeanImpl

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

package org.apache.derby.impl.services.cache;

import java.security.AccessControlException;
import java.security.AccessController;
import org.apache.derby.mbeans.CacheManagerMBean;
import org.apache.derby.shared.common.security.SystemPermission;

/**
 * This class provides monitoring capabilities for ConcurrentCache through
 * Java Management Extension (JMX).
 */
final class ConcurrentCacheMBeanImpl implements CacheManagerMBean {

    private final ConcurrentCache cache;

    ConcurrentCacheMBeanImpl(ConcurrentCache cache) {
        this.cache = cache;
    }

    @Override
    public void setCollectAccessCounts(boolean collect) {
        checkPermission();
        cache.setCollectAccessCounts(collect);
    }

    @Override
    public boolean getCollectAccessCounts() {
        checkPermission();
        return cache.getCollectAccessCounts();
    }

    @Override
    public long getHitCount() {
        checkPermission();
        return cache.getHitCount();
    }

    @Override
    public long getMissCount() {
        checkPermission();
        return cache.getMissCount();
    }

    @Override
    public long getEvictionCount() {
        checkPermission();
        return cache.getEvictionCount();
    }

    @Override
    public long getMaxEntries() {
        checkPermission();
        return cache.getMaxEntries();
    }

    @Override
    public long getAllocatedEntries() {
        checkPermission();
        return cache.getAllocatedEntries();
    }

    @Override
    public long getUsedEntries() {
        checkPermission();
        return cache.getUsedEntries();
    }

    private static void checkPermission() {
        if (System.getSecurityManager() != null) {
            try {
                AccessController.checkPermission(
                        SystemPermission.ENGINE_MONITOR);
            } catch (AccessControlException ace) {
                // Need to throw a simplified version as AccessControlException
                // will have a reference to Derby's SystemPermission class,
                // which most likely will not be available on the client.
                throw new SecurityException(ace.getMessage());
            }
        }
    }
}
