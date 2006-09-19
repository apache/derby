/*

   Derby - RAFContainerFactory

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.lang.reflect.Constructor;


/**
 * Constructs <code>RAFContainer</code> objects suitable for the running JVM.
 * Java 1.4 and above get <code>RAFContainer4</code> objects capable of
 * utilizing Java's New IO framework to perform thread-safe concurrent IO to a
 * container. Java 1.3 gets plain <code>RAFContainer<code> objects that
 * serialize all IOs to the container.
 * <p>
 * Derby's support for Java 1.3 is deprecated as of this writing, and when
 * support is ultimately removed this class can go away with it as
 * <code>RAFContainer4</code> is merged back into <code>RAFContainer</code>.
 */
class RAFContainerFactory {

    /**
     * Used to create <code>RAFContainer</code> or <code>RAFContainer4</code
     * objects as appropriate depending on what JVM we are running in.
     * <p>
     * MT: Immutable, initialized by constructor.
     *
     * @see newRAFContainer(BaseDataFileFactory factory)
     */
    protected final Constructor rafContainerConstructor;

    /**
     * Determines what JVM we're running in and loads the appropriate
     * <code>RAFContainer</code> class, and stores its Constructor in
     * rafContainerConstructor for use in
     * <code>newRafContainer(BaseDataFileFactory factory)</code>.
     */
    public RAFContainerFactory() {
        Constructor foundConstructor = null;
        try {
            Class containerClass;
            Class factoryClass = BaseDataFileFactory.class;

            if( JVMInfo.JDK_ID >= JVMInfo.J2SE_14) {
                containerClass = Class.forName(
                        "org.apache.derby.impl.store.raw.data.RAFContainer4");
            } else {
                containerClass = RAFContainer.class;
            }

            foundConstructor = containerClass.getDeclaredConstructor(
                    new Class[] {factoryClass});
        } catch (Exception e) {
            if(SanityManager.DEBUG) {
                SanityManager.DEBUG_PRINT("RAFContainerFactory",
                        "Caught exception when setting up rafContainerConstructor");
            }
            /*
             * If there's a problem we'll back away from this trick.
             * newRAFContainer() checks for null and uses a regular
             * reflectionless <code>return new RAFContainer(this)</code> if
             * that's the case.
             */
        } finally { // Ensure that rafContainerConstructor is defined
            rafContainerConstructor = foundConstructor;
        }
    }

    /**
     * Produces a <code>RAFContainer</code> object appropriate for this JVM.
     */
    public RAFContainer newRAFContainer(BaseDataFileFactory factory) {
        if(rafContainerConstructor != null) {
            try {
                return (RAFContainer) rafContainerConstructor.newInstance(
                            new BaseDataFileFactory[] {factory});
            } catch (Exception e) {
                if(SanityManager.DEBUG) {
                    SanityManager.DEBUG_PRINT("RAFContainerFactory",
                            "Caught exception when attempting to create RAFContainer object");
                }
                // Falls through and constructs old-style RAFContainer instead.
            }
        }
        /* If rafContainerConstructor is null, the static initializer may
         * have failed to load the JVM 1.4 class or its constructor */
        return new RAFContainer(factory);
    }
}
