/*
   Derby - Class org.apache.derbyTesting.junit.NetworkServerControlWrapper

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
package org.apache.derbyTesting.junit;

import java.io.PrintWriter;
import java.lang.reflect.Method;

/**
 * A class wrapping a {@code NetworkServerControl} instance, using reflection
 * to allow {@code TestConfiguration} to be used without having
 * <tt>derbynet.jar</tt> on the classpath.
 * <p>
 * Only methods actually required by {@code TestConfiguration} are wrapped, and
 * this class depends on the functionality implemented by
 * {@link org.apache.derbyTesting.junit.NetworkServerTestSetup}.
 * <p>
 * The problem before was that an exception was thrown during class loading
 * time, even if the network server functionality was never required by the
 * tests being run. With this wrapper, an exception will be thrown only if the
 * functionality is actually required and the necessary classes are not on the
 * classpath.
 */
public class NetworkServerControlWrapper {

    private static final int PING = 0;
    private static final int SHUTDOWN = 1;
    private static final int START = 2;

    /** Associated {@code NetworkServerControl} instance. */
    private final Object ctrl;

    /** Array with the various method objects. */
    private final Method[] METHODS = new Method[3];

    /**
     * Creates a new wrapper object.
     *
     * @throws Exception if creating the {@code NetworkServerControl} instance
     *      fails
     */
    NetworkServerControlWrapper()
            throws Exception {
        // Try to load the NetworkServerControl class.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        Class<?> clazzSC = null;
        try {
            clazzSC =
                    Class.forName("org.apache.derby.drda.NetworkServerControl");
        } catch (ClassNotFoundException cnfe) {
            BaseTestCase.fail("No runtime support for network server", cnfe);
        }
        Class<?> clazzTS = Class.forName(
                "org.apache.derbyTesting.junit.NetworkServerTestSetup");
        Method m = clazzTS.getMethod("getNetworkServerControl");
        // Invoke method to obtain the NSC instance.
        this.ctrl = m.invoke(null);

        // Create the NSC method objects.
        METHODS[PING] = clazzSC.getMethod("ping");
        METHODS[SHUTDOWN] = clazzSC.getMethod("shutdown");
        METHODS[START] = clazzSC.getMethod(
                "start", new Class[] {PrintWriter.class});
    }

    /**
     * Helper method that invokes a method returning {@code void}.
     *
     * @param methodIndex index of the method to invoke ({@link #METHODS})
     * @param args arguments to pass to the method being invoked
     * @throws Exception a broad range of exceptions can be thrown, both
     *      related to reflection and any exceptions the invoked methods
     *      themselves might throw
     */
    private final void invoke(int methodIndex, Object[] args)
            throws Exception {
        try {
            // Invoke the method with the passed in arguments.
            METHODS[methodIndex].invoke(this.ctrl, args);
        } catch (IllegalArgumentException iae) {
            // Something is off with the passed in arguments.
            BaseTestCase.fail("Test framework programming error", iae);
        }
    }

    // Method forwarding / invocation follows below //

    /** @see org.apache.derby.drda.NetworkServerControl#ping */
    public void ping()
            throws Exception {
        invoke(PING, null);
    }

    /** @see org.apache.derby.drda.NetworkServerControl#shutdown */
    public void shutdown()
            throws Exception {
        invoke(SHUTDOWN, null);
    }

    /** @see org.apache.derby.drda.NetworkServerControl#start */
    public void start(PrintWriter printWriter)
            throws Exception {
        invoke(START, new Object[] {printWriter});
    }
}
