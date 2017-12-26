/*
   Derby - Class org.apache.derby.iapi.error.ExceptionUtil
 
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
package org.apache.derby.iapi.error;
/* Until DERBY-289 related issue settle for shared code
 * Engine have similar code as client code even though some of 
 * code is potentially sharable. If you fix a bug in ExceptionUtil for engine, 
 * please also change the code in 
 * java/shared/org/apache/derby/shared/common/error/ExceptionUtil.java for 
 * client if necessary.
 */

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.error.ExceptionSeverity;

/**
 * This class provides utility routines for exceptions 
 */
public class ExceptionUtil
{


    /**
     *  Convert a message identifier from 
     *  org.apache.derby.shared.common.reference.SQLState to
     *  a SQLState five character string.
     *
     *	@param messageID - the sql state id of the message from Derby
     *	@return String 	 - the 5 character code of the SQLState ID to returned to the user 
     */
    public static String getSQLStateFromIdentifier(String messageID) {

        if (messageID.length() == 5)
            return messageID;
        return messageID.substring(0, 5);
    }
    
    /**
     * Get the severity given a message identifier from {@code SQLState}.
     *
     * @param messageID the string carrying the SQL state
     * @return the constant representing the severity, as defined in
     *        {@link org.apache.derby.iapi.error.ExceptionSeverity}.
     */
    public static int getSeverityFromIdentifier(String messageID) {

        int lseverity = ExceptionSeverity.NO_APPLICABLE_SEVERITY;

        switch (messageID.length()) {
        case 5:
            switch (messageID.charAt(0)) {
            case '0':
                switch (messageID.charAt(1)) {
                case '1':
                    lseverity = ExceptionSeverity.WARNING_SEVERITY;
                    break;
                case 'A':
                case '7':
                    lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
                    break;
                case '8':
                    lseverity = ExceptionSeverity.SESSION_SEVERITY;
                    break;
                }
                break;	
            case '2':
            case '3':
                lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
                break;
            case '4':
                switch (messageID.charAt(1)) {
                case '0':
                    lseverity = ExceptionSeverity.TRANSACTION_SEVERITY;
                    break;
                case '2':
                    lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
                    break;
                }
                break;	
            }
            break;

        default:
            switch (messageID.charAt(6)) {
            case 'M':
                lseverity = ExceptionSeverity.SYSTEM_SEVERITY;
                break;
            case 'D':
                lseverity = ExceptionSeverity.DATABASE_SEVERITY;
                break;
            case 'C':
                lseverity = ExceptionSeverity.SESSION_SEVERITY;
                break;
            case 'T':
                lseverity = ExceptionSeverity.TRANSACTION_SEVERITY;
                break;
            case 'S':
                lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
                break;
            case 'U':
                lseverity = ExceptionSeverity.NO_APPLICABLE_SEVERITY;
                break;
            }
            break;
        }

        return lseverity;
    }

    /**
     * Dumps stack traces for all the threads if the JVM supports it.
     * The result is returned as a string, ready to print.
     *
     * If the JVM doesn't have the method Thread.getAllStackTraces
     * i.e, we are on a JVM &lt; 1.5, or  if we don't have the permissions:
     * java.lang.RuntimePermission "getStackTrace" and "modifyThreadGroup",
     * a message saying so is returned instead.
     *
     * @return stack traces for all live threads as a string or an error message.
     */
    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch", "UseSpecificCatch"})
    public static String dumpThreads() {

        StringWriter out = new StringWriter();
        PrintWriter p = new PrintWriter(out, true);

        //Try to get a thread dump and deal with various situations.
        try {
            //This checks that we are on a jvm >= 1.5 where we
            //can actually do threaddumps.
            Thread.class.getMethod("getAllStackTraces", new Class<?>[] {});

            //Then get the thread dump.
            Class<?> c = Class.forName("org.apache.derby.iapi.error.ThreadDump");
            final Method m = c.getMethod("getStackDumpString",new Class<?>[] {});

            String dump;

            dump = (String) AccessController.doPrivileged
            (new PrivilegedExceptionAction<Object>(){
                public Object run() throws
                IllegalArgumentException,
                IllegalAccessException,
                InvocationTargetException{
                    return m.invoke(null, (Object[]) null);
                }
            }
            );

            //Print the dump to the message string. That went OK.
            p.print("---------------\nStack traces for all " +
            "live threads:");
            p.println("\n" + dump);
            p.println("---------------");
        } catch (NoSuchMethodException e) {
            p.println("(Skipping thread dump because it is not " +
            "supported on JVM 1.4)");

        } catch (Exception e) {
            if (e instanceof PrivilegedActionException &&
                e.getCause() instanceof InvocationTargetException &&
                e.getCause().getCause() instanceof AccessControlException){

                p.println("(Skipping thread dump "
                        + "because of insufficient permissions:\n"
                        + e.getCause().getCause() + ")\n");
            } else {
                p.println("\nAssertFailure tried to do a thread dump, but "
                        + "there was an error:");
                e.getCause().printStackTrace(p);
            }
        }
        return out.toString();
    }

    /**
     * Determine if the given {@code SQLState} string constant is a deferred
     * constraint transactional error. If this is so, return {@code true}, else
     * return {@code false}.
     *
     * @param e the string with the SQL state
     * @return see method description
     */
    public static boolean isDeferredConstraintViolation(String e) {
        return
            e.equals(
                ExceptionUtil.getSQLStateFromIdentifier(
                    SQLState.LANG_DEFERRED_DUPLICATE_KEY_CONSTRAINT_T)) ||
            e.equals(
                ExceptionUtil.getSQLStateFromIdentifier(
                    SQLState.LANG_DEFERRED_CHECK_CONSTRAINT_T));
    }
}
