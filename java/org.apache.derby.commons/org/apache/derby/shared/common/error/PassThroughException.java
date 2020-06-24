/*

   Derby - Class org.apache.derby.shared.common.error.PassThroughException

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

package org.apache.derby.shared.common.error;

/**
 * Unchecked exception class that can be used to pass checked exceptions out
 * of methods that are not declared to throw any checked exception.
 */
public final class PassThroughException extends RuntimeException {

    /**
     * Wrap a {@code Throwable} in this unchecked exception to allow it
     * to pass through methods that are not declared to raise this kind of
     * condition.
     *
     * @param cause the {@code Throwable} to pass through
     */
    public PassThroughException(Throwable cause) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4010
        super(cause);
    }

}
