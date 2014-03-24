/*

   Derby - Class org.apache.derby.iapi.services.io.FilePermissionService

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

package org.apache.derby.iapi.services.io;

import java.io.File;
import java.io.IOException;

/**
 * Interface for changing file permissions using the java.nio.file.attribute
 * package that is available on Java 7 and higher. The purpose of this
 * interface is to provide an indirection layer between the FileUtil class and
 * the java.nio.file.attribute package so that FileUtil can be compiled for
 * older platforms than Java 7.
 */
interface FilePermissionService {

    /**
     * Change the permissions of a file so that only the owner can access it.
     *
     * @param file the file whose permissions should be changed
     * @return {@code true} if the permissions were successfully changed, or
     * {@code false} if the implementation does not support changing
     * permissions on this file system
     * @throws IOException if an error happens when changing the permissions
     */
    boolean limitAccessToOwner(File file) throws IOException;

}
