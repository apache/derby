/*

   Derby - Class org.apache.derby.iapi.services.io.FilePermissionServiceImpl

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
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.util.Collections;

/**
 * Class that limits file permissions using an {@code AclFileAttributeView}.
 * For use by {@link FileUtil#limitAccessToOwner(File)}.
 */
final class FilePermissionServiceImpl implements FilePermissionService {
    public boolean limitAccessToOwner(File file) throws IOException {
        Path fileP = file.toPath();
        FileStore fileStore = Files.getFileStore(fileP);

        // If we have a posix view, just return and fall back on
        // the JDK 6 approach.
        if (fileStore.supportsFileAttributeView(PosixFileAttributeView.class)) {
            return false;
        }

        if (!fileStore.supportsFileAttributeView(AclFileAttributeView.class)) {
            return false;
        }

        AclFileAttributeView aclView =
                Files.getFileAttributeView(fileP, AclFileAttributeView.class);
        if (aclView == null) {
            return false;
        }

        // Since we have an AclFileAttributeView which is not a
        // PosixFileAttributeView, we probably have an NTFS file
        // system.

        // Remove existing ACEs, build a new one which simply
        // gives all possible permissions to current owner.
        AclEntry ace = AclEntry.newBuilder()
                .setPrincipal(Files.getOwner(fileP))
                .setType(AclEntryType.ALLOW)
                .setPermissions(AclEntryPermission.values())
                .build();

        aclView.setAcl(Collections.singletonList(ace));

        return true;
    }
}
