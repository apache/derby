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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.EnumSet;

/**
 * <p>
 * Class that limits file permissions using a {@code PosixFileAttributeView}
 * or an {@code AclFileAttributeView}.
 * </p>
 *
 * <p>
 * For use by {@link FileUtil#limitAccessToOwner(File)}.
 * </p>
 */
final class FilePermissionServiceImpl implements FilePermissionService {
    public boolean limitAccessToOwner(File file) throws IOException {
        Path fileP = file.toPath();

        PosixFileAttributeView posixView = Files.getFileAttributeView(
                fileP, PosixFileAttributeView.class);
        if (posixView != null) {

            // This is a POSIX file system. Usually,
            // FileUtil.limitAccessToOwnerViaFile() will successfully set
            // the permissions on such file systems using the java.io.File
            // class, so we don't get here. If, however, that approach failed,
            // we try again here using a PosixFileAttributeView. That's likely
            // to fail too, but at least now we will get an IOException that
            // explains why it failed.

            EnumSet<PosixFilePermission> perms = EnumSet.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE);

            if (file.isDirectory()) {
                perms.add(PosixFilePermission.OWNER_EXECUTE);
            }

            posixView.setPermissions(perms);

            return true;
        }

        AclFileAttributeView aclView = Files.getFileAttributeView(
                fileP, AclFileAttributeView.class);
        if (aclView != null) {

            // Since we have an AclFileAttributeView which is not a
            // PosixFileAttributeView, we probably have an NTFS file
            // system.

            // Remove existing ACEs, build a new one which simply
            // gives all possible permissions to current owner.
            AclEntry ace = AclEntry.newBuilder()
                    .setPrincipal(Files.getOwner(fileP))
                    .setType(AclEntryType.ALLOW)
                    .setPermissions(EnumSet.allOf(AclEntryPermission.class))
                    .build();

            aclView.setAcl(Collections.singletonList(ace));

            return true;
        }

        // We don't know how to set permissions on this file system.
        return false;
    }
}
