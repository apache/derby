/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.SequenceDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.impl.sql.catalog.DDdependableFinder;

/**
 * This class describes rows in the SYS.SYSPERMS system table, which keeps track of the
 * permissions that have been granted but not revoked.
 */
public class PermDescriptor extends PermissionsDescriptor
        implements Provider {
    private String objectType;
    private UUID permObjectId;
    private String permission;
    private boolean grantable;

    /**
     * Constructor
     *
     * @param dataDictionary data dictionary
     * @param permUUID       unique identification in time and space of this perm
     *                       descriptor
     */

    public PermDescriptor(DataDictionary dataDictionary, UUID permUUID, String objectType,
                          UUID permObjectId, String permission, String grantor,
                          String grantee, boolean isGrantable) {
        super(dataDictionary, grantee, grantor);
        setUUID(permUUID);
        this.objectType = objectType;
        this.permObjectId = permObjectId;
        this.permission = permission;
        this.grantable = isGrantable;
    }

    public PermDescriptor(DataDictionary dd, UUID permUUID)
            throws StandardException {
        /*
    TODO When merging all permisions catalogs to this master catalog at a future date,
    this pattern which uses a partially initialised key descriptor should be cleaned up.
     */
        this(dd, permUUID, null, null, null, null, null, false);
    }

    public String getObjectType() {
        return objectType;
    }

    public UUID getPermObjectId() {
        return permObjectId;
    }

    public String getPermission() {
        return permission;
    }

    public boolean isGrantable() {
        return grantable;
    }

    public int getCatalogNumber() {
        return DataDictionary.SYSPERMS_CATALOG_NUM;
    }

    public String toString() {
        if (SanityManager.DEBUG) {
            return "permUUID: " + oid + "\n" +
                    "objectType: " + objectType + "\n" +
                    "permObjectId: " + permObjectId + "\n" +
                    "permission: " + permission + "\n" +
                    "grantable: " + grantable + "\n";
        } else {
            return "";
        }
    }

    /**
     * @return true iff the key part of this perm descriptor equals the key part of another perm
     *         descriptor.
     */
    public boolean equals(Object other) {
        if (!(other instanceof PermDescriptor))
            return false;
        PermDescriptor otherPerm = (PermDescriptor) other;
        return super.keyEquals(otherPerm) &&
                oid.equals(otherPerm.oid);
    }

    /**
     * @return the hashCode for the key part of this permissions descriptor
     */
    public int hashCode() {
        return super.keyHashCode() + oid.hashCode();
    }

    /**
     * @see PermissionsDescriptor#checkOwner
     */
    public boolean checkOwner(String authorizationId) throws StandardException {
        UUID sd = getDataDictionary().getAliasDescriptor(oid).getSchemaUUID();
        if (getDataDictionary().getSchemaDescriptor(sd, null).getAuthorizationId()
                .equals(authorizationId)) {
            return true;
        } else {
            return false;
        }
    }

    //////////////////////////////////////////////
    //
    // PROVIDER INTERFACE
    //
    //////////////////////////////////////////////

    /**
     * Return the name of this Provider.  (Useful for errors.)
     *
     * @return String   The name of this provider.
     */
    public String getObjectName() {
        return permission + "privilege on " + objectType;
    }

    /**
     * Get the provider's type.
     *
     * @return char         The provider's type.
     */
    public String getClassType() {
        return Dependable.PERM;
    }

    /**
     * @return the stored form of this provider
     * @see Dependable#getDependableFinder
     */
    public DependableFinder getDependableFinder() {
        return new DDdependableFinder(StoredFormatIds.PERM_DESCRIPTOR_FINDER_V01_ID);
    }

}
