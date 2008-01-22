/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.RoleDescriptor

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
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.catalog.DDdependableFinder;

/**
 * This class is used by rows in the SYS.SYSROLES system table.
 */
public class RoleDescriptor extends TupleDescriptor
{
    private final UUID uuid;
    private final String roleName;
    private final String grantee;
    private final String grantor;
    private boolean withAdminOption;
    private final boolean isDef; // if true, represents a role
                                 // definition, else a grant

    /**
     * Constructor
     *
     * @param dd data dictionary
     * @param uuid  unique identification in time and space of this role
     *              descriptor
     * @param roleName
     * @param grantee
     * @param grantor
     * @param withAdminOption
     * @param isDef
     *
     */
    RoleDescriptor(DataDictionary dd,
                   UUID uuid,
                   String roleName,
                   String grantee,
                   String grantor,
                   boolean withAdminOption,
                   boolean isDef) {
        super(dd);
        this.uuid = uuid;
        this.roleName = roleName;
        this.grantee = grantee;
        this.grantor = grantor;
        this.withAdminOption = withAdminOption;
        this.isDef = isDef;
    }

    public UUID getUUID() {
        return uuid;
    }

    public String getGrantee() {
        return grantee;
    }

    public String getGrantor() {
        return grantor;
    }

    public boolean isDef() {
        return isDef;
    }

    public String getRoleName() {
        return roleName;
    }

    public boolean isWithAdminOption() {
        return withAdminOption;
    }

    public void setWithAdminOption(boolean b) {
        withAdminOption = b;
    }

    public String toString() {
        if (SanityManager.DEBUG) {
            return "uuid: " + uuid + "\n" +
                "roleName: " + roleName + "\n" +
                "grantor: " + grantor + "\n" +
                "grantee: " + grantee + "\n" +
                "withadminoption: " + withAdminOption + "\n" +
                "isDef: " + isDef + "\n";
        } else {
            return "";
        }
    }

    public String getDescriptorType()
    {
        return "Role";
    }

    public String getDescriptorName()
    {
        return roleName + " " + grantor + " " + grantee;
    }


    /**
     * Drop this role.descriptor
     *
     * @throws StandardException Could not be dropped.
     */
    public void drop(LanguageConnectionContext lcc) throws StandardException
    {
        DataDictionary dd = getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();

        dd.dropRoleDescriptor(roleName, grantee, grantor, tc);
    }
}
