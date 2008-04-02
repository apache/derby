/*

   Derby - Class org.apache.derby.impl.sql.conn.SQLSessionContextImpl

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

package org.apache.derby.impl.sql.conn;

import java.lang.String;
import org.apache.derby.iapi.sql.conn.SQLSessionContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

public class SQLSessionContextImpl implements SQLSessionContext {

    private String currentRole;
    private SchemaDescriptor currentDefaultSchema;

    public SQLSessionContextImpl (SchemaDescriptor sd) {
        currentRole = null;
        currentDefaultSchema = sd;
    }

    public void setRole(String role) {
        currentRole = role;
    }

    public String getRole() {
        return currentRole;
    }

    public void setDefaultSchema(SchemaDescriptor sd) {
        currentDefaultSchema = sd;
    }

    public SchemaDescriptor getDefaultSchema() {
        return currentDefaultSchema;
    }
}
