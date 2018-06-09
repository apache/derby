/*

   Derby - Class org.apache.derby.impl.sql.catalog.XPLAINTableDescriptor

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;

public abstract class XPLAINTableDescriptor
{
    public abstract String getCatalogName();
    protected abstract SystemColumn []buildColumnList();

    private String tableInsertStmt;

    public String[]getTableDDL(String schemaName)
    {
        String escapedSchema = IdUtil.normalToDelimited(schemaName);
        String escapedTableName = IdUtil.normalToDelimited(getCatalogName());
        SystemColumn []cols = buildColumnList();
        StringBuffer buf = new StringBuffer();
        StringBuffer insBuf = new StringBuffer();
        StringBuffer valsBuf = new StringBuffer();
        for (int c = 0; c < cols.length; c++)
        {
            if (c == 0)
            {
                buf.append("(");
                insBuf.append("(");
                valsBuf.append("(");
            }
            else
            {
                buf.append(",");
                insBuf.append(",");
                valsBuf.append(",");
            }
            buf.append(cols[c].getName());
            insBuf.append(cols[c].getName());
            valsBuf.append("?");
            buf.append(" ");
            buf.append(cols[c].getType().getCatalogType().getSQLstring());
        }
        buf.append(")");
        insBuf.append(")");
        valsBuf.append(")");
        String query = 
            "create table " + escapedSchema + "." + escapedTableName +
            buf.toString();

        // FIXME -- need to create the index, too.

        tableInsertStmt =
            "insert into " + escapedSchema + "." + escapedTableName +
            insBuf.toString() + " values " + valsBuf.toString();

        return new String[]{query};
    }
    public String getTableInsert()
    {
        return tableInsertStmt;
    }
}

