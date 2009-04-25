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

