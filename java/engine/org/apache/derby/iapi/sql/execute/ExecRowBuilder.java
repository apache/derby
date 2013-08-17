/*

   Derby - Class org.apache.derby.iapi.sql.execute.ExecRowBuilder

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

package org.apache.derby.iapi.sql.execute;

import java.io.Serializable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * A class used for storing information on how to build {@code ExecRow}
 * instances. Typically created by the compiler and used during execution
 * to produce and reset row templates.
 */
public class ExecRowBuilder implements Serializable {

    /**
     * Serial version produced by the serialver utility. Needed in order to
     * make serialization work reliably across different compilers.
     */
    private static final long serialVersionUID = -1078823466492523202L;

    /** If true, the row should be an {@code ExecIndexRow}. */
    private final boolean indexable;

    /**
     * Array of templates used for creating NULL values to put in the row.
     * The templates are either {@code DataValueDescriptor}s or
     * {@code DataTypeDescriptor}s.
     */
    private final Object[] template;

    /** Array of 1-based column numbers for the columns to access. */
    private final int[] columns;

    /** The number of columns to set in the row. */
    private int count;

    /** The highest column number in the row. */
    private int maxColumnNumber;

    /**
     * Create an instance that produces an {@code ExecRow} instance of
     * the specified size.
     *
     * @param size the number of columns to initialize in the produced row
     * @param indexable {@code true} if the returned row should be an
     * {@code ExecIndexRow}, {@code false} otherwise
     */
    public ExecRowBuilder(int size, boolean indexable) {
        this.template = new Object[size];
        this.columns = new int[size];
        this.indexable = indexable;
    }

    /**
     * Add a template from which a NULL value of the correct type can be
     * created. It should either be a {@code DataValueDescriptor} or a
     * {@code DataTypeDescriptor}.
     *
     * @param column the column number
     * @param columnTemplate a template from which a NULL value can be created
     * (either a {@code DataValueDescriptor} or a {@code DataTypeDescriptor})
     */
    public void setColumn(int column, Object columnTemplate) {
        if (SanityManager.DEBUG &&
                !(columnTemplate instanceof DataTypeDescriptor) &&
                !(columnTemplate instanceof DataValueDescriptor)) {
            SanityManager.THROWASSERT(
                "Expected DataTypeDescriptor or DataValueDescriptor. Got: " +
                ((columnTemplate == null) ? columnTemplate :
                    columnTemplate.getClass().getName()));
        }
        template[count] = columnTemplate;
        columns[count] = column;
        count++;
        maxColumnNumber = Math.max(maxColumnNumber, column);
    }

    /**
     * Build a new {@code ExecRow} instance with the columns specified by
     * the {@link #setColumn(int, Object)} method initialized to empty (NULL)
     * values.
     *
     * @param ef an execution factory used to create a row
     * @return a row initialized with NULL values of the requested types
     */
    public ExecRow build(ExecutionFactory ef) throws StandardException {
        ExecRow row = indexable ?
                ef.getIndexableRow(maxColumnNumber) :
                ef.getValueRow(maxColumnNumber);

        for (int i = 0; i < count; i++) {
            Object o = template[i];
            DataValueDescriptor dvd = (o instanceof DataValueDescriptor) ?
                    ((DataValueDescriptor) o).getNewNull() :
                    ((DataTypeDescriptor) o).getNull();
            row.setColumn(columns[i], dvd);
        }

        return row;
    }

    /**
     * Reset a row by creating fresh NULL values.
     * @param row the row to reset
     */
    public void reset(ExecRow row) throws StandardException {
        for (int i = 0; i < count; i++) {
            int col = columns[i];
            row.setColumn(col, row.getColumn(col).getNewNull());
        }
    }
}
