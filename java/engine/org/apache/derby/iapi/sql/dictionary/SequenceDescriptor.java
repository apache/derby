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
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.impl.sql.catalog.DDdependableFinder;

/**
 * This class is used by rows in the SYS.SYSSEQUENCES system table.
 */
public class SequenceDescriptor extends TupleDescriptor
        implements Provider, UniqueSQLObjectDescriptor {

    private UUID sequenceUUID;
    private String sequenceName;
    private final SchemaDescriptor schemaDescriptor;
    private UUID schemaId;
    private DataTypeDescriptor dataType;
    private long currentValue;
    private long startValue;
    private long minimumValue;
    private long maximumValue;
    private long increment;
    private boolean cycle;

    /**
     * Constructor
     *
     * @param dataDictionary data dictionary
     * @param sequenceUUID   unique identification in time and space of this sequence
     *                       descriptor
     * @param sequenceName
     */

    public SequenceDescriptor(DataDictionary dataDictionary, SchemaDescriptor sd, UUID sequenceUUID, String sequenceName,
                              DataTypeDescriptor dataType, long currentValue,
                              long startValue, long minimumValue, long maximumValue, long increment, boolean cycle) {
        super(dataDictionary);
        if (SanityManager.DEBUG) {
            if (sd.getSchemaName() == null) {
                SanityManager.THROWASSERT("new SequenceDescriptor() schema " +
                        "name is null for Sequence " + sequenceName);
            }
        }
        this.sequenceUUID = sequenceUUID;
        this.schemaDescriptor = sd;
        this.sequenceName = sequenceName;
        this.schemaId = sd.getUUID();
        this.dataType = dataType;
        this.currentValue = currentValue;
        this.startValue = startValue;
        this.minimumValue = minimumValue;
        this.maximumValue = maximumValue;
        this.increment = increment;
        this.cycle = cycle;
    }

   /**
	 * @see UniqueTupleDescriptor#getUUID
	 */
	public UUID	getUUID()
	{
		return sequenceUUID;
	}

    public String toString() {
        if (SanityManager.DEBUG) {
            return "sequenceUUID: " + sequenceUUID + "\n" +
                    "sequenceName: " + sequenceName + "\n" +
                    "schemaId: " + schemaId + "\n" +
                    "dataType: " + dataType.getTypeName() + "\n" +
                    "currentValue: " + currentValue + "\n" +
                    "startValue: " + startValue + "\n" +
                    "minimumValue: " + minimumValue + "\n" +
                    "maximumValue: " + maximumValue + "\n" +
                    "increment: " + increment + "\n" +
                    "cycle: " + cycle + "\n";
        } else {
            return "";
        }
    }

    public String getName() {
        return sequenceName;
    }

    public SchemaDescriptor getSchemaDescriptor() throws StandardException {
        return schemaDescriptor;
    }

    //////////////////////////////////////////////
    //
    // PROVIDER INTERFACE
    //
    //////////////////////////////////////////////

    /**
     * Get the provider's UUID
     *
     * @return The provider's UUID
     */
    public UUID getObjectID() {
        return sequenceUUID;
    }

    /**
     * Is this provider persistent?  A stored dependency will be required
     * if both the dependent and provider are persistent.
     *
     * @return boolean              Whether or not this provider is persistent.
     */
    public boolean isPersistent() {
        return true;
    }

    /**
     * Return the name of this Provider.  (Useful for errors.)
     *
     * @return String   The name of this provider.
     */
    public String getObjectName() {
        return (sequenceName);
    }

    /**
     * Get the provider's type.
     *
     * @return char         The provider's type.
     */
    public String getClassType() {
        return Dependable.SEQUENCE;
    }

    /**
     * @return the stored form of this provider
     * @see Dependable#getDependableFinder
     */
    public DependableFinder getDependableFinder() {
        return new DDdependableFinder(StoredFormatIds.SEQUENCE_DESCRIPTOR_FINDER_V01_ID);
    }

    /*Accessor methods*/
    public String getSequenceName() {
        return sequenceName;
    }

    public UUID getSequenceUUID() {
        return sequenceUUID;
    }

    public UUID getSchemaId() {
        return schemaId;
    }

    public DataTypeDescriptor getDataType() {
        return dataType;
    }

    public long getCurrentValue() {
        return currentValue;
    }

    public long getStartValue() {
        return startValue;
    }

    public long getMinimumValue() {
        return minimumValue;
    }

    public long getMaximumValue() {
        return maximumValue;
    }

    public long getIncrement() {
        return increment;
    }

    public boolean isCycle() {
        return cycle;
    }
}
