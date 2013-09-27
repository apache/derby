/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.TriggerDescriptor_v10_10

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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.io.StoredFormatIds;

/**
 * <p>
 * Representation of a {@code TriggerDescriptor} in the format used in version
 * 10.10 and earlier. To be used when reading descriptors written by an older
 * Derby version, or when writing a descriptor to a database that has been
 * soft-upgraded from an older Derby version.
 * </p>
 *
 * <p>
 * The format of trigger descriptors changed in version 10.11 when support for
 * the WHEN clause was added (DERBY-534).
 * </p>
 */
public class TriggerDescriptor_v10_10 extends TriggerDescriptor {

    /**
     * Niladic constructor, for formatable.
     */
    public TriggerDescriptor_v10_10() {
    }

    TriggerDescriptor_v10_10(
            DataDictionary dataDictionary,
            SchemaDescriptor sd,
            UUID id,
            String name,
            int eventMask,
            boolean isBefore,
            boolean isRow,
            boolean isEnabled,
            TableDescriptor td,
            UUID whenSPSId,
            UUID actionSPSId,
            Timestamp creationTimestamp,
            int[] referencedCols,
            int[] referencedColsInTriggerAction,
            String triggerDefinition,
            boolean referencingOld,
            boolean referencingNew,
            String oldReferencingName,
            String newReferencingName) {
        super(dataDictionary, sd, id, name, eventMask, isBefore, isRow,
              isEnabled, td, whenSPSId, actionSPSId, creationTimestamp,
              referencedCols, referencedColsInTriggerAction, triggerDefinition,
              referencingOld, referencingNew, oldReferencingName,
              newReferencingName, null);
    }

    @Override
    public int getTypeFormatId() {
        return StoredFormatIds.TRIGGER_DESCRIPTOR_V01_ID;
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        readExternal_v10_10(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        writeExternal_v10_10(out);
    }
}
