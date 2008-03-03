/*

   Derby - Class org.apache.derby.catalog.types.OldRoutineType

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
package org.apache.derby.catalog.types;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.TypeId;

/**
 * Class to simply read the old format written by
 * DataTypeDescriptor prior to DERBY-2775 being addressed.
 * The format was incorrect used
 * in system catalogs for routine parameter and return
 * types. The format contained repeated information.
 * DERBY-2775 changed the code so that these catalog
 * types were written as TypeDescriptor (which is what
 * always had occurred for the types in SYSCOLUMNS).
 */
final class OldRoutineType implements Formatable {
    
    private TypeDescriptor catalogType;

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        
        // Redundant old TypeId object, just ignore,
        // comprised of two parts the old wrapper format number
        // and then a BaseTypeId. Information was duplicated
        // in the catalog type.
        FormatIdUtil.readFormatIdInteger(in);
        in.readObject(); 
        catalogType = (TypeDescriptor) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (SanityManager.DEBUG)
        {
            SanityManager.THROWASSERT("OldRoutineType must be read only!");
        }
    }

    public int getTypeFormatId() {
        return StoredFormatIds.DATA_TYPE_IMPL_DESCRIPTOR_V01_ID;
    }

    TypeDescriptor getCatalogType() {
        return catalogType;
    }
}
