/*

   Derby - Class org.apache.derby.client.net.FdocaSimpleDataArray

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

package org.apache.derby.client.net;

class FdocaSimpleDataArray {
    //---------------------navigational members-----------------------------------

    //-----------------------------state------------------------------------------

    // store the protocol type. this is needed to know
    // which protocol type the mdd override is for.
    int protocolType_;

    // the FD:OCA field type indicator shows exactly how the data is represented
    // in the environment.  see the FD:OCA reference for a detailed explanation of
    // these types.
    int fdocaFieldType_;

    // this is the representation used by the DNC converters.  this is like the
    // FD:OCA field type but the dnc converters don't use FD:OCA types as input.
    int representation_;

    // the ccsid identifies the encoding of the character data.  converting the
    // ccsid into binary form generates the four byte representation.  The
    // FD:OCA rules state that if the high order 16 bits of the CCSID field
    // are zero, then the low order 16 bits are to be interpreted as a CCSID
    int ccsid_;

    // indicates the number of bytes each character takes in storage.
    // 1 is used for character, date, time, timestamp, and numeric character fields.
    // it must be 0 for all other types.
    int characterSize_;

    // this is used to specify mode of interpretation of FD:OCA
    // architecture for all variable length data types (including null
    // terminated), that as the SBCS variable character type.  The
    // low order bit of this byte is used to control interpretation
    // of Length Fields in SDAs for variable length types.  A '0' in that
    // bit indicates that non-zero length field values indicate the space
    // reserved for data and that all the space is transmitted
    // whether or not it contains valid data.  In the case of variable
    // length types, the first two bytes of the data itself determine
    // the valid data length.  A '1' in this bit shows that non-zero length
    // field values indicate the maximum value of the length fields
    // that the data will contain.  Only enough space to contain each
    // data value is transmitted for each value.
    int mode_;

    // this represents the maximum valid value.  when and if a group
    // data array (GDA) triplet overrides it, the value can be reduced.
    // For character fields with only DBCS characters, this is the length in
    // characters (bytes/2).  For all other cases, the length is in bytes.
    // It does not include the length of the length field (variable length
    // types) or null indicator (nullable types).
    //
    int fieldLength_;

    // this is a group of types which indicates how the data length are computed.
    int typeToUseForComputingDataLength_;

    //---------------------constructors/finalizer---------------------------------

    FdocaSimpleDataArray(int protocolType,
                         int fdocaFieldType,
                         int representation,
                         int ccsid,
                         int characterSize,
                         int mode,
                         int fieldLength,
                         int typeToUseForComputingDataLength) {
        protocolType_ = protocolType;
        fdocaFieldType_ = fdocaFieldType;
        representation_ = representation;
        ccsid_ = ccsid;
        characterSize_ = characterSize;
        mode_ = mode;
        fieldLength_ = fieldLength;
        typeToUseForComputingDataLength_ = typeToUseForComputingDataLength;
    }

    public void update(int protocolType,
                       int fdocaFieldType,
                       int representation,
                       int ccsid,
                       int characterSize,
                       int mode,
                       int fieldLength,
                       int typeToUseForComputingDataLength) {
        protocolType_ = protocolType;
        fdocaFieldType_ = fdocaFieldType;
        representation_ = representation;
        ccsid_ = ccsid;
        characterSize_ = characterSize;
        mode_ = mode;
        fieldLength_ = fieldLength;
        typeToUseForComputingDataLength_ = typeToUseForComputingDataLength;
    }

}
