/*

   Derby - Class org.apache.derby.client.net.DssConstants

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

class DssConstants {
    static final int MAX_DSS_LEN = 32767;

    // Registered SNA GDS identifier indicating DDM data (xD0 for DDM data).
    static final int GDS_ID = 0xD0;

    // GDS chaining bits.
    static final int GDSCHAIN = 0x40;

    // GDS chaining bits where next DSS has different correlation ID.
    static final int GDSCHAIN_SAME_ID = 0x10;

    // GDS formatter for an Encrypted OBJDSS.
    static final int GDSFMT_ENCOBJDSS = 0x04;

    // GDS formatter for an OBJDSS.
    static final int GDSFMT_OBJDSS = 0x03;

    // GDS formatter for an RPYDSS.
    static final int GDSFMT_RPYDSS = 0x02;

    // GDS formatter for an RQSDSS.
    static final int GDSFMT_RQSDSS = 0x01;


    // GDS formatter for an RQSDSS without a reply.
    static final int GDSFMT_RQSDSS_NOREPLY = 0x05;

    static final byte RQST_CHN_DIFFCOR_CONT = (byte) 0x61;
    static final byte RQST_CHN_DIFFCOR_NOCONT = (byte) 0x41;
    static final byte RQST_CHN_SAMECOR_CONT = (byte) 0x71;
    static final byte RQST_CHN_SAMECOR_NOCONT = (byte) 0x51;
    static final byte RQST_NOCHN_CONT = (byte) 0x21;
    static final byte RQST_NOCHN_NOCONT = (byte) 0x01;

    static final byte RPY_CHN_DIFFCOR_CONT = (byte) 0x62;
    static final byte RPY_CHN_DIFFCOR_NOCONT = (byte) 0x42;
    static final byte RPY_CHN_SAMECOR_CONT = (byte) 0x72;
    static final byte RPY_CHN_SAMECOR_NOCONT = (byte) 0x52;
    static final byte RPY_NOCHN_CONT = (byte) 0x22;
    static final byte RPY_NOCHN_NOCONT = (byte) 0x02;

    // hide the default constructor
    private DssConstants() {
    }
}
