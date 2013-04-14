/*

   Derby - Class org.apache.derby.client.ClientXid

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
package org.apache.derby.client;

import javax.transaction.xa.Xid;
import org.apache.derby.client.net.NetXAResource;
import org.apache.derby.shared.common.sanity.SanityManager;

public class ClientXid implements Xid {
    //
    // The format identifier for the Xid. A value of -1 indicates
    // that the NULLXid
    //
    private int formatID_;

    //
    // The number of bytes in the global transaction identfier
    //
    private int gtrid_length_;

    //
    // The number of bytes in the branch qualifier
    //
    private int bqual_length_;

    //
    // The data for the Xid.
    // <p> The Xid is made up of two contiguous parts. The first (of size
    // <b>gtrid_length</b>) is the global transaction identfier and the second
    // (of size <b>bqual_length</b>) is the branch qualifier.
    // <p>If the <b>formatID</b> is -1, indicating the NULLXid, the data is
    //    ignored.
    //
    private byte data_[];

    //
    // The size of <b>data</b>.
    //
    static private final int XidDATASIZE = 128;

    //
    // The maximum size of the branch qualifier.
    //
    static private final int MAXBQUALSIZE = 64;

    static private final String hextab_ = "0123456789ABCDEF";


    //
    // Constructs a new null Xid.
    // <p>After construction the data within the Xid should be initialized.
    //
    public ClientXid() {
        data_ = new byte[XidDATASIZE];
        gtrid_length_ = 0;
        bqual_length_ = 0;
        formatID_ = -1;
    }

    //
    // another contructor
    //
    public ClientXid(int formatID, byte[] gtrid, byte[] bqual) {

        formatID_ = formatID;
        gtrid_length_ = gtrid.length;
        bqual_length_ = bqual.length;
        data_ = new byte[XidDATASIZE];
        System.arraycopy(gtrid, 0, data_, 0, gtrid_length_);
        System.arraycopy(bqual, 0, data_, gtrid_length_, bqual_length_);
    }

    //
    // Return a string representing this Xid for debugging
    //
    // @return the string representation of this Xid
    //
    public String toString() {
        StringBuffer d;             // Data String, in HeXidecimal
        String s;             // Resultant String
        int i;
        int v;
        int L;

        L = gtrid_length_ + bqual_length_;
        d = new StringBuffer(L + L);

        for (i = 0; i < L; i++) {
            // Convert data string to hex
            v = data_[i] & 0xff;
            d.append(hextab_.charAt(v / 16));
            d.append(hextab_.charAt(v & 15));
            if ((i + 1) % 4 == 0 && (i + 1) < L) {
                d.append(" ");
            }
        }

        s = "{ClientXid: " +
                "formatID(" + formatID_ + "), " +
                "gtrid_length(" + gtrid_length_ + "), " +
                "bqual_length(" + bqual_length_ + "), " +
                "data(" + d.toString() + ")" +
                "}";
        return s;
    }

    //
    // Returns the branch qualifier for this Xid.
    //
    // @return the branch qualifier
    //
    public byte[] getBranchQualifier() {
        byte[] bqual = new byte[bqual_length_];
        System.arraycopy(data_, gtrid_length_, bqual, 0, bqual_length_);
        return bqual;
    }

    //
    // Set the branch qualifier for this Xid.
    //
    // @param qual a Byte array containing the branch qualifier to be set. If
    // the size of the array exceeds MAXBQUALSIZE, only the first MAXBQUALSIZE
    // elements of qual will be used.
    //
    public void setBranchQualifier(byte[] qual) {
        bqual_length_ = qual.length > MAXBQUALSIZE ? MAXBQUALSIZE : qual.length;
        System.arraycopy(qual, 0, data_, gtrid_length_, bqual_length_);
    }

    //
    // Obtain the format identifier part of the Xid.
    //
    // @return Format identifier. -1 indicates a null Xid
    //
    public int getFormatId() {
        return formatID_;
    }

    //
    // Set the format identifier part of the Xid.
    //
    // @param Format identifier. -1 indicates a null Xid.
    //
    public void setFormatID(int formatID) {
        formatID_ = formatID;
    }

    //
    // Returns the global transaction identifier for this Xid.
    //
    // @return the global transaction identifier
    //
    public byte[] getGlobalTransactionId() {
        byte[] gtrid = new byte[gtrid_length_];
        System.arraycopy(data_, 0, gtrid, 0, gtrid_length_);
        return gtrid;
    }

    //
    // return fields of Xid
    //
    public byte[] getData() {
        return data_.clone();
    }

    public int getGtridLength() {
        return gtrid_length_;
    }

    public int getBqualLength() {
        return bqual_length_;
    }

    public int hashCode() {
        if (formatID_ == (-1)) {
            return (-1);
        }
        return formatID_ + gtrid_length_ - bqual_length_;
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if(obj instanceof Xid) {
            return NetXAResource.xidsEqual(this, (Xid)obj);
        } else {
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT(
                        "ClientXid#equals: object of unexpected type: " +
                        obj.getClass().getName());
            }
            return false;
        }
    }
} // class Xid
