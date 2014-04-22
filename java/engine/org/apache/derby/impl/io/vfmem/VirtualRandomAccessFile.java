/*

   Derby - Class org.apache.derby.impl.io.vfmem.VirtualRandomAccessFile

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

package org.apache.derby.impl.io.vfmem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.derby.io.StorageRandomAccessFile;

/**
 * A random access file capable of reading and writing from/into a virtual file
 * whose data is represented by a {@code BlockedByteArray}.
 * <p>
 * If the file is opened in read-only mode and the caller invokes one of the
 * write methods, it will fail with a {@code NullPointerException}.
 */
public class VirtualRandomAccessFile
        implements StorageRandomAccessFile {

    /** The source entry. */
    private final DataStoreEntry entry;
    /** whether the file is read-only */
    private final   boolean _readOnly;
    /** Current position / file pointer. */
    private long fp;
    /** Stream used to read from the source entry. */
    private final BlockedByteArrayInputStream bIn;
    /** Data input stream on top of the source input stream. */
    private final DataInputStream dIs;
    /**
     * Stream used to write into the source entry. Will be {@code null} if the
     * file is opened in read-only mode.
     */
    private final BlockedByteArrayOutputStream bOut;
    /**
     * Data output stream on top of the source output stream. Will be
     * {@code null} if the file is opened in read-only mode.
     */
    private final DataOutputStream dOs;

    /**
     * Creates a new virtual random access file.
     *
     * @param entry the source entry
     * @param readOnly if the file should be opened read-only or not
     * @throws FileNotFoundException if the denoted path is a directory, or
     *      the denoted file has been marked read-only and the file is opened
     *      for writing
     */
    public VirtualRandomAccessFile(DataStoreEntry entry, boolean readOnly)
            throws FileNotFoundException {
        this.entry = entry;
        _readOnly = readOnly;
        bIn = entry.getInputStream();
        bIn.setPosition(0L);
        dIs = new DataInputStream(bIn);
        // Only create writeable streams if the mode isn't read-only.
        if (readOnly) {
            bOut = null;
            dOs = null;
        } else {
            bOut = entry.getOutputStream(true);
            bOut.setPosition(0L);
            dOs = new DataOutputStream(bOut);
        }
    }

    public  VirtualRandomAccessFile clone()
    {
        try {
            return new VirtualRandomAccessFile( entry, _readOnly );
        }
        catch (IOException ioe)
        {
            throw new RuntimeException( ioe.getMessage(), ioe );
        }
    }

    public void close() throws IOException {
        dIs.close();
        // If opened in read-only mode, the output streams are null.
        if (dOs != null) {
            dOs.close();
        }
        fp = Long.MIN_VALUE;
    }

    public long getFilePointer() {
        return fp;
    }

    public long length() {
        return entry.length();
    }

    public void seek(long newFilePointer) throws IOException {
        if (newFilePointer < 0) {
            throw new IOException("Negative position: " + newFilePointer);
        }
        fp = newFilePointer;
        bIn.setPosition(newFilePointer);
        // Output streams are null if opened in read-only mode.
        if (bOut != null) {
            bOut.setPosition(newFilePointer);
        }
    }

    public void setLength(long newLength) {
        if (bOut == null) {
            throw new NullPointerException();
        }
        entry.setLength(newLength);
        // If truncation took place, check file pointer.
        if (newLength < fp) {
            fp = newLength;
        }
    }

    public void sync() {
        // Do nothing, everything is already synced.
    }

    public int read(byte[] b, int off, int len) throws IOException {
        int ret = bIn.read(b, off, len);
        fp = bIn.getPosition();
        return ret;
    }

    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        dIs.readFully(b, off, len);
        fp = bIn.getPosition();
    }

    public int skipBytes(int n) {
        if (n <= 0) {
            return 0;
        }
        long skipped = Math.min(n, entry.length() - fp);
        fp += skipped;
        return (int)skipped;
    }

    public boolean readBoolean() throws IOException {
        boolean ret = dIs.readBoolean();
        fp = bIn.getPosition();
        return ret;
    }

    public byte readByte() throws IOException {
        byte ret = dIs.readByte();
        fp = bIn.getPosition();
        return ret;
    }

    public int readUnsignedByte() throws IOException {
        int ret = dIs.readUnsignedByte();
        fp = bIn.getPosition();
        return ret;
    }

    public short readShort() throws IOException {
        short ret = dIs.readShort();
        fp = bIn.getPosition();
        return ret;
    }

    public int readUnsignedShort() throws IOException {
        int ret = dIs.readUnsignedShort();
        fp = bIn.getPosition();
        return ret;
    }

    public char readChar() throws IOException {
        char ret = dIs.readChar();
        fp = bIn.getPosition();
        return ret;
    }

    public int readInt() throws IOException {
        int ret = dIs.readInt();
        fp = bIn.getPosition();
        return ret;
    }

    public long readLong() throws IOException {
        long ret = dIs.readLong();
        fp = bIn.getPosition();
        return ret;
    }

    public float readFloat() throws IOException {
        float ret = dIs.readFloat();
        fp = bIn.getPosition();
        return ret;
    }

    public double readDouble() throws IOException {
        double ret = dIs.readDouble();
        fp = bIn.getPosition();
        return ret;
    }

    public String readLine() throws IOException {
        throw new UnsupportedOperationException("readLine");
    }

    public String readUTF() throws IOException {
        String utfStr = dIs.readUTF();
        fp = bIn.getPosition();
        return utfStr;
    }

    public void write(int b) throws IOException {
        dOs.write(b);
        fp = bOut.getPosition();
    }

    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        dOs.write(b, off, len);
        fp = bOut.getPosition();
    }

    public void writeBoolean(boolean v) throws IOException {
        dOs.writeBoolean(v);
        fp = bOut.getPosition();
    }

    public void writeByte(int v) throws IOException {
        dOs.writeByte(v);
        fp = bOut.getPosition();
    }

    public void writeShort(int v) throws IOException {
        dOs.writeShort(v);
        fp = bOut.getPosition();
    }

    public void writeChar(int v) throws IOException {
        dOs.writeChar(v);
        fp = bOut.getPosition();
    }

    public void writeInt(int v) throws IOException {
        dOs.writeInt(v);
        fp = bOut.getPosition();
    }

    public void writeLong(long v) throws IOException {
        dOs.writeLong(v);
        fp = bOut.getPosition();
    }

    public void writeFloat(float v) throws IOException {
        dOs.writeFloat(v);
        fp = bOut.getPosition();
    }

    public void writeDouble(double v) throws IOException {
        dOs.writeDouble(v);
        fp = bOut.getPosition();
    }

    public void writeBytes(String s) throws IOException {
        dOs.writeBytes(s);
        fp = bOut.getPosition();
    }

    public void writeChars(String s) throws IOException {
        dOs.writeChars(s);
        fp = bOut.getPosition();
    }

    public void writeUTF(String s) throws IOException {
        dOs.writeUTF(s);
        fp = bOut.getPosition();
    }
}
