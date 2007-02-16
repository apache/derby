package org.apache.derbyTesting.functionTests.util.streams;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ReadOnceByteArrayInputStream extends ByteArrayInputStream {

    private boolean isClosed;
    
    public ReadOnceByteArrayInputStream(byte[] arg0) {
        super(arg0);
    }

    public ReadOnceByteArrayInputStream(byte[] arg0, int arg1, int arg2) {
        super(arg0, arg1, arg2);
    }
    
    public boolean markSupported()
    {
        return false;
    }
    
    public void close() throws IOException
    {
        isClosed = true;
        super.close();
    }
    
    public int read(byte[] b,
            int off,
            int len)
    {
        if (isClosed)
            return -1;
        return super.read(b, off, len);
    }
    
    public int read()
    {
        if (isClosed)
            return -1;
        return super.read();
    }

}
