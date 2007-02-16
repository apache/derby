package org.apache.derbyTesting.functionTests.util.streams;

import java.io.StringReader;

public class StringReaderWithLength extends StringReader {

    private final int length;
    
    public StringReaderWithLength(String arg0) {
        super(arg0);
        this.length = arg0.length();
    }
    
    public boolean markSupported() {
        return false;
    }

    public int getLength()
    {
        return length;
    }
}
