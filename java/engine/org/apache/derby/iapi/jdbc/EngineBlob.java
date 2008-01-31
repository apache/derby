package org.apache.derby.iapi.jdbc;

/**
 * Additional methods the embedded engine exposes on its Blob object
 * implementations. An internal api only, mainly for the network
 * server.  
 * 
 */

public interface EngineBlob {

    /**
     * Return lob locator key that can be used with 
     * EmbedConnection.getLobMapping(int) to retrieve this Blob.
     * 
     * @return lob locator for this Blob
     */
    public int getLocator();
}
