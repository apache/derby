package org.apache.derby.iapi.jdbc;

/**
 * Additional methods the embedded engine exposes on its Blob object
 * implementations. An internal api only, mainly for the network
 * server.  
 * 
 */

public interface EngineClob {
    /**
     * Return lob locator key that can be used with 
     * EmbedConnection.getLobMapping(int) to retrieve this Clob.
     * 
     * @return lob locator for this Clob
     */
    public int getLocator();
}
