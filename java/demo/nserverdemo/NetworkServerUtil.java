/*
 * (C) Copyright IBM Corp. 2003, 2004.
 *
 * The source code for this program is not published or otherwise divested
 * of its trade secrets, irrespective of what has been deposited with the
 * U.S. Copyright Office.
 */

package nserverdemo;

import java.util.Properties;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.io.IOException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.io.PrintWriter;
import java.net.InetAddress;

import org.apache.derby.drda.NetworkServerControl; //derby network server
import java.io.FileOutputStream;

/**
 * Class for starting the Derby NetworkServer on a separate Thread.
 * This class provides methods to start, and shutdown the server
 *
 * <P>
 * <I>IBM Corp. reserves the right to change, rename, or
 * remove this interface at any time.</I>
 */

public class NetworkServerUtil  {

    private int portNum;
    private NetworkServerControl serverControl;
	private PrintWriter pw;

    public NetworkServerUtil(int port, PrintWriter pw) {

        this.portNum = port;
		this.pw = pw;
        try {
          serverControl = new
			  NetworkServerControl(InetAddress.getByName("localhost"), port);
          pw.println("Derby Network Server created");
        } catch (Exception e) {
            e.printStackTrace();
          }
    }

    /**
     * trace utility of server
     */
    public void trace(boolean onoff) {
      try {
        serverControl.trace(onoff);
      } catch (Exception e) {
          e.printStackTrace();
        }
    }


	/**
	 * Try to test for a connection
	 * Throws exception if unable to get a connection
	 */
	public void testForConnection()
	throws Exception {
		serverControl.ping();
	}


    /**
     * Shutdown the NetworkServer
     */
    public void shutdown() {
        try {
            serverControl.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


	/**
	 * Start Derby Network server
	 * 
	 */
    public void start() {
        try {
			serverControl.start(pw);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}



