/*

   Derby - Class org.apache.derby.iapi.db.Database

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.db;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.services.i18n.LocaleFinder;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Date;
import java.util.Hashtable;
import java.util.Locale;

/**
 * The org.apache.derby.iapi.db.Database
 * interface provides "internal" methods on the database which are
 * not available to JBMS users (org.apache.derby.database.Database,
 * which this interface extends, provides all the externally visible
 * methods).
 * <P>
 * At the present moment, this file defines methods which will at 
 * some point be moved to to the external database interface.
 *
 * <B> There are a bunch of the unimplemninted interface that used to be in
 * this file.  They have been moved to old_Database.java.  old_Database.java is
 * checked into the codeline but is not built, it is there for reference </B>
 *
 * @author Jeff Lichtman
 */

public interface Database extends org.apache.derby.database.Database, LocaleFinder
{
	// this interface gets used on a module, so we name it:
	// Note that doers not point to this class name, but instead to
	// the public API for this class. This ensures that the name
	// written in service.properties is not a obfuscated one.
	
	/**
	 * Sets up a connection to the Database, owned by the given user.
	 *
	 * The JDBC version of getConnection takes a URL. The purpose
	 * of the URL is to tell the driver where the database system is.
	 * By the time we get here, we have found the database system
	 * (that's how we're making this method call), so the URL is not
	 * necessary to establish the connection here. The driver should
	 * remember the URL that was used to establish the connection,
	 * so it can implement the DatabaseMetaData.getURL() method.
	 *
	 * @param user	The UserID of the user getting the connection
	 * @param unicode	True means unicode codeset, false means ASCII codeset
	 * @param drdaID	The drda id of the connection (from network server)
	 * @param dbname	The database name
	 *
	 * @return	A new LanguageConnectionContext
	 *
	 * @exception StandardException thrown if unable to create the connection.
	 */
	public LanguageConnectionContext setupConnection(ContextManager cm, String user, String drdaID, String dbname) throws StandardException;

	/**
	  Push a DbContext onto the provided context stack. This conext will
	  shut down the database in case of a DatabaseException being
	  cleaned up.
	 */
	public void pushDbContext(ContextManager cm);

	/**
		Is the database active (open).
	*/
	public boolean isActive();

	/**
	  */
	public	int	getEngineType();

	/**
	 * This method returns the authentication service handle for the
	 * database.
	 *
	 * NOTE: There is always a Authentication Service per database
	 * and at the system level.
	 *
	 * @return	The authentication service handle for the database
	 */
	public AuthenticationService getAuthenticationService();

	/**
	 * Get a Resource Adapter - only used by XA system.  There is one and only
	 * one resource adapter per cloudscape database.
	 *
	 * @return the resource Adapter for the database, null if no resource
	 * adapter is available for this database. Returned as an Object
	 * so that non-XA aggressive JVMs such as Chai don't get ClassNotFound.
	 * caller must cast result to ResourceAdapter.
	 *
	 */
	public Object getResourceAdapter();

	/** Set the Locale that is returned by this LocaleFinder */
	public	void	setLocale(Locale locale);
}
