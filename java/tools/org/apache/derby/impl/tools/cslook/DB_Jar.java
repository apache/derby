/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.cslook
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.cslook;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.HashMap;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.derby.tools.dblook;

public class DB_Jar {

	/* 
		IBM Copyright &copy notice.
	*/
	/**
		IBM Copyright &copy notice.
	*/

	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

	/* ************************************************
	 * Generate the DDL for all jars in a given
	 * database.
	 * @param dbName Name of the database (for locating the jar).
	 * @param conn Connection to the source database.
	 * @return The DDL for the jars has been written
	 *  to output via Logs.java.
	 ****/

	public static void doJars(String dbName, Connection conn)
		throws SQLException
	{

		String separator = System.getProperty("file.separator");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT FILENAME, SCHEMAID, " +
			"GENERATIONID FROM SYS.SYSFILES");

		boolean firstTime = true;
		while (rs.next()) {

			String jarName = dblook.addQuotes(
				dblook.expandDoubleQuotes(rs.getString(1)));
			String schemaId = rs.getString(2);
			String schemaName = dblook.lookupSchemaId(schemaId);
			if (dblook.isIgnorableSchema(schemaName))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("CSLOOK_Header", "jars");
				Logs.reportMessage("CSLOOK_Jar_Note");
				Logs.reportString("----------------------------------------------\n");
			}

			String genID = rs.getString(3);

			String schemaWithoutQuotes = dblook.stripQuotes(schemaName);
			StringBuffer jarFullName = new StringBuffer(separator);
			jarFullName.append(dblook.stripQuotes(jarName));
			jarFullName.append(".jar.G");
			jarFullName.append(genID);

			StringBuffer oldJarPath = new StringBuffer();
			oldJarPath.append(dbName);
			oldJarPath.append(separator);
			oldJarPath.append("jar");
			oldJarPath.append(separator);
			oldJarPath.append(schemaWithoutQuotes);
			oldJarPath.append(jarFullName);

			// Copy jar file to CSJARS directory.
			String absJarDir = null;
			try {

				// Create the CSJARS directory.
				File jarDir = new File(System.getProperty("user.dir") +
					separator + "CSJARS" + separator + schemaWithoutQuotes);
				absJarDir = jarDir.getAbsolutePath();
				jarDir.mkdirs();

				// Create streams.
				FileInputStream oldJarFile =
					new FileInputStream(oldJarPath.toString());
				FileOutputStream newJarFile =
					new FileOutputStream(absJarDir + jarFullName);

				// Copy.
				int st = 0;
				while (true) {
					if (oldJarFile.available() == 0)
						break;
					byte[] bAr = new byte[oldJarFile.available()];
					oldJarFile.read(bAr);
					newJarFile.write(bAr);
				}

				newJarFile.close();
				oldJarFile.close();

			} catch (Exception e) {
				Logs.debug("CSLOOK_FailedToLoadJar",
					absJarDir + jarFullName.toString());
				Logs.debug(e);
				firstTime = false;
				continue;
			}

			// Now, add the DDL to read the jar from CSJARS.
			StringBuffer loadJarString = new StringBuffer();
			loadJarString.append("CALL SQLJ.INSTALL_JAR('file:");
			loadJarString.append(absJarDir);
			loadJarString.append(jarFullName);
			loadJarString.append("', '");
			loadJarString.append(schemaName);
			loadJarString.append(".");
			loadJarString.append(jarName);
			loadJarString.append("', 0)");

			Logs.writeToNewDDL(loadJarString.toString());
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		stmt.close();
		rs.close();

	}

}
