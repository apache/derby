/*

   Derby - Class org.apache.derby.impl.sql.execute.JarUtil

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.impl.sql.execute.JarDDL;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.FileInfoDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.io.StorageFile;

import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

public class JarUtil
{
	public static final String ADD_JAR_DDL = "ADD JAR";
	public static final String DROP_JAR_DDL = "DROP JAR";
	public static final String REPLACE_JAR_DDL = "REPLACE JAR";
	public static final String READ_JAR = "READ JAR";
	//
	//State passed in by the caller
	private UUID id; //For add null means create a new id.
	private String schemaName;
	private String sqlName;

	//Derived state
	private LanguageConnectionContext lcc;
	private FileResource fr;
	private DataDictionary dd;
	private DataDescriptorGenerator ddg;
	
	//
	//State derived from the caller's context
	public JarUtil(UUID id, String schemaName, String sqlName)
		 throws StandardException
	{
		this.id = id;
		this.schemaName = schemaName;
		this.sqlName = sqlName;

        lcc = (LanguageConnectionContext)
			ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
		fr = lcc.getTransactionExecute().getFileHandler();
		dd = lcc.getDataDictionary();
		ddg = dd.getDataDescriptorGenerator();
	}

	/**
	  Add a jar file to the current connection's database.

	  @param id The id for the jar file we add. If null this makes up a new id.
	  @param schemaName the name for the schema that holds the jar file.
	  @param sqlName the sql name for the jar file.
	  @param externalPath the path for the jar file to add.
	  @return The generationId for the jar file we add.

	  @exception StandardException Opps
	  */
	static public long
	add(UUID id, String schemaName, String sqlName, String externalPath)
		 throws StandardException
	{
		JarUtil jutil = new JarUtil(id, schemaName, sqlName);
		InputStream is = null;
		
		try {
			is = FileUtil.getInputStream(externalPath, 0);
			return jutil.add(is);
		} catch (java.io.IOException fnfe) {
			throw StandardException.newException(SQLState.SQLJ_INVALID_JAR, fnfe, externalPath);
		}
		finally {
			try {if (is != null) is.close();}
			catch (IOException ioe) {}
		}
	}

	/**
	  Add a jar file to the current connection's database.

	  <P> The reason for adding the jar file in this private instance
	  method is that it allows us to share set up logic with drop and
	  replace.
	  @param is A stream for reading the content of the file to add.
	  @exception StandardException Opps
	  */
	public long add(InputStream is) throws StandardException
	{
		//
		//Like create table we say we are writing before we read the dd
		dd.startWriting(lcc);
		FileInfoDescriptor fid = getInfo();
		if (fid != null)
			throw
				StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT, 
											   fid.getDescriptorType(), sqlName, fid.getSchemaDescriptor().getDescriptorType(), schemaName);

		try {
			notifyLoader(false);
			dd.invalidateAllSPSPlans();
			long generationId = fr.add(JarDDL.mkExternalName(schemaName, sqlName, fr.getSeparatorChar()),is);

			SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, null, true);

			fid = ddg.newFileInfoDescriptor(id, sd,
							sqlName, generationId);
			dd.addDescriptor(fid, sd, DataDictionary.SYSFILES_CATALOG_NUM,
							 false, lcc.getTransactionExecute());
			return generationId;
		} finally {
			notifyLoader(true);
		}
	}

	/**
	  Drop a jar file from the current connection's database.

	  @param id The id for the jar file we drop. Ignored if null.
	  @param schemaName the name for the schema that holds the jar file.
	  @param sqlName the sql name for the jar file.
	  @param purgeOnCommit True means purge the old jar file on commit. False
	    means leave it around for use by replication.

	  @exception StandardException Opps
	  */
	static public void
	drop(UUID id, String schemaName, String sqlName,boolean purgeOnCommit)
		 throws StandardException
	{
		JarUtil jutil = new JarUtil(id, schemaName,sqlName);
		jutil.drop(purgeOnCommit);
	}

	/**
	  Drop a jar file from the current connection's database.

	  <P> The reason for dropping  the jar file in this private instance
	  method is that it allows us to share set up logic with add and
	  replace.
	  @param purgeOnCommit True means purge the old jar file on commit. False
	    means leave it around for use by replication.

	  @exception StandardException Opps
	  */
	public void drop(boolean purgeOnCommit) throws StandardException
	{
		//
		//Like create table we say we are writing before we read the dd
		dd.startWriting(lcc);
		FileInfoDescriptor fid = getInfo();
		if (fid == null)
			throw StandardException.newException(SQLState.LANG_FILE_DOES_NOT_EXIST, sqlName,schemaName);

		if (SanityManager.DEBUG)
		{
			if (id != null && !fid.getUUID().equals(id))
			{
				SanityManager.THROWASSERT("Drop id mismatch want="+id+
						" have "+fid.getUUID());
			}
		}

		String dbcp_s = PropertyUtil.getServiceProperty(lcc.getTransactionExecute(),Property.DATABASE_CLASSPATH);
		if (dbcp_s != null)
		{
			String[][]dbcp= IdUtil.parseDbClassPath(dbcp_s,
                                                            lcc.getIdentifierCasing() != lcc.ANTI_ANSI_CASING );
			boolean found = false;
			//
			//Look for the jar we are dropping on our database classpath.
			//We don't concern ourselves with 3 part names since they may
			//refer to a jar file in another database and may not occur in
			//a database classpath that is stored in the propert congomerate.
			for (int ix=0;ix<dbcp.length;ix++)
				if (dbcp.length == 2 &&
					dbcp[ix][0].equals(schemaName) && dbcp[ix][1].equals(sqlName))
					found = true;
			if (found)
				throw StandardException.newException(SQLState.LANG_CANT_DROP_JAR_ON_DB_CLASS_PATH_DURING_EXECUTION, 
									IdUtil.mkQualifiedName(schemaName,sqlName),
									dbcp_s);
		}

		try {
		
			notifyLoader(false);
			dd.invalidateAllSPSPlans();
			DependencyManager dm = dd.getDependencyManager();
			dm.invalidateFor(fid, DependencyManager.DROP_JAR, lcc);

			dd.dropFileInfoDescriptor(fid);

			fr.remove(JarDDL.mkExternalName(schemaName, sqlName, fr.getSeparatorChar()),
				fid.getGenerationId(), true /*purgeOnCommit*/);
		} finally {
			notifyLoader(true);
		}
	}

	/**
	  Replace a jar file from the current connection's database with the content of an
	  external file. 


	  @param id The id for the jar file we add. Ignored if null.
	  @param schemaName the name for the schema that holds the jar file.
	  @param sqlName the sql name for the jar file.
	  @param externalPath the path for the jar file to add.
	  @param purgeOnCommit True means purge the old jar file on commit. False
	    means leave it around for use by replication.
	  @return The new generationId for the jar file we replace.

	  @exception StandardException Opps
	  */
	static public long
	replace(UUID id,String schemaName, String sqlName,
			String externalPath,boolean purgeOnCommit)
		 throws StandardException
	{
		JarUtil jutil = new JarUtil(id,schemaName,sqlName);
		InputStream is = null;
		

		try {
			is = FileUtil.getInputStream(externalPath, 0);

			return jutil.replace(is,purgeOnCommit);
		} catch (java.io.IOException fnfe) {
			throw StandardException.newException(SQLState.SQLJ_INVALID_JAR, fnfe, externalPath);
		}
		finally {
			try {if (is != null) is.close();}
			catch (IOException ioe) {}
		}
	}

	/**
	  Replace a jar file in the current connection's database with the
	  content of an external file.

	  <P> The reason for adding the jar file in this private instance
	  method is that it allows us to share set up logic with add and
	  drop.
	  @param is An input stream for reading the new content of the jar file.
	  @param purgeOnCommit True means purge the old jar file on commit. False
	    means leave it around for use by replication.
	  @exception StandardException Opps
	  */
	public long replace(InputStream is,boolean purgeOnCommit) throws StandardException
	{
		//
		//Like create table we say we are writing before we read the dd
		dd.startWriting(lcc);

		//
		//Temporarily drop the FileInfoDescriptor from the data dictionary.
		FileInfoDescriptor fid = getInfo();
		if (fid == null)
			throw StandardException.newException(SQLState.LANG_FILE_DOES_NOT_EXIST, sqlName,schemaName);

		if (SanityManager.DEBUG)
		{
			if (id != null && !fid.getUUID().equals(id))
			{
				SanityManager.THROWASSERT("Replace id mismatch want="+
					id+" have "+fid.getUUID());
			}
		}

		try {
			// disable loads from this jar
			notifyLoader(false);
			dd.invalidateAllSPSPlans();
			dd.dropFileInfoDescriptor(fid);

			//
			//Replace the file.
			long generationId = 
				fr.replace(JarDDL.mkExternalName(schemaName, sqlName, fr.getSeparatorChar()),
					fid.getGenerationId(), is, purgeOnCommit);

			//
			//Re-add the descriptor to the data dictionary.
			FileInfoDescriptor fid2 = 
				ddg.newFileInfoDescriptor(fid.getUUID(),fid.getSchemaDescriptor(),
								sqlName,generationId);
			dd.addDescriptor(fid2, fid.getSchemaDescriptor(),
							 DataDictionary.SYSFILES_CATALOG_NUM, false, lcc.getTransactionExecute());
			return generationId;

		} finally {

			// reenable class loading from this jar
			notifyLoader(true);
		}
	}

	/**
	  Get the FileInfoDescriptor for a jar file from the current connection's database or
	  null if it does not exist.

	  @param schemaName the name for the schema that holds the jar file.
	  @param sqlName the sql name for the jar file.
	  @return The FileInfoDescriptor.
	  @exception StandardException Opps
	  */
	public static FileInfoDescriptor getInfo(String schemaName, String sqlName, String statementType)
		 throws StandardException
	{
		JarUtil jUtil = new JarUtil(null,schemaName,sqlName);
		return jUtil.getInfo();
	}

	/**
	  Get the FileInfoDescriptor for the Jar file or null if it does not exist.
	  @exception StandardException Ooops
	  */
	private FileInfoDescriptor getInfo()
		 throws StandardException
	{
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, null, true);
		return dd.getFileInfoDescriptor(sd,sqlName);
	}

	// get the current version of the jar file as a File or InputStream
	public static Object getAsObject(String schemaName, String sqlName)
		 throws StandardException
	{
		JarUtil jUtil = new JarUtil(null,schemaName,sqlName);

		FileInfoDescriptor fid = jUtil.getInfo();
		if (fid == null)
			throw StandardException.newException(SQLState.LANG_FILE_DOES_NOT_EXIST, sqlName,schemaName);

		long generationId = fid.getGenerationId();

		StorageFile f = jUtil.getAsFile(generationId);
		if (f != null)
			return f;

		return jUtil.getAsStream(generationId);
	}

	private StorageFile getAsFile(long generationId) {
		return fr.getAsFile(JarDDL.mkExternalName(schemaName, sqlName, fr.getSeparatorChar()), generationId);
	}

	public static InputStream getAsStream(String schemaName, String sqlName,
		long generationId) throws StandardException {
		JarUtil jUtil = new JarUtil(null,schemaName,sqlName);

		return jUtil.getAsStream(generationId);		
	}

	private InputStream getAsStream(long generationId) throws StandardException {
		try {
			return fr.getAsStream(JarDDL.mkExternalName(schemaName, sqlName, fr.getSeparatorChar()), generationId);
		} catch (IOException ioe) {
			throw StandardException.newException(SQLState.LANG_FILE_ERROR, ioe.toString(),ioe);	
		}
	}

	private void notifyLoader(boolean reload) throws StandardException {
		ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
		cf.notifyModifyJar(reload);
	}
}
