/*

   Derby - Class org.apache.derby.impl.sql.execute.JarUtil

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.security.Securable;
import org.apache.derby.iapi.security.SecurityUtil;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.FileInfoDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.io.StorageFile;

public class JarUtil
{
	//
	//State passed in by the caller
    private LanguageConnectionContext lcc;
	private String schemaName;
	private String sqlName;

	//Derived state
	
	private FileResource fr;
	private DataDictionary dd;
	private DataDescriptorGenerator ddg;
	
	//
	//State derived from the caller's context
	private JarUtil(LanguageConnectionContext lcc,
            String schemaName, String sqlName)
		 throws StandardException
	{
		this.schemaName = schemaName;
		this.sqlName = sqlName;

        this.lcc = lcc;
		fr = lcc.getTransactionExecute().getFileHandler();
		dd = lcc.getDataDictionary();
		ddg = dd.getDataDescriptorGenerator();
	}

	/**
	  install a jar file to the current connection's database.

	  @param schemaName the name for the schema that holds the jar file.
	  @param sqlName the sql name for the jar file.
	  @param externalPath the path for the jar file to add.
	  @return The generationId for the jar file we add.

	  @exception StandardException Opps
	  */
	public static long
	install(LanguageConnectionContext lcc,
            String schemaName, String sqlName, String externalPath)
		 throws StandardException
	{
        // make sure that application code doesn't bypass security checks
        // by calling this public entry point
        SecurityUtil.authorize( Securable.INSTALL_JAR );
            
		JarUtil jutil = new JarUtil(lcc, schemaName, sqlName);
		InputStream is = null;
		
		try {
			is = openJarURL(externalPath);
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
	private long add(final InputStream is) throws StandardException
	{
		//
		//Like create table we say we are writing before we read the dd
		dd.startWriting(lcc);
		FileInfoDescriptor fid = getInfo();
		if (fid != null)
			throw
				StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT, 
											   fid.getDescriptorType(), sqlName, fid.getSchemaDescriptor().getDescriptorType(), schemaName);

        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, null, true);
        try {
            notifyLoader(false);
            dd.invalidateAllSPSPlans();
            UUID id = BaseActivation.getMonitor().getUUIDFactory().createUUID();
            final String jarExternalName = JarUtil.mkExternalName(
                id, schemaName, sqlName, fr.getSeparatorChar());

            long generationId = setJar(jarExternalName, is, true, 0L);

            fid = ddg.newFileInfoDescriptor(id, sd, sqlName, generationId);
            dd.addDescriptor(fid, sd, DataDictionary.SYSFILES_CATALOG_NUM,
                    false, lcc.getTransactionExecute());
            return generationId;
        } finally {
            notifyLoader(true);
        }
	}

	/**
     * Drop a jar file from the current connection's database.
     * 
     * @param schemaName
     *            the name for the schema that holds the jar file.
     * @param sqlName
     *            the sql name for the jar file.
     * 
     * @exception StandardException
     *                Opps
     */
	public static void
	drop(LanguageConnectionContext lcc, String schemaName, String sqlName)
		 throws StandardException
	{
        // make sure that application code doesn't bypass security checks
        // by calling this public entry point
        SecurityUtil.authorize( Securable.REMOVE_JAR );
            
		JarUtil jutil = new JarUtil(lcc, schemaName,sqlName);
		jutil.drop();
	}

	/**
	  Drop a jar file from the current connection's database.

	  <P> The reason for dropping  the jar file in this private instance
	  method is that it allows us to share set up logic with add and
	  replace.

	  @exception StandardException Opps
	  */
	private void drop() throws StandardException
	{
		//
		//Like create table we say we are writing before we read the dd
		dd.startWriting(lcc);
		FileInfoDescriptor fid = getInfo();
		if (fid == null)
			throw StandardException.newException(SQLState.LANG_FILE_DOES_NOT_EXIST, sqlName,schemaName);

		String dbcp_s = PropertyUtil.getServiceProperty(lcc.getTransactionExecute(),Property.DATABASE_CLASSPATH);
		if (dbcp_s != null)
		{
			String[][]dbcp= IdUtil.parseDbClassPath(dbcp_s);
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

            UUID id = fid.getUUID();
			dd.dropFileInfoDescriptor(fid);
            fr.remove(
                JarUtil.mkExternalName(
                    id, schemaName, sqlName, fr.getSeparatorChar()),
				fid.getGenerationId());
		} finally {
			notifyLoader(true);
		}
	}

	/**
	  Replace a jar file from the current connection's database with the content of an
	  external file. 


	  @param schemaName the name for the schema that holds the jar file.
	  @param sqlName the sql name for the jar file.
	  @param externalPath the path for the jar file to add.
	  @return The new generationId for the jar file we replace.

	  @exception StandardException Opps
	  */
	public static long
	replace(LanguageConnectionContext lcc, String schemaName, String sqlName,
			String externalPath)
		 throws StandardException
	{
        // make sure that application code doesn't bypass security checks
        // by calling this public entry point
        SecurityUtil.authorize( Securable.REPLACE_JAR );
            
		JarUtil jutil = new JarUtil(lcc, schemaName,sqlName);
		InputStream is = null;
		

		try {
			is = openJarURL(externalPath);

			return jutil.replace(is);
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
	  @exception StandardException Opps
	  */
	private long replace(InputStream is) throws StandardException
	{
		//
		//Like create table we say we are writing before we read the dd
		dd.startWriting(lcc);

		//
		//Temporarily drop the FileInfoDescriptor from the data dictionary.
		FileInfoDescriptor fid = getInfo();
		if (fid == null)
			throw StandardException.newException(SQLState.LANG_FILE_DOES_NOT_EXIST, sqlName,schemaName);

		try {
			// disable loads from this jar
			notifyLoader(false);
			dd.invalidateAllSPSPlans();
			dd.dropFileInfoDescriptor(fid);
            final String jarExternalName =
                JarUtil.mkExternalName(
                    fid.getUUID(), schemaName, sqlName, fr.getSeparatorChar());

			//
			//Replace the file.
			long generationId = setJar(jarExternalName, is, false,
					fid.getGenerationId());
            
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
	  Get the FileInfoDescriptor for the Jar file or null if it does not exist.
	  @exception StandardException Ooops
	  */
	private FileInfoDescriptor getInfo()
		 throws StandardException
	{
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, null, true);
		return dd.getFileInfoDescriptor(sd,sqlName);
	}

	private void notifyLoader(boolean reload) throws StandardException {
		ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
		cf.notifyModifyJar(reload);
	}

    /**
     * Open an input stream to read a URL or a file.
     * URL is attempted first, if the string does not conform
     * to a URL then an attempt to open it as a regular file
     * is tried.
     * <BR>
     * Attempting the file first can throw a security execption
     * when a valid URL is passed in.
     * The security exception is due to not have the correct permissions
     * to access the bogus file path. To avoid this the order was reversed
     * to attempt the URL first and only attempt a file open if creating
     * the URL throws a MalformedURLException.
     */
    private static InputStream openJarURL(final String externalPath)
        throws IOException
    {
        try {
            return AccessController.doPrivileged
            (new java.security.PrivilegedExceptionAction<InputStream>(){
                
                public InputStream run() throws IOException {    
                    try {
                        return new URL(externalPath).openStream();
                    } catch (MalformedURLException mfurle)
                    {
                        return new FileInputStream(externalPath);
                    }
                }
            });
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getException();
        }
    }
    
    /**
     * Copy the jar from the externally obtained 
     * input stream into the database
     * @param jarExternalName Name of jar with database structure.
     * @param contents Contents of jar file.
     * @param add true to add, false to replace
     * @param currentGenerationId generation id of existing version, ignored when adding.
     */
    private long setJar(final String jarExternalName,
            final InputStream contents,
            final boolean add,
            final long currentGenerationId)
            throws StandardException {
        try {
            return (AccessController
                    .doPrivileged(new java.security.PrivilegedExceptionAction<Long>() {

                        public Long run() throws StandardException {
                            long generationId;
                            
                            if (add)
                                generationId = fr.add(jarExternalName, contents);
                            else
                                generationId =  fr.replace(jarExternalName,
                                        currentGenerationId, contents);
                            return generationId;
                        }
                    })).longValue();
        } catch (PrivilegedActionException e) {
            throw (StandardException) e.getException();
        }
    }
    
    /**
      Make an external name for a jar file stored in the database.
      */
    public static String mkExternalName(
            UUID id, 
            String schemaName, 
            String sqlName, 
            char separatorChar) throws StandardException
    {
        return mkExternalNameInternal(
            id, schemaName, sqlName, separatorChar, false, false);
    }

    private static String mkExternalNameInternal(
            UUID id,
            String schemaName,
            String sqlName,
            char separatorChar,
            boolean upgrading,
            boolean newStyle) throws StandardException
    {
        StringBuffer sb = new StringBuffer(30);
        sb.append(FileResource.JAR_DIRECTORY_NAME);
        sb.append(separatorChar);

        boolean uuidSupported = false;

        if (!upgrading) {
            LanguageConnectionContext lcc =
                (LanguageConnectionContext)getContextOrNull(
                    LanguageConnectionContext.CONTEXT_ID);

            // DERBY-5357 UUIDs introduced in jar file names in 10.9
            uuidSupported =
                lcc.getDataDictionary().
                checkVersion(DataDictionary.DD_VERSION_DERBY_10_9, null);
        }


        if (!upgrading && uuidSupported || upgrading && newStyle) {
            sb.append(id.toString());
            sb.append(".jar");
        } else {
            sb.append(schemaName);
            sb.append(separatorChar);
            sb.append(sqlName);
            sb.append(".jar");
        }

        return sb.toString();
    }

    /**
     * Upgrade code: upgrade one jar file to new style (&gt;= 10.9)
     *
     * @param tc transaction controller
     * @param fid the jar file to be upgraded
     * @throws StandardException
     */
    public static void upgradeJar(
            TransactionController tc,
            FileInfoDescriptor fid)
            throws StandardException {

        FileResource fh = tc.getFileHandler();

        StorageFile oldFile = fh.getAsFile(
            mkExternalNameInternal(
                fid.getUUID(),
                fid.getSchemaDescriptor().getSchemaName(),
                fid.getName(),
                File.separatorChar,
                true,
                false),
            fid.getGenerationId());

        StorageFile newFile = fh.getAsFile(
            mkExternalNameInternal(
                fid.getUUID(),
                fid.getSchemaDescriptor().getSchemaName(),
                fid.getName(),
                File.separatorChar,
                true,
                true),
            fid.getGenerationId());

        FileUtil.copyFile(
                new File(oldFile.getPath()),
                new File(newFile.getPath()), null);
    }
    
    /**
     * Privileged lookup of a Context. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Context    getContextOrNull( final String contextID )
    {
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getContextOrNull( contextID );
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<Context>()
                 {
                     public Context run()
                     {
                         return ContextService.getContextOrNull( contextID );
                     }
                 }
                 );
        }
    }

}
