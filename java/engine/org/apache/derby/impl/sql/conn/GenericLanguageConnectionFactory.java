/*

   Derby - Class org.apache.derby.impl.sql.conn.GenericLanguageConnectionFactory

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

package org.apache.derby.impl.sql.conn;

import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.impl.sql.GenericStatement;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.services.compiler.JavaFactory;
import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.db.Database;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.compile.TypeCompilerFactory;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.sql.compile.Parser;

import org.apache.derby.iapi.services.property.PropertyFactory;

import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.sql.compile.OptimizerFactory;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.cache.CacheFactory;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.cache.CacheableFactory;
import org.apache.derby.iapi.services.cache.Cacheable;

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.property.PropertySetCallback;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.reference.EngineType;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.Dictionary;
import java.io.Serializable;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.util.StringUtil;

/**
 * LanguageConnectionFactory generates all of the items
 * a language system needs that is specific to a particular
 * connection. Alot of these are other factories.
 *
 */
public class GenericLanguageConnectionFactory
	implements LanguageConnectionFactory, CacheableFactory, PropertySetCallback, ModuleControl, ModuleSupportable {

	/*
		fields
	 */
	private 	ExecutionFactory		ef;
	private 	OptimizerFactory		of;
	private		TypeCompilerFactory		tcf;
	private 	DataValueFactory		dvf;
	private 	UUIDFactory				uuidFactory;
	private 	JavaFactory				javaFactory;
	private 	ClassFactory			classFactory;
	private 	PropertyFactory			pf;

	private		int						nextLCCInstanceNumber;

	/*
	  for caching prepared statements 
	*/
	private int cacheSize = org.apache.derby.shared.common.reference.Property.STATEMENT_CACHE_SIZE_DEFAULT;
	private CacheManager singleStatementCache;

	/*
	   constructor
	*/
	public GenericLanguageConnectionFactory() {
	}

	/*
	   LanguageConnectionFactory interface
	*/

	/*
		these are the methods that do real work, not just look for factories
	 */

	/**
		Get a Statement for the connection
		@param compilationSchema schema
		@param statementText the text for the statement
		@param forReadOnly if concurrency is CONCUR_READ_ONLY
		@return	The Statement
	 */
        public Statement getStatement(SchemaDescriptor compilationSchema, String statementText, boolean forReadOnly)
        {
	    return new GenericStatement(compilationSchema, statementText, forReadOnly);
	}


	/**
		Get a LanguageConnectionContext. this holds things
		we want to remember about activity in the language system,
		where this factory holds things that are pretty stable,
		like other factories.
		<p>
		The returned LanguageConnectionContext is intended for use
		only by the connection that requested it.

		@return a language connection context for the context stack.
		@exception StandardException the usual -- for the subclass
	 */

	public LanguageConnectionContext newLanguageConnectionContext(
		ContextManager cm,
		TransactionController tc,
		LanguageFactory lf,
		Database db,
		String userName,
		String drdaID,
		String dbname) throws StandardException {
		
		return new GenericLanguageConnectionContext(cm,
													tc,
													lf,
													this,
													db,
													userName,
													getNextLCCInstanceNumber(),
													drdaID,
													dbname);
	}

	public Cacheable newCacheable(CacheManager cm) {
		return new CachedStatement();
	}

	/*
		these methods all look for factories that we booted.
	 */
	 
	 /**
		Get the UUIDFactory to use with this language connection
		REMIND: this is only used by the compiler; should there be
		a compiler module control class to boot compiler-only stuff?
	 */
	public UUIDFactory	getUUIDFactory()
	{
		return uuidFactory;
	}

	/**
		Get the ClassFactory to use with this language connection
	 */
	public ClassFactory	getClassFactory()
	{
		return classFactory;
	}

	/**
		Get the JavaFactory to use with this language connection
		REMIND: this is only used by the compiler; should there be
		a compiler module control class to boot compiler-only stuff?
	 */
	public JavaFactory	getJavaFactory()
	{
		return javaFactory;
	}

	/**
		Get the ExecutionFactory to use with this language connection
	 */
	public ExecutionFactory	getExecutionFactory() {
		return ef;
	}

	/**
		Get the PropertyFactory to use with this language connection
	 */
	public PropertyFactory	getPropertyFactory() 
	{
		return pf;
	}	

	/**
		Get the OptimizerFactory to use with this language connection
	 */
	public OptimizerFactory	getOptimizerFactory() {
		return of;
	}
	/**
		Get the TypeCompilerFactory to use with this language connection
	 */
	public TypeCompilerFactory getTypeCompilerFactory() {
		return tcf;
	}

	/**
		Get the DataValueFactory to use with this language connection
	 */
	public DataValueFactory		getDataValueFactory() {
		return dvf;
	}

	/*
		ModuleControl interface
	 */

	/**
		this implementation will not support caching of statements.
	 */
	public boolean canSupport(Properties startParams) {

		return Monitor.isDesiredType( startParams,
                EngineType.STANDALONE_DB | EngineType.STORELESS_ENGINE);
	}

	private	int	statementCacheSize(Properties startParams)
	{
		String wantCacheProperty = null;

		wantCacheProperty =
			PropertyUtil.getPropertyFromSet(startParams, org.apache.derby.shared.common.reference.Property.STATEMENT_CACHE_SIZE);

		if (SanityManager.DEBUG)
			SanityManager.DEBUG("StatementCacheInfo", "Cacheing implementation chosen if null or 0<"+wantCacheProperty);

		if (wantCacheProperty != null) {
			try {
			    cacheSize = Integer.parseInt(wantCacheProperty);
			} catch (NumberFormatException nfe) {
				cacheSize = org.apache.derby.shared.common.reference.Property.STATEMENT_CACHE_SIZE_DEFAULT; 
			}
		}

		return cacheSize;
	}
	
	/**
	 * Start-up method for this instance of the language connection factory.
	 * Note these are expected to be booted relative to a Database.
	 *
	 * @param startParams	The start-up parameters (ignored in this case)
	 *
	 * @exception StandardException	Thrown on failure to boot
	 */
	public void boot(boolean create, Properties startParams) 
		throws StandardException {

		//The following call to Monitor to get DVF is going to get the already
		//booted DVF (DVF got booted by BasicDatabase's boot method. 
		//BasicDatabase also set the correct Locale in the DVF. There after,
		//DVF with correct Locale is available to rest of the Derby code.
		dvf = (DataValueFactory) bootServiceModule(create, this, org.apache.derby.shared.common.reference.ClassName.DataValueFactory, startParams);
		javaFactory = (JavaFactory) startSystemModule(org.apache.derby.shared.common.reference.Module.JavaFactory);
		uuidFactory = getMonitor().getUUIDFactory();
		classFactory = (ClassFactory) getServiceModule(this, org.apache.derby.shared.common.reference.Module.ClassFactory);
		if (classFactory == null)
 			classFactory = (ClassFactory) findSystemModule(org.apache.derby.shared.common.reference.Module.ClassFactory);

		//set the property validation module needed to do propertySetCallBack
		//register and property validation
		setValidation();

		ef = (ExecutionFactory) bootServiceModule(create, this, ExecutionFactory.MODULE, startParams);
		of = (OptimizerFactory) bootServiceModule(create, this, OptimizerFactory.MODULE, startParams);
		tcf =
		   (TypeCompilerFactory) startSystemModule(TypeCompilerFactory.MODULE);

		// If the system supports statement caching boot the CacheFactory module.
		int cacheSize = statementCacheSize(startParams);
		if (cacheSize > 0) {
			CacheFactory cacheFactory = (CacheFactory) startSystemModule(org.apache.derby.shared.common.reference.Module.CacheFactory);
			singleStatementCache = cacheFactory.newCacheManager(this,
												"StatementCache",
												cacheSize/4,
												cacheSize);

            // Start a management bean for the statement cache to allow
            // monitoring through JMX, if it is available and enabled.
            DataFactory df = (DataFactory)
                    findServiceModule(this, DataFactory.MODULE);
            singleStatementCache.registerMBean(df.getRootDirectory());
		}

	}

	/**
	 * returns the statement cache that this connection should use; currently
     * there is a statement cache per database.
	 */
	public CacheManager getStatementCache()
	{
		return singleStatementCache;
	}

	/**
     * Stop this module.
	 */
	public void stop() {
        if (singleStatementCache != null) {
            singleStatementCache.deregisterMBean();
        }
	}

	/*
	** Methods of PropertySetCallback
	*/

	public void init(boolean dbOnly, Dictionary p) {
		// not called yet ...
	}

	/**
	  @see PropertySetCallback#validate
	  @exception StandardException Thrown on error.
	*/
	public boolean validate(String key,
						 Serializable value,
						 Dictionary p)
		throws StandardException {
		if (value == null)
			return true;
		else if (key.equals(Property.DEFAULT_CONNECTION_MODE_PROPERTY))
		{
			String value_s = (String)value;
			if (value_s != null &&
				!StringUtil.SQLEqualsIgnoreCase(value_s, Property.NO_ACCESS) &&
				!StringUtil.SQLEqualsIgnoreCase(value_s, Property.READ_ONLY_ACCESS) &&
				!StringUtil.SQLEqualsIgnoreCase(value_s, Property.FULL_ACCESS))
				throw StandardException.newException(SQLState.AUTH_INVALID_AUTHORIZATION_PROPERTY, key, value_s);

			return true;
		}
		else if (key.equals(Property.READ_ONLY_ACCESS_USERS_PROPERTY) ||
				 key.equals(Property.FULL_ACCESS_USERS_PROPERTY))
		{
			String value_s = (String)value;

			/** Parse the new userIdList to verify its syntax. */
			String[] newList_a;
			try {newList_a = IdUtil.parseIdList(value_s);}
			catch (StandardException se) {
                throw StandardException.newException(SQLState.AUTH_INVALID_AUTHORIZATION_PROPERTY, se, key,value_s);
			}

			/** Check the new list userIdList for duplicates. */
			String dups = IdUtil.dups(newList_a);
			if (dups != null) throw StandardException.newException(SQLState.AUTH_DUPLICATE_USERS, key,dups);

			/** Check for users with both read and full access permission. */
			String[] otherList_a;
			String otherList;
			if (key.equals(Property.READ_ONLY_ACCESS_USERS_PROPERTY))
				otherList = (String)p.get(Property.FULL_ACCESS_USERS_PROPERTY);
			else
				otherList = (String)p.get(Property.READ_ONLY_ACCESS_USERS_PROPERTY);
			otherList_a = IdUtil.parseIdList(otherList);
			String both = IdUtil.intersect(newList_a,otherList_a);
			if (both != null) throw StandardException.newException(SQLState.AUTH_USER_IN_READ_AND_WRITE_LISTS, both);
			
			return true;
		}

		return false;
	}
	/** @see PropertySetCallback#apply */
	public Serviceable apply(String key,
							 Serializable value,
							 Dictionary p)
	{
			 return null;
	}
	/** @see PropertySetCallback#map */
	public Serializable map(String key, Serializable value, Dictionary p)
	{
		return null;
	}

	protected void setValidation() throws StandardException {
		pf = (PropertyFactory) findServiceModule(this,
			org.apache.derby.shared.common.reference.Module.PropertyFactory);
		pf.addPropertySetNotification(this);
	}

    public Parser newParser(CompilerContext cc)
    {
        return new org.apache.derby.impl.sql.compile.ParserImpl(cc);
    }

	// Class methods

	/**
	 * Get the instance # for the next LCC.
	 * (Useful for logStatementText=true output.
	 *
	 * @return instance # of next LCC.
	 */
	protected synchronized int getNextLCCInstanceNumber()
	{
		return nextLCCInstanceNumber++;
	}
    
    /**
     * Privileged Monitor lookup. Must be package private so that user code
     * can't call this entry point.
     */
    static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

    
    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object  startSystemModule( final String factoryInterface )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.startSystemModule( factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    
    /**
     * Privileged lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object  findSystemModule( final String factoryInterface )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.findSystemModule( factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    
    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object bootServiceModule
        (
         final boolean create, final Object serviceModule,
         final String factoryInterface, final Properties properties
         )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.bootServiceModule( create, serviceModule, factoryInterface, properties );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object findServiceModule( final Object serviceModule, final String factoryInterface)
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.findServiceModule( serviceModule, factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    /**
     * Privileged module lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object getServiceModule( final Object serviceModule, final String factoryInterface )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.getServiceModule( serviceModule, factoryInterface );
                 }
             }
             );
    }

}
