/*

   Derby - Class org.apache.derby.impl.sql.catalog.SequenceUpdater

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
package org.apache.derby.impl.sql.catalog;

import java.util.HashMap;

import org.apache.derby.catalog.SequencePreallocator;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.BulkInsertCounter;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.RowLocation;

/**
 * <p>
 * An object cached in the data dictionary which manages new values
 * for sequences. Note that this class must be public and
 * have a 0-arg constructor in order to satisfy the Cacheable contract.
 * </p>
 *
 * <p>
 * This is the abstract superclass of specific implementations for specific
 * sequences. For instance, one subclass handles the ANSI/ISO sequences
 * stored in SYSSEQUENCES. Another subclass could handle the sequences
 * stored in Derby's identity columns.
 * </p>
 *
 * <p>
 * This class does a couple tricky things:
 * </p>
 *
 * <ul>
 * <li>It pre-allocates a range of values from a sequence so that we don't have to change
 *  the on-disk value every time we get the next value for a sequence.</li>
 * <li>When updating the on-disk value, we use a subtransaction of the user's
 * execution transaction. If the
 * special transaction cannot do its work immediately, without waiting for a lock, then
 * a TOO MUCH CONTENTION error is raised. It is believed that this can only happen
 * if someone holds locks on SYSSEQUENCES, either via sequence DDL or a scan
 * of the catalog. The TOO MUCH CONTENTION error tells
 * the user to not scan SYSSEQUENCES directly, but to instead use the
 * SYSCS_UTIL.SYSCS_PEEK_AT_SEQUENCE() if the user needs the current value of the
 * sequence generator.</li>
 * </ul>
 *
 * <p>
 * Here is the algorithm pursued when the caller asks for the next number in a sequence:
 * </p>
 *
 *
 * <ul>
 * <li>We try to get the next number from a cache of pre-allocated numbers. The endpoint
 * (last number in the pre-allocated range) was previously recorded in the catalog row which
 * describes this sequence. If we are successful in getting the next number, we
 * return it and all is well.</li>
 * <li>Otherwise, we must allocate a new range by updating the catalog row. We should not
 * be in contention with another connection because the update method is synchronized.</li>
 * </ul>
 *
 */
public abstract class SequenceUpdater implements Cacheable
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANT STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // DataDictionary where this generator is cached.
    protected DataDictionaryImpl _dd;

    //
    // The following state needs to be reset whenever this Cachable is re-used.
    //
    
    /** This is the key used to lookup this generator in the cache. */
    protected String _uuidString;

    /** This is the object which allocates ranges of sequence values */
    protected SequenceGenerator _sequenceGenerator;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** No-arg constructor to satisfy the Cacheable contract */
    public SequenceUpdater()
    {
    }

    /** Normal constructor */
    public SequenceUpdater( DataDictionaryImpl dd )
    {
        this();
        
        _dd = dd;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ABSTRACT OR OVERRIDABLE BEHAVIOR TO BE IMPLEMENTED BY CHILDREN
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Initialize the sequence generator. Work is done inside a read-only subtransaction of
     * the session's execution transaction.
     * </p>
     */
     abstract protected SequenceGenerator createSequenceGenerator( TransactionController readOnlyTC )
         throws StandardException;

    /**
     * <p>
     * Update the sequence value on disk. This method does its work in a subtransaction of
     * the user's execution transaction.
     * </p>
     *
     * @param tc The transaction to use
     * @param oldValue Expected value on disk for this sequence
     * @param newValue The value to poke into the system table backing this sequence
     * @param wait Whether to wait for a lock
     *
	 * 
	 * @return Returns true if the value was successfully updated, false if we lost a race with another session.
     *
     * @throws StandardException May throw an exception if a lock can't be obtained.
     */
    abstract protected boolean updateCurrentValueOnDisk( TransactionController tc, Long oldValue, Long newValue, boolean wait ) throws StandardException;
    
    /**
     * <p>
     * Create an exception to state that there is too much contention on the generator.
     * For backward compatibility reasons, different messages are needed by sequences
     * and identities. See DERBY-5426.
     * </p>
     */
    private   StandardException   tooMuchContentionException()
    {
        // If the sequence lives in the SYS schema, then it is used to back an identity column.
        if ( SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME.equals( _sequenceGenerator.getSchemaName() ) )
        {
            return StandardException.newException( SQLState.LOCK_TIMEOUT );
        }
        else
        {
            return StandardException.newException
                ( SQLState.LANG_TOO_MUCH_CONTENTION_ON_SEQUENCE, _sequenceGenerator.getName() );
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Cacheable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public void clean(boolean forRemove) throws StandardException
	{
        //
        // Flush current value to disk. This prevents us from leaking values when DDL
        // is performed. The metadata caches are invalidated and cleared when DDL is performed.
        // We flush the current value to disk on database shutdown also.
        // The call to updateCurrentValueOnDisk can fail if someone is holding a lock
        // on the SYS.SYSSEQUENCES row. This can happen if the user disregards our
        // advice and scans that catalog. This can also happen if the transaction which
        // creates the sequences is open for a long time and some later sequence creation
        // causes us to evict the old, uncommitted sequence generator from the cache.
        //
        boolean gapClosed = false;

        try {
            if ( _sequenceGenerator == null ) { gapClosed = true; }
            else
            {
                gapClosed = updateCurrentValueOnDisk( null, peekAtCurrentValue() );
            }
        }
        catch (StandardException se)
        {
            // The too much contention exception is redundant because the problem is logged
            // by the message below
            if ( !SQLState.LANG_TOO_MUCH_CONTENTION_ON_SEQUENCE.equals( se.getMessageId() ) )
            {
                throw se;
            }
        }
        finally
        {
            // log an error message if we failed to flush the preallocated values.
            if ( !gapClosed )
            {
                String  errorMessage = MessageService.getTextMessage
                    (
                     SQLState.LANG_CANT_FLUSH_PREALLOCATOR,
                     _sequenceGenerator.getSchemaName(),
                     _sequenceGenerator.getName()
                     );

                Monitor.getStream().println( errorMessage );
            }

            _uuidString = null;
            _sequenceGenerator = null;
        }
	}
    
	public boolean isDirty() { return false; }
    public Object getIdentity() { return _uuidString; }

	public void clearIdentity()
    {
        try
        {
            clean( false );
        } catch (StandardException se)
        {
            //Doing check for lcc and db to be certain
            LanguageConnectionContext lcc = getLCC();
            if (lcc != null)
            {
                Database db = lcc.getDatabase();
                boolean isactive = (db != null ? db.isActive() : false);
                lcc.getContextManager().cleanupOnError(se, isactive);
            }
        }
    }

	public Cacheable createIdentity( Object key, Object createParameter ) throws StandardException
	{
        Cacheable cacheable = this;

        //
        // The createParameter arg is unused.
        //
        return cacheable.setIdentity( key );
	}

	/**
	 * @see Cacheable#setIdentity
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Cacheable setIdentity(Object key) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (!(key instanceof String))
			{
				SanityManager.THROWASSERT( "Key for a SequenceUpdater is a " + key.getClass().getName() );
			}

            if ( (_uuidString != null) || (_sequenceGenerator != null) )
			{
				SanityManager.THROWASSERT( "Identity being changed on a live cacheable. Old uuidString = " + _uuidString );
			}
		}

		_uuidString = (String) key;

        if ( _sequenceGenerator == null )
        {
            TransactionController executionTC = getLCC().getTransactionExecute();
            
            //
            // We lookup information in a read-only subtransaction in order to minimize
            // contention. Since this is a read-only subtransaction, there should be
            // no conflict with the parent transaction.
            //
            TransactionController subTransaction = 
                executionTC.startNestedUserTransaction( true, true );

            try {
                _sequenceGenerator = createSequenceGenerator( subTransaction );
            }
            finally
            {
                // if we failed to get a generator, we have no identity. see DERBY-5389.
                if ( _sequenceGenerator == null ) { _uuidString = null; }
            
                subTransaction.commit();
                subTransaction.destroy();
            }
        }

		if ( _sequenceGenerator != null ) { return this; }
		else { return null; }
	}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Reset the sequence generator to a new start value. This is used by the special
     * bulk-insert optimization in InsertResultSet.
     * </p>
     */
    public synchronized void reset( Long newValue )
        throws StandardException
    {
        // first try to reset on disk
        updateCurrentValueOnDisk( null, newValue );

        // now reset the sequence generator
        _sequenceGenerator = _sequenceGenerator.clone( newValue );
    }
    
    /**
     * <p>
     * Get the SequenceUpdater used for the bulk-insert optimization in InsertResultSet.
     * </p>
     *
     * @param restart   True if the counter should be re-initialized to its start position.
     */
    public synchronized BulkInsertUpdater   getBulkInsertUpdater( boolean restart )
        throws StandardException
    {
        return new BulkInsertUpdater( this, restart );
    }
    
    /**
     * <p>
     * Get the next sequence number managed by this generator and advance the number. Could raise an
     * exception if the legal range is exhausted and wrap-around is not allowed.
     * Only one thread at a time is allowed through here. We do not want a race between the
     * two calls to the sequence generator: getCurrentValueAndAdvance() and allocateNewRange().
     * </p>
     *
     * @param returnValue This value is stuffed with the new sequence number.
     */
    public synchronized void getCurrentValueAndAdvance
        ( NumberDataValue returnValue ) throws StandardException
    {
        //
        // We may have to try to get a value from the Sequence Generator twice.
        // The first attempt may fail because we need to pre-allocate a new chunk
        // of values.
        //
        for ( int i = 0; i < 2; i++ )
        {
            //
            // We try to get a sequence number. The SequenceGenerator method is synchronized
            // so only one writer should be in there at a time. Lock contention is possible if
            // someone has selected from SYSSEQUENCES contrary to our advice. In that case,
            // we raise a TOO MUCH CONTENTION exception.
            //
            long[] cvaa = _sequenceGenerator.getCurrentValueAndAdvance();
            
            int status = (int) cvaa[ SequenceGenerator.CVAA_STATUS ];
            long currentValue = cvaa[ SequenceGenerator.CVAA_CURRENT_VALUE ];
            long lastAllocatedValue = cvaa[ SequenceGenerator.CVAA_LAST_ALLOCATED_VALUE ];
            long numberOfValuesAllocated = cvaa[ SequenceGenerator.CVAA_NUMBER_OF_VALUES_ALLOCATED ];
            
            switch ( status )
            {
            case SequenceGenerator.RET_OK:
                returnValue.setValue( currentValue );
                return;
                
            case SequenceGenerator.RET_MARK_EXHAUSTED:
                updateCurrentValueOnDisk( new Long( currentValue ), null );
                returnValue.setValue( currentValue );
                return;
                
            case SequenceGenerator.RET_ALLOCATE_NEW_VALUES:
                
                if ( updateCurrentValueOnDisk( new Long( currentValue ), new Long( lastAllocatedValue ) ) )
                {
                    _sequenceGenerator.allocateNewRange( currentValue, numberOfValuesAllocated );
                }
                break;
            
            default:
                throw unimplementedFeature();
            }
        }

        //
        // If we get here, then we failed to allocate a new sequence number range.
        //
        throw tooMuchContentionException();
    }

    /**
     * <p>
     * Get the current value of the sequence generator without advancing it.
     * May return null if the generator is exhausted.
     * </p>
     */
    public Long peekAtCurrentValue() throws StandardException
    {
        return _sequenceGenerator.peekAtCurrentValue();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // DISK WRITING MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Update the value on disk. Does its work in a subtransaction of the user's
     * execution transaction. If that fails, raises a TOO MUCH CONTENTION exception.
     * </p>
	 * 
	 * @return Returns true if the value was successfully updated, false if we lost a race with another session.
     *
     */
    public synchronized boolean updateCurrentValueOnDisk( Long oldValue, Long newValue ) throws StandardException
    {
        LanguageConnectionContext   lcc = getLCC();

        //
        // Not having an LCC should mean that we are in the middle of engine
        // shutdown. We get here only to flush the current value to disk so that
        // we don't leak unused sequence numbers. See DERBY-5398.
        //
        if ( lcc == null )
        {
            if (SanityManager.DEBUG)
            {
				SanityManager.ASSERT( oldValue == null, "We should be flushing unused sequence values here." );
			}
            
            ContextService csf = ContextService.getFactory();
            ContextManager cm = csf.getCurrentContextManager();
            AccessFactory af = _dd.af;
            TransactionController   dummyTransaction = af.getTransaction( cm );

            boolean retval = updateCurrentValueOnDisk( dummyTransaction, oldValue, newValue, false );
            dummyTransaction.commit();
            dummyTransaction.destroy();

            return retval;
		}

        TransactionController executionTransaction = lcc.getTransactionExecute();
        TransactionController nestedTransaction = 
            executionTransaction.startNestedUserTransaction( false, true );

        if ( nestedTransaction != null )
        {
            boolean retval = false;
            boolean escalateToParentTransaction = false;
            
            try
            {
                retval = updateCurrentValueOnDisk( nestedTransaction, oldValue, newValue, false );
            }
            catch (StandardException se)
            {
                if ( !se.isLockTimeout() )
                {
                    if ( se.isSelfDeadlock() )
                    {
                        // We're blocked by a lock held by our parent transaction.
                        // Escalate into the parent transaction now. See DERBY-6554.
                        escalateToParentTransaction = true;
                    }
                    else
                    {
                        Monitor.logThrowable( se );
                        throw se;
                    }
                }
            }
            finally
            {
                // DERBY-5494, if this commit does not flush log then an
                // unorderly shutdown could lose the update.  Do not use
                // commitNoSync(), and store needs to flush user nested update
                // transaction commits by default.
                nestedTransaction.commit();
                nestedTransaction.destroy();

                if ( escalateToParentTransaction )
                {
                    retval = updateCurrentValueOnDisk( executionTransaction, oldValue, newValue, false );
                }

                return retval;
            }
        }
        
        // If we get here, we failed to do the work in the nested transaction.
        // We might be self-deadlocking if the user has selected from SYSSEQUENCES
        // contrary to our advice.

        throw tooMuchContentionException();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // UTILITY MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Make a new range allocator (called when the generator is instantiated) */
    protected SequencePreallocator  makePreallocator( TransactionController tc )
        throws StandardException
    {
        String  propertyName = Property.LANG_SEQUENCE_PREALLOCATOR;
        String  className = PropertyUtil.getServiceProperty( tc, propertyName );

        if ( className == null ) { return new SequenceRange(); }

        try {
            // If the property value was a number rather than a class name, then
            // use that as the default size for preallocated ranges.
            if ( isNumber( className ) )
            {
                return new SequenceRange( Integer.parseInt( className ) );
            }
            
            return (SequencePreallocator) Class.forName( className ).newInstance();
        }
        catch (ClassNotFoundException e) { throw missingAllocator( propertyName, className, e ); }
        catch (ClassCastException e) { throw missingAllocator( propertyName, className, e ); }
        catch (InstantiationException e) { throw missingAllocator( propertyName, className, e ); }
        catch (IllegalAccessException e) { throw missingAllocator( propertyName, className, e ); }
        catch (NumberFormatException e) { throw missingAllocator( propertyName, className, e ); }
    }
    private StandardException   missingAllocator( String propertyName, String className, Exception e )
    {
        return StandardException.newException( SQLState.LANG_UNKNOWN_SEQUENCE_PREALLOCATOR, e, propertyName, className );
    }
    private boolean isNumber( String text )
    {
        int length = text.length();

        for ( int i = 0; i < length; i++ )
        {
            if ( !Character.isDigit( text.charAt( i ) ) ) { return false; }
        }

        return true;
    }
    
	private static LanguageConnectionContext getLCC()
    {
		return (LanguageConnectionContext) 
					ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
	}

    /** Report an unimplemented feature */
    private StandardException unimplementedFeature()
    {
        return StandardException.newException( SQLState.BTREE_UNIMPLEMENTED_FEATURE );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Specific implementation of SequenceUpdater for the sequences managed by
     * SYSSEQUENCES.
     * </p>
     */
    public static final class SyssequenceUpdater extends SequenceUpdater
    {
        private RowLocation _sequenceRowLocation;

        public SyssequenceUpdater() { super(); }
        public SyssequenceUpdater( DataDictionaryImpl dd ) { super( dd ); }
    
        //
        // SequenceUpdater BEHAVIOR
        //

        protected SequenceGenerator createSequenceGenerator( TransactionController readOnlyTC )
            throws StandardException
        {
            RowLocation[] rowLocation = new RowLocation[ 1 ];
            SequenceDescriptor[] sequenceDescriptor = new SequenceDescriptor[ 1 ];
            
            _dd.computeSequenceRowLocation( readOnlyTC, _uuidString, rowLocation, sequenceDescriptor );
            
            _sequenceRowLocation = rowLocation[ 0 ];
            
            SequenceDescriptor isd = sequenceDescriptor[ 0 ];
            
            return new SequenceGenerator
                (
                 isd.getCurrentValue(),
                 isd.canCycle(),
                 isd.getIncrement(),
                 isd.getMaximumValue(),
                 isd.getMinimumValue(),
                 isd.getStartValue(),
                 isd.getSchemaDescriptor().getSchemaName(),
                 isd.getSequenceName(),
                 makePreallocator( readOnlyTC )
                 );
        }

        protected boolean updateCurrentValueOnDisk( TransactionController tc, Long oldValue, Long newValue, boolean wait ) throws StandardException
        {
            return _dd.updateCurrentSequenceValue( tc, _sequenceRowLocation, wait, oldValue, newValue );
        }
    }

    /**
     * <p>
     * Implementation of SequenceUpdater for use with the bulk-insert optimization
     * used by InsertResultSet. This BulkInsertUpdater doesn't really write to disk. It is assumed
     * that the BulkInsertUpdater will only be used by the bulk-insert code, where the
     * user has exclusive write-access on the table whose identity column is backed by
     * the original SequenceUpdater. At the end of bulk-insert, the current value of the
     * BulkInsertUpdater is written to disk by other code.
     * </p>
     */
    public static final class BulkInsertUpdater extends SequenceUpdater implements BulkInsertCounter
    {
        public BulkInsertUpdater() { super(); }
        public BulkInsertUpdater( SequenceUpdater originalUpdater, boolean restart )
        {
            _sequenceGenerator = originalUpdater._sequenceGenerator.clone( restart );
        }
    
        //
        // SequenceUpdater BEHAVIOR
        //

        protected SequenceGenerator createSequenceGenerator( TransactionController readOnlyTC )
            throws StandardException
        {
            return _sequenceGenerator;
        }

        protected boolean updateCurrentValueOnDisk( TransactionController tc, Long oldValue, Long newValue, boolean wait )
            throws StandardException
        {
            // always succeeds
            return true;
        }
    }

}

