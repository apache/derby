/*

   Derby - Class org.apache.derby.iapi.jdbc.ResourceAdapter

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.jdbc;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.store.access.xa.XAResourceManager;
import org.apache.derby.iapi.store.access.xa.XAXactId;
import org.apache.derby.iapi.error.StandardException;

/**
	The resource adapter is the clearing house for managing connections,
	transactions, and XAResources in a JDBC based resource manager living in
	the distributed transaction processing environment.  

	<P> There is one instance of ResourceAdapter per Resource Manager (database).
	The ResourceAdapter is responsible for keeping track of all run time global
	transactions and their state.   The resource adapter only knows of run time
	global transactions, i.e., it does not know of in-doubt global transactions
	re-created by recovery.

	<P>	The following is an overall design of the JTA implementation in cloudscape,
	most of it has little to do with the ResourceAdapter interface itself.
	<P><B>Design Overview </B>

	<P>The overriding design principle is that existing code should be disturbed
	as little as possible.  This is so that DTP code will not add to the bloat
	and drag of a normal, local, embbeded system.  The second design principle
	is that as much of the JDBC 2.0 extension functionality is to be
	implemented in the Connectivity layer and not in the underlying storage
	system as possible.  Ideally, the additional storage interface will
	implement no more than what is necessary to support the XAResource
	interface.

	<P>Language and replication code should not be touched, or have very
	minimal API changes.  The API changes are confined to passing XA calls down
	to the store.

	<P>Some change will be made to existing Connectivity code and new XA
	modules will be added.  This collection of code is hereby referred to as
	the "blob of mysterious connectivity code", or the "resource adapter", or
	"RA" for short.  In the JTA doc, the resource adapter is considered to be
	part of the JDBC driver.  This RA means "some connectivity code", it
	doesn't mean the object that implements the ResourceAdapter interface.

	<P>The most important difference, in terms of implementation, between a
	Connection that deals with a local transaction and a Connection that deals
	with a global transaction is that in a global transaction, 2 or more
	objects and threads can influence it - maybe concurrently.  The normal JDBC
	interaction goes thru the Connection, but transaction demarcation comes
	from an XAResource object(s).  The RA will channel all XAResource calls
	that deal with a run time XA transaction (i.e., commit, end, forget,
	prepare, start) thru the TransactionController that represents the real
	transaction underneath.   Furthermore, the RA will make sure that all calls
	thru a Connection or thru any XAResource objects must pass thru some sort
	of synchronized object before it can get to the underlying transaction
	object.  This is so that there is only one path to change the state of a
	run time transaction and the transaction object and the context manager can
	remain single thread access.

	<P>In-doubt transaction (i.e., transactions re-created by recovery)
	management and concurrency control is the responsibiliy of store. Moreover,
	since the RA does not know the identities of the list of in-doubt
	transactions, store must deal with (throw exception) when someone wants to
	start a transaction with the same Xid as an existing in-doubt transaction.

	<P>In terms of what this means to the app server that is calling us: if the
	Connection and the XAResource that represents a global transaction is being
	accessed by 2 different threads, they will access the database serially and
	not concurrently. An in-doubt transaction gotten thru recovery has no
	transaction object that is ever visible to the RA - because there is no
	connection that was ever made to it.  Therefore it is safe to influence the
	state of an in-doubt transaction directly thru some store factory interface
	- and have that go thru the transaction table underneath to find the actual
	transaction object and context manager etc.

	<P>One new functionality of a Connection is the ability to switch around
	with different transactions.  Before JTA, the lifetime of a transaction is
	bounded by a connection, and a transaction cannot migrate from one
	connection to another.  In JTA, a global transaction can be detached from a
	Connection.  A transaction can move around and be attached to different
	connections and its lifetime is not confine to the connection that started
	it.  From the Connection's point of view, before JTA, a (local) transaction
	is always started and ended in the same connection. With JTA, it needs to
	"take on" existing global transactions that was started by some other
	connections.

	<P>The RA will have the responsibility of 
	<OL>
	<LI>setting up a Context with the appropriate transaction before calling
	store to do work.</LI>
	<LI>handling error on the context.</LI>
	<LI>restoring a previous context if it was switched out due to an XAResouce
	call to commit a transaction that is not what the XAResoruce is currently
	attached to. </LI>
	</OL>

	<P>Because of all these switching around, a Connection may be in a
	transaction-less state.  This happens between an XAResource.end call that
	detached the current global transaction from the Connection, and the next
	XAResource.start call that attach the next global transaction with the
	Connection.

	<BR>An (inferior) implementation is for the Connection object to start a
	local connection once it is detached from a global transaction.  If the
	user then uses the Connection immediately without a XAResource.start call,
	then this Connection behaves just like it did before JTA, i.e., with a
	local transaction.  If, on the other hand, an XAResource.start call happens
	next, then either the local transaction is "morphed" into a global
	transaction, or, if the start call is to attach the connection to a
	pre-existing global transaction, then the local transaction is thrown away
	and the Connection will take on the pre-exising global transaction.

	<BR>Another (superior) implementation is to make it possible for a
	Connection to be transaction-less.  When a Connection is first created by
	XAConnection.getConnection, or when a XAResource.end call detached a global
	transaction from the Connection, it is left in a transaction-less state.
	If a XAResource.start call happens next, then the Connection either start a
	new global transaction or it takes on an existing one.  If a call is made
	directly on the Connection before XAResource.start call happens, then the
	Connection starts a new local transaction.  This only affects Connections
	that was gotten thru the XAConnection.getConnection().  Connections gotten
	thru the DriverManager or a DataSource will have a local transaction
	automatically started, as is the behavior today.  When a Connection with a
	local transaction commits, the transaction is still around but it is chain
	to the next one - this is the current behavior.  This behavior is very
	desirable from a performance point of view, so it should be retained.
	However, a local transaction cannot "morph" into a global transaction,
	therefore when this Connection is attached to a global transaction, the
	local transaction is thrown away and a global one started

	<P>The RA will need to keep track of all global transactions.  This is done
	by (yet another) transaction table that lives in the RA.  This transaction
	table maps Xid to the ContextManager of the global transaction and whatever
	else a connection needs to talk to the transaction - I assume the
	Connection object currently have tendrils into various contexts and objects
	and these are things that need to be detached and attached when a
	Connection is hooked up with another transaction.  The reason for yet
	another transaction table instead of the one in store is because the one in
	store keeps track of local and internal transactions and is really quite
	overworked already.

	<P><B>Detailed design</B>

	<BR> First some ugly pictures.  Some links are not shown to reduce
	clutter.  Externally visible object is in <B>bold</B>.
  
	<P><PRE>
* 
* When user ask for an XAConnection via a XADataSource, the following objects
* exists 
* <BR>
*
*                                                     |-------------|
*                                  |======= produces=>| <B>XAResource</B>  |
*                                  ||                 |-------------|
*                                  ||                       |
*                                  ||                     has A
*                                  ||                       |
*                                  ||  |---------------------
*                                  ||  V
* |--------------| produces |--------------| 
* | <B>XADataSource</B> |=========>| <B>XAConnection</B>
* |--------------|          |--------------| 
*       |                          | 
*     extends                    extends
*       |                          | 
*       |                |-----------------------|   |----------------------|
*       |                | DB2jPooledConnection |==>| BrokeredConnection |
*       |                |-----------------------|   |----------------------|
*       |                          |       ^                  |
*       |                        has A     |               has A
*       |                          |       |                  |
* |-----------------|              |       --------------------
* | EmbeddedDataSource |              |
* |-----------------|              |
*       |                          |
*     has A                        |
*       |                          |
*       V                          V
* |------------|           |----------------------|   |-----------------------|
* | JDBCDriver |=produces=>| DetachableConnection |==>| XATransactionResource |
* | LocalDriver|           |----------------------|   |                       |
* |------------|                   |                  |   points to :         |
*                                  |                  |XATransactionController|
*                                  |                  | ContextManager        |
*                                  |                  | LCC                   |
*                                  |                  | .. etc ..             |
*                                  |                  |-----------------------| 
*                                  |                            |
*                                extends                     extends
*                                  |                            |
*                           |-----------------|       |-----------------------|
*                           | EmbedConnection |-- ?-->|  TransactionResource  |
*                           |-----------------|       |-----------------------|
*
* 
* <BR><BR>
* When user ask for a PooledConnection via a PooledDataSource, the following
* objects exists 
* <BR>
* |-------------------------------|
* | <B>EmbeddedConnectionPoolDataSource</B> |
* |-------------------------------|
*       |                  ||
*       |                  ||
*     extends             produces
*       |                  ||
*       |                  \/
*       |                |-----------------------|   |----------------------|
*       |                | <B>DB2jPooledConnection</B> |==>| <B>BrokeredConnection</B> |
*       |                |-----------------------|   |----------------------|
*       |                          |       ^                  |
*       |                        has A     |               has A
*       |                          |       |                  |
* |-----------------|              |       --------------------
* | EmbeddedDataSource |              |
* |-----------------|              |
*       |                          |
*     has A                        |
*       |                          |
*       V                          V
* |------------|           |----------------------|   |-----------------------|
* | JDBCDriver |=produces=>| EmbedConnection |==>|  TransactionResource  |
* | LocalDriver|           |----------------------|   |-----------------------|
* |------------| 
* 
* 
* 
* <BR><BR>
* When user ask for a (normal) Connection via a DataSource, the following
* objects exists. The EmbeddedDataSource is just a wrapper for the JDBCDriver.
* <BR>
* |-----------------|
* | <B>EmbeddedDataSource</B> |
* |-----------------|
*       |
*     has A
*       |
*       V
* |------------|            |-----------------|     |-----------------------|
* | JDBCDriver |==produces=>| <B>EmbedConnection</B> |- ?->| TransactionResource   |
* | LocalDriver|            |-----------------|     |-----------------------|
* |------------|

	</PRE>

	<P>XADataSource inherits DataSource methods from EmbeddedDataSource.  It also
	implements ResourceAdapter, whose main job is to keep track of run time
	global transactions.  A global transaction table maps XIDs to
	XATransactionResource.  XADataSource also has a XAResourceManager, which 
	implements XAResource functionality in the Store.
	
	<P>XAConnection is the one thing that unites a global connection and the
	XAResource that delineates the global transaction.  This is where the real
	XAResource functionality is implemented.  All XAResource calls to the
	XAResource object as well as Connection call to the BrokeredConnection
	channels thrus the XAConnection, which makes sure only one thread can be
	accessing the DB2jPooledConnection at any given time.

	<P>XAResource and BrokeredConnection[23]0 are the two objects we give back
	to the TM and the user application respectively to control a distributed
	transaction.  According to the XA spec, the app server is supposed to make
	sure that these objects are not used the same time by multiple threads, but
	we don't trust the app server.  Therefore, we channel everthing back to the
	XAConnection.

	<P>The MT consideration is actually more complicated than this,
	because a XAResource is allowed to control any transaction, not just the
	one its XAConnection is current attached to.  So it is not sufficient to
	just synchronized on XAConnection to guarentee single thread access to the
	underlying transaction context.  To control some arbitrary global
	transaction, the TM can call XAResource to prepare any Xid.  To do that,
	the XAResource pass the request to the XAConnection, the XAConnection ask
	the XADataSource to find the XATransactionResource, sets up the thread's
	context, and call ask the XATransactionResource to prepare.  The
	XATransactionResource is synchronized to prevent some other thread from
	attaching, commiting, and in any way calling on the the same transaction
	context.  If any error is thrown, it is handled with the context of the
	transaction being prepared.  After the error is handled, the old context
	(the one where the XAResource is really attached to), is restored.  While
	this monkey business is going on, the thread that holds the connection the
	XAConnection is supposed to be attached to is blocked out.  It can resume
	after its XAConnection restored its context.  (Here is where I am not
	really sure what happens since that thread obviously doesn't know about all
	these hanky panky caused by the thread holding the XAResource commiting,
	preparing and rolling back some other irrelavant transactions, so how would
	its context be affected in any way?).

	<P>DB2jPooledConnection implements PooledConnection, is hands out these
	connection handles which allows some app server to do connection pooling.
	This is a very thin layer.  A connection handle implements a Connection by
	passing thru all calls to the underlaying connection.  In this case, it
	passes Connection call thru the DB2jPooledConnection to the
	DetachableConnection underneath.

	<P>EmbeddedDataSource implements JNDI and is a replacement for Driver.

	<P>The LocalDriver can now produce a DetachableConnection as well as a
	EmbedConnection (which is the pre-JTA Connection that cannot detach and
	attach to different transactions).  The way the LocalDriver knows to create
	a DetachableConnection versus a EmbedConnection is thru some extremely
	hackish URL settings.  This thing is very ugly and a more elegant way can
	(and should) no doubt be found.

	<P>DetachableConnection is a connection which can detach and attach to
	different XATransactionResource, and can be totally unattached to any
	transaction.

	<P>XATransactionResource is a bundle of things that sets up a connection
	with all the stuff it needs to actually talk to the database, do error
	handling, etc.  It is also the object that lives in the transaction table
	managed by the ResourceAdapter (XADataSource).  A XAResource (which may or
	may not be attached to a transaction) can commit, prepare, or rollback any
	global transaction that is not attached to an XAConnection.  To do that,
	the ResourceAdapter fishes out the XATransactionResource, set up the
	context, and do the commit processing/error handling on the current
	thread.

	<P>Local Connection is the same old local Connection except one
	difference.  Pre-JTA, a localConnection uses itself (or a root Connection)
	as the object to synchronized upon so that multiple threads getting hold of
	the same Connection object cannot simultaneously issue calls to the
	underlying transaction or context (since those things must be single thread
	access).  With JTA, the object of synchronization is the
	TransactionResource itself.  This part has not been well thought through
	and is probably wrong.

	<P>TransactionResource is a base class for XATransactionResource.  For a
	local transaction which cannot be detached from a connection, there is no
	need to encapsulate a bundle of things to set up a connection, so a
	TransactionResource (probably misnamed) has nothing and is used only for
	synchronization purposes.  This part has not been well thought throught and
	is probably wrong. 

	<P>The non-XA PooledConnection is just a thin veneer over the normal
	connection.  I now have it over a Detachable connection just to simplify
	the inheritence (XAConnection need to extend PooledConnection and XAConnect
	needs to be detachable.  However, PooledConnection itself need not be
	detachable).  It could be changed around to have LocalDriver producing
	either EmbedConnection or XAConnection, and have the XAConnection
	implements detachable.  But the current way is simpler.

 */
public interface ResourceAdapter {

	/**
		If a run time global transaction exists, the resource adapter will find
		it and return a capsule of information so that a Connection can be
		attached to the transaction. 

		@param xid the global transaction id
		@return the transaction resource if the xid correspond to a run
		time transaction, otherwise return null
	 */
	//XATransactionResource findTransaction(XAXactId xid);

	/**
		Start a run time global transaction.  Add this to the list of
		transactions managed by this resource adapter.

		@return true if transaction can be added, otherwise false (dupid).

	 */
	//boolean addTransaction(XATransactionResource tr);

	/**
		Terminates a run time global transction.  Remove this from the list of
		transactions managed by this resource adapter.
	 */
	//void removeTransaction(XATransactionResource tr);

	/**
		Let a xaResource get the XAResourceManager to commit or rollback an
		in-doubt transaction.
	 */
	XAResourceManager getXAResourceManager();

	/**
		Get the context service factory.
	 */
	//ContextService getContextServiceFactory();

	/**
		Is the Resource Manager active
	 */
	boolean isActive();

	public Object findConnection(XAXactId xid);

	public boolean addConnection(XAXactId xid, Object conn);

	public Object removeConnection(XAXactId xid);

}
