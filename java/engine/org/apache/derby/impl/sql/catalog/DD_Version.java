/*

   Derby - Class org.apache.derby.impl.sql.catalog.DD_Version

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TabInfo;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.info.ProductGenusNames;
import org.apache.derby.iapi.services.info.ProductVersionHolder;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.DB2Limit;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Types;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Generic code for upgrading data dictionaries.
 * Currently has all minor version upgrade logic.
 * <p>
 * A word about minor vs. major upgraded.  Minor
 * upgrades must be backwards/forwards compatible.
 * So they cannot version classes or introduce new
 * classes.  Major releases are only backwards compatible;
 * they will run against an old database, but not the
 * other way around.  So they can introduce new classes,
 * etc.
 *
 * @author Rick
 */

public	class DD_Version implements	Formatable
{
	////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	////////////////////////////////////////////////////////////////////////

	private		transient	DataDictionaryImpl	bootingDictionary;

	int majorVersionNumber;
	private int minorVersionNumber;

	////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	////////////////////////////////////////////////////////////////////////

	/**
	  *	Public niladic constructor needed for Formatable interface.
	  */
	public	DD_Version() {}


	/**
	 * Construct a Version for the currently booting data dictionary.
	 * The minor version is set by the subclass.
	 *
	 * @param	bootingDictionary	The booting dictionary that needs to be upgraded.
	 */
	DD_Version( DataDictionaryImpl bootingDictionary, int majorVersionNumber)
	{
		this.majorVersionNumber = majorVersionNumber;
		this.minorVersionNumber = getJBMSMinorVersionNumber();
		this.bootingDictionary = bootingDictionary;
	}

	////////////////////////////////////////////////////////////////////////
	//
	//	OVERRIDE OBJECT METHODS
	//
	////////////////////////////////////////////////////////////////////////

	/**
	  *	Stringify this Version.
	  *
	  *	@return	String representation of this Version.
	  */
	public	String	toString()
	{
		return DD_Version.majorToString(majorVersionNumber);
	}

	private static String majorToString(int majorVersionNumber) {
		switch (majorVersionNumber) {
		case DataDictionary.DD_VERSION_CS_5_0:
			return "5.0";
		case DataDictionary.DD_VERSION_CS_5_1:
			return "5.1";
		case DataDictionary.DD_VERSION_CS_5_2:
			return "5.2";
		case DataDictionary.DD_VERSION_CS_8_1:
			return "8.1";
		case DataDictionary.DD_VERSION_CS_10_0:
			return "10.0";
		default:
			return null;
		}
	}

	////////////////////////////////////////////////////////////////////////
	//
	//	DataDictionary SPECIFIC
	//
	////////////////////////////////////////////////////////////////////////

	/**
	 * Upgrade the data dictionary catalogs to the version represented by this
	 * DD_Version.
	 *
	 * @param dictionaryVersion the version of the data dictionary tables.
	 * @exception StandardException Ooops
	 */
	void upgradeIfNeeded(DD_Version dictionaryVersion,
								TransactionController tc, Properties startParams)
		 throws StandardException
	{
		// database has been upgrade with a later engine version than this?
		if (dictionaryVersion.majorVersionNumber > majorVersionNumber) {
			throw StandardException.newException(SQLState.LANG_CANT_UPGRADE_CATALOGS,  
				dictionaryVersion, this);
		}


		boolean minorOnly = false;
		boolean performMajorUpgrade = false;
		boolean softUpgradeRun = false;


		if (dictionaryVersion.majorVersionNumber == majorVersionNumber) {

			// exact match of engine to database, do nothing.
			if (dictionaryVersion.minorVersionNumber == minorVersionNumber)
				return;

			// database and engine at same major level
			minorOnly = true;

		} else {
           
			if (Monitor.isFullUpgrade(startParams, dictionaryVersion.toString())) {
				performMajorUpgrade = true;
			} else {
				softUpgradeRun = true;
			}
		}

		// make sure we have a clean transaction for the upgrade
		tc.commit();

		if (performMajorUpgrade) {
			// real upgrade changes.
			doFullUpgrade( tc, dictionaryVersion.majorVersionNumber );
		}

		if (!minorOnly) {
			// apply changes that can be made and will continue to work
			// against previous version.

			// See if we have already applied these changes.
			DD_Version softUpgradeVersion = (DD_Version) tc.getProperty(
											DataDictionary.SOFT_DATA_DICTIONARY_VERSION);

			// need to apply them if we have never performed a soft upgrade
			// or only a soft upgrade using a previous version.
			int softUpgradeMajorVersion = 0;
			if (softUpgradeVersion != null)
				softUpgradeMajorVersion = softUpgradeVersion.majorVersionNumber;

			if (softUpgradeMajorVersion < majorVersionNumber) {
				applySafeChanges( tc, dictionaryVersion.majorVersionNumber, softUpgradeMajorVersion);
			}
		}

		// changes such as invalidating SPS so they will recompile against
		// the new internal classes.
		// this method also changes the on-disk format version on the disk and in-memory as well.
		handleMinorRevisionChange(tc, dictionaryVersion, softUpgradeRun);

		// commit any upgrade
		tc.commit();
	}

	/**
		Apply changes that can safely be made in soft upgrade.
		Any changes must not prevent the database from being re-booted
		by the a Cloudscape engine at the older version fromMajorVersionNumber

	  *
	  * @param	tc	transaction controller
	  * @param	fromMajorVersionNumber	version of the on-disk database
	    @param  lastSoftUpgradeVersion last engine to perform a soft upgrade that made changes.
	  *
	  *	@exception StandardException  Standard Cloudscape error policy.
	  */
	private	void	applySafeChanges(TransactionController tc, int fromMajorVersionNumber, int lastSoftUpgradeVersion)
		throws StandardException
	{

		if (lastSoftUpgradeVersion <= DataDictionary.DD_VERSION_CS_5_1)
		{

			// All these soft upgrade actions are new in 5.2 (first ever soft upgrade)
			if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_CS_5_0)
				modifySysTableNullability(tc,
					DataDictionaryImpl.SYSALIASES_CATALOG_NUM);

			if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_CS_5_1)
				modifySysTableNullability(tc,
					DataDictionaryImpl.SYSSTATEMENTS_CATALOG_NUM);

		}

		tc.setProperty(DataDictionary.SOFT_DATA_DICTIONARY_VERSION, this, true);
	}

	/**
		Do full upgrade.  Apply changes that can NOT be safely made in soft upgrade.

	  *
	  * @param	tc	transaction controller
	  * @param	fromMajorVersionNumber	version of the on-disk database
	  *
	  *	@exception StandardException  Standard Cloudscape error policy.
	  */
	private	void	doFullUpgrade(TransactionController tc, int fromMajorVersionNumber)
		throws StandardException
	{
		if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_CS_5_1)
		{
			// drop sps in SYSIBM, SYSIBM, recreate SYSIBM, SYSDUMMY1, populate SYSDUMMY1, create procs
			dropJDBCMetadataSPSes(tc, true);
			SchemaDescriptor sd = bootingDictionary.getSchemaDescriptor("SYSIBM", null, false);
			if (sd != null)
				bootingDictionary.dropSchemaDescriptor("SYSIBM", tc);
			sd = bootingDictionary.getSysIBMSchemaDescriptor();
			bootingDictionary.addDescriptor(sd, null, DataDictionary.SYSSCHEMAS_CATALOG_NUM, false, tc);
			bootingDictionary.upgradeMakeCatalog(tc, DataDictionary.SYSDUMMY1_CATALOG_NUM);
			bootingDictionary.populateSYSDUMMY1(tc);
			bootingDictionary.create_SYSIBM_procedures(tc);
			bootingDictionary.createSystemSps(tc);
		}
	}

	/**
	 * Do any work needed for a minor revision change.
	 * For the data dictionary this is always invalidating
	 * stored prepared statements.  When we are done 
	 * with the upgrade, we always recompile all SPSes
	 * so the customer doesn't have to (and isn't going
	 * to get deadlocks because of the recomp).
	 *
	 * @param tc the xact
	 *
	 * @exception StandardException  Standard Cloudscape error policy.
	 */
	private void handleMinorRevisionChange(TransactionController tc, DD_Version fromVersion, boolean softUpgradeRun) 
		throws StandardException
	{
		boolean isReadOnly = bootingDictionary.af.isReadOnly();

		if (!isReadOnly) {
			bootingDictionary.clearSPSPlans();

			DD_Version lastRun;
			
			if (softUpgradeRun)
			{
				// log a version that will cause a minor revision change
				// for any subsequent re-boot, including an old Cloudscape version
				fromVersion.minorVersionNumber = 1; // see getJBMSMinorVersionNumber
				lastRun = fromVersion;
			}
			else
			{
				// log the new version
				lastRun = this;
			
				// and change the in-memory version.
				fromVersion.majorVersionNumber = majorVersionNumber;
				fromVersion.minorVersionNumber = minorVersionNumber;
			}

			tc.setProperty(DataDictionary.CORE_DATA_DICTIONARY_VERSION, fromVersion, true);
		}
		else
		{
			// For a readonly database where we need some kind of upgrade
			// (either minor release or soft upgrade) then since we cannot
			// invalidate all the procedures we need to indicate that
			// any procedure we read off disk is automatically invalid,
			// so we do not try to load the generated class.
			bootingDictionary.readOnlyUpgrade = true;
		}

		bootingDictionary.clearCaches();
	}

	/**
	 * Drop all jdbc metadata spses.  This
	 * it to ensure that we don't have any problems
	 * with old metadata queries that have outdated
	 * query text (the plans are always cleared out
	 * on upgrade time).
	 *
	 * @param tc the xact
	 *
	 * @exception StandardException  Standard Cloudscape error policy.
	 */
	protected void dropJDBCMetadataSPSes(TransactionController tc, boolean removeSYSIBMonly)
		throws StandardException
	{
		for (java.util.Iterator it = bootingDictionary.getAllSPSDescriptors().iterator(); it.hasNext(); )
		{
			SPSDescriptor spsd = (SPSDescriptor) it.next();
			SchemaDescriptor sd = spsd.getSchemaDescriptor();
			// need to compare the name, old SYSIBM is not built-in
			boolean isSYSIBM = sd.getSchemaName().equals(SchemaDescriptor.IBM_SYSTEM_SCHEMA_NAME);
			if (! sd.isSystemSchema() && ! isSYSIBM)
				continue;
			/*
			** Is it in SYS? if so, zap it.
			*/
			if ((removeSYSIBMonly && isSYSIBM) || (! removeSYSIBMonly && ! isSYSIBM))
			{
				bootingDictionary.dropSPSDescriptor(spsd, tc);
				bootingDictionary.dropDependentsStoredDependencies(spsd.getUUID(), tc);
			}
		}
	}

	/**
 	 * Make a catalog.
	 *	@param	tc	TransactionController
	 *  @param  crf CatalogRowFactory for the catalog to drop.
	 *	@exception StandardException  Standard Cloudscape error policy.
	 */
	protected void makeSystemCatalog(TransactionController tc,
									 TabInfo ti)
		throws StandardException
	{
		SchemaDescriptor sd = bootingDictionary.getSystemSchemaDescriptor();
		bootingDictionary.makeCatalog(ti,sd,tc);
	}

	/**
	  Remove the description of a System table from the data dictionary.
	  This does not delete the conglomerates that hold the catalog or
	  its indexes.
	  @param	tc TransactionController
	  @param    td Table descriptor for the catalog to drop. 
	  @exception StandardException  Standard Cloudscape error policy.
	  */
	protected void
	dropSystemCatalogDescription(TransactionController tc, TableDescriptor td)
		throws StandardException
	{
		/* Drop the columns */
		bootingDictionary.dropAllColumnDescriptors(td.getUUID(), tc);

		/* Drop the conglomerate descriptors */
		bootingDictionary.dropAllConglomerateDescriptors(td, tc);

		/* Drop table descriptor */
		bootingDictionary.dropTableDescriptor( td, td.getSchemaDescriptor(), tc );
		bootingDictionary.clearCaches();
	}

	/**
 	 * Drop a System catalog.
	 *	@param	tc	TransactionController
	 *  @param  crf CatalogRowFactory for the catalog to drop.
	 *	@exception StandardException  Standard Cloudscape error policy.
	 */
	protected void dropSystemCatalog(TransactionController tc,
							 CatalogRowFactory crf)
		throws StandardException
	{
		SchemaDescriptor		sd = bootingDictionary.getSystemSchemaDescriptor();
		TableDescriptor			td = bootingDictionary.getTableDescriptor(
											crf.getCatalogName(),
											sd);
		ConglomerateDescriptor[]	cds = td.getConglomerateDescriptors();
		for (int index = 0; index < cds.length; index++)
		{
			tc.dropConglomerate(cds[index].getConglomerateNumber());
		}
		dropSystemCatalogDescription(tc,td);
	}


	/**
	 * Populates a new system index from the base system table.
	 *
	 *	@param	tc						transaction controller
	 *	@param	heapConglomerateNumber	identifies system table to Store
	 *	@param	tabInfo					describes base system table
	 *	@param	indexNumber				index to populate
	 *
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected	void	fillIndex
	(
		TransactionController	tc,
		long					heapConglomerateNumber,
		TabInfo					tabInfo,
		int						indexNumber
    )
		throws StandardException
	{
		long						indexConglomerateNumber = tabInfo.getIndexConglomerate( indexNumber );
		IndexRowGenerator			indexRowGenerator = tabInfo.getIndexRowGenerator( indexNumber );
		CatalogRowFactory			rowFactory = tabInfo.getCatalogRowFactory();
		ExecRow						heapRow = rowFactory.makeEmptyRow();
		ExecIndexRow				indexableRow = indexRowGenerator.getIndexRowTemplate();

		ScanController				heapScan =
			tc.openScan(
				heapConglomerateNumber,       // conglomerate to open
				false,                          // don't hold open across commit
				0,                              // for read
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
				(FormatableBitSet) null,                 // all fields as objects
				null,                           // start position - first row
				ScanController.GE,              // startSearchOperation
				null,                           //scanQualifier,
				null,                           //stop position-through last row
				ScanController.GT);             // stopSearchOperation

		RowLocation					heapLocation = 
            heapScan.newRowLocationTemplate();

		ConglomerateController		indexController = 
			tc.openConglomerate( 
				indexConglomerateNumber, 
                false,
				TransactionController.OPENMODE_FORUPDATE,
				TransactionController.MODE_TABLE,
				TransactionController.ISOLATION_REPEATABLE_READ);

		while ( heapScan.next() )
        {
            heapScan.fetch( heapRow.getRowArray() );
			heapScan.fetchLocation( heapLocation );

			indexRowGenerator.getIndexRow( heapRow, heapLocation, indexableRow, (FormatableBitSet) null );

			indexController.insert(indexableRow.getRowArray());
		}

		indexController.close();
		heapScan.close();
	}

	////////////////////////////////////////////////////////////////////////
	//
	//	FORMATABLE INTERFACE
	//
	////////////////////////////////////////////////////////////////////////
	/**
	 * Get the formatID which corresponds to this class.
	   Map to the 5.0 version identifier so that 5.0 will understand
	   this object when we write it out in soft upgrade mode.
	   CS 5.0 will de-serialize it correctly.
	   When we are writing out a 5.1 version number we write out
	   the 5.1 version just to ensure no problems.
	   
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{
		return majorVersionNumber == DataDictionary.DD_VERSION_CS_5_1 ?
			StoredFormatIds.DD_ARWEN_VERSION_ID : StoredFormatIds.DD_DB2J72_VERSION_ID;
	}
	/**
	 * Read this object from a stream of stored objects. Set
	 * the minor version.  Ignore the major version.  
	 *
	 * @param in read this.
	 *
	 * @exception IOException on error
	 */
	public final void readExternal( ObjectInput in ) throws IOException
	{
		majorVersionNumber = in.readInt();
		minorVersionNumber = in.readInt();
	}

	/**
	 * Write this object to a stream of stored objects. Write
	 * out the minor version which is bumped across minor release.
	 * Just to be safe, write out the major version too.  This
	 * will allow us to do versioning of a specific Version impl
	 * in the future.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException on error
	 */
	public final void writeExternal( ObjectOutput out ) throws IOException
	{ 
		out.writeInt(majorVersionNumber);
		out.writeInt(minorVersionNumber);
	}
	/**
	 * Get the minor version from the JBMS product minor version/maint version.
	 * Bumps it up by 1 if production, or 0 if beta to ensure
	 * minor upgrade across beta.  Starts at 2 because of an
	 * old convention. We use this starting at 2 to allow soft upgrade to
	 * write a version of 1 with the old major number to ensure a minor upgrade
	   when reverting to an old version afer a soft upgrade. E.g run with 5.0.2,
	   then 5.2.1.1, then 5.0.2. Want to ensure 5.0.2 does the minor upgrade.
	 *
	 * @return the minor version

		For 5.0 and 5.1 the minor number was calculated as

		jbmsVersion.getMinorVersion()*100 +jbmsVersion.getMaintVersion() + (jbmsVersion.isBeta() ? 0 : 1) + 2

		5.0.22 => (0*100) + 22 + 2 =  24 - (5.0 has a unique major number)
		5.1.2  => (1*100) + 2 + 2  = 104 - (5.1 has a unique major number) 


		With the switch to the four part scheme in 5.2, the maint number now is in increments of one million,
		thus the above scheme could lead to duplicate numbers. Note that the major number may not change
		when the minor external release changes, e.g. 5.2 and 5.3 could share a DD_Version major number.

		5.2.1.100 => (2*100) + 1000100 + 2 = 1000302
		5.3.1.0   => (3*100) + 1000000 + 2 = 1000302

		

	 */
	private int getJBMSMinorVersionNumber() 
	{
		ProductVersionHolder jbmsVersion = Monitor.getMonitor().getEngineVersion();

		return jbmsVersion.getMinorVersion()*100 +jbmsVersion.getMaintVersion() + (jbmsVersion.isBeta() ? 0 : 1) + 2;
	}
	
	/**
	 * Modifies the nullability of the system table corresponding
	 * to the received catalog number.
	 *
	 * @param tc			TransactionController.
	 * @param catalogNum	The catalog number corresponding
	 *  to the table for which we will modify the nullability.
	 *  If this corresponds to SYSALIASES, then the nullability of
	 *  the SYSALIASES.ALIASINFO column will be changed to true
	 *  (Beetle 4430).  If this corresponds to SYSSTATEMENTS,
	 *  the nullability of the SYSSTATEMENTS.LASTCOMPILED
	 *  column will be changed to true.
	 *
	 * @exception StandardException
	 */

	private void modifySysTableNullability(TransactionController tc, int catalogNum)
	throws StandardException
	{

		TabInfo ti = bootingDictionary.getNonCoreTIByNumber(catalogNum);
		CatalogRowFactory rowFactory = ti.getCatalogRowFactory();
		if (catalogNum == DataDictionaryImpl.SYSALIASES_CATALOG_NUM) {
		// SYSALIASES table ==> ALIASINFO needs to be modified.
			bootingDictionary.upgrade_setNullability(rowFactory,
				SYSALIASESRowFactory.SYSALIASES_ALIASINFO, true, tc);
		}
		else if (catalogNum == DataDictionaryImpl.SYSSTATEMENTS_CATALOG_NUM) {
		// SYSSTATEMENTS table ==> LASTCOMPILED needs to be modified.
			bootingDictionary.upgrade_setNullability(rowFactory,
				SYSSTATEMENTSRowFactory.SYSSTATEMENTS_LASTCOMPILED, true, tc);
		}

	}

	/**
		Check to see if a database has been upgraded to the required
		level in order to use a language feature.

		@param majorVersion Data Dictionary major version
		@param feature Non-null to throw an error, null to return the state of the version match.

		@return True if the database has been upgraded to the required level, false otherwise.
	*/
	boolean checkVersion(int requiredMajorVersion, String feature) throws StandardException {

		if (majorVersionNumber < requiredMajorVersion) {

			if (feature != null)
				throw StandardException.newException(SQLState.LANG_STATEMENT_UPGRADE_REQUIRED, feature,
					DD_Version.majorToString(majorVersionNumber),
					DD_Version.majorToString(requiredMajorVersion));

			return false;
		}

		return true;
	}

}
