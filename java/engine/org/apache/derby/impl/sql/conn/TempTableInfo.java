/*

   Derby - Class org.apache.derby.impl.sql.conn.TempTableInfo

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.conn;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

//this class is for temporary tables. The information kept here is necessary to implement the rollback
//and commit behavior for temporary tables.

/**
The temp tables will have following data structure
TableDescriptor
Declared in savepoint level
Dropped in savepoint level
Modified in savepoint level

The actual logic

LanguageConnectionContext will keep the "current savepoint level". At any point in
time, this is the total number of savepoints defined for a transaction.
At the start of any new transaction, the "current savepoint level" will be set to 0.

Everytime a new user defined savepoint is set, store returns the total number of savepoints
for the connection at that point.
For eg, in a new transaction,
"current savepoint level' will be 0. When the first savepoint is set, store will return
1 and "current savepoint level" will be set to 1. For next savepoint, store will return 2 and so on
and so forth.

When language calls rollback or release of a savepoint, store will again return the total number of savepoints
for the connection after rollback or release and we will set "current savepoint level" to that number. For eg,
start tran ("current savepoint level"=0)
set savepoint 1 ("current savepoint level"=1)
set savepoint 2 ("current savepoint level"=2)
set savepoint 3 ("current savepoint level"=3)
set savepoint 4 ("current savepoint level"=4)
release savepoint 3 ("current savepoint level"=2)
rollback savepoint 1 ("current savepoint level"=0)

If the temporary table was declared with ON ROLLBACK DELETE ROWS and contents of that temporary table
were modified in a transaction or within a savepoint unit, then we keep track of that by saving the
savepoint level in dataModifiedInSavepointLevel. This information will be used at rollback of savepoint or transaction.
Also, when a savepoint is released, we check if the table was modified in any of the savepoints that
are getting released. If yes, then we put the current savepoint level as dataModifiedInSavepointLevel.
eg
start tran ("current savepoint level"=0)
declare temp table 0 ON ROLLBACK DELETE ROWS("declared in savepoint level"=0, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)
commit (temp table 0 ("declared in savepoint level"=-1, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1))
start tran ("current savepoint level = 0)
  temp table 0 ("declared in savepoint level"=-1, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)
set savepoint 1("current savepoint level = 1")
  temp table 0 ("declared in savepoint level"=-1, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)
set savepoint 2("current savepoint level = 2")
  delete 1 row from temp table 0
  temp table 0 ("declared in savepoint level"=-1, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=2)
release savepoint 2 ("current savepoint level"=1) and reset the modified in savepoint level as follows
  temp table 0 ("declared in savepoint level"=-1, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=1)
rollback ("current savepoint level"=0) All the rows from the temp table 0 will be removed
At the time of commit, we set dataModifiedInSavepointLevel to -1.
At the time of rollback (transaction / savepoint), first we check if the table was modified in the unit of work
getting rolled back. If yes, then we delete all the data from the temp table and we set dataModifiedInSavepointLevel to -1.

When language calls release of a savepoint, store will again return the total number of savepoints
in the system after release. We will go through all the temp tables and reset their declared or
dropped or modified in savepoint level to the value returned by the release savepoint if those tables had their
declared or dropped or modified in savepoint levels higher than what was returned by the release savepoint.
eg
start tran ("current savepoint level"=0)
declare temp table 0 ("declared in savepoint level"=0, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)
set savepoint 1("current savepoint level = 1")
declare temp table 1 ("declared in savepoint level"=1, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)
set savepoint 2("current savepoint level = 2")
declare temp table 2 ("declared in savepoint level"=2, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)
release savepoint 1 ("current savepoint level"=0) and reset the savepoint levels as follows
  temp table 1 ("declared in savepoint level"=0, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)
  temp table 2 ("declared in savepoint level"=0, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)
set savepoint 3("current savepoint level = 1")
rollback savepoint 3 ("current savepoint level"=0) and temp table info will look as follows
  temp table 0 ("declared in savepoint level"=0, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)
  temp table 1 ("declared in savepoint level"=0, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)
  temp table 2 ("declared in savepoint level"=0, "dropped in savepoint level"=-1, "dataModifiedInSavepointLevel"=-1)

When you declare a temp table, it will have "declared in savepoint level" as the current savepoint
level of the LanguageConnectionContext (which will be 0 in a transaction with no user-defined savepoints).
The "dropped in savepoint level" for new temp tables will be set to -1.
The "dataModifiedInSavepointLevel" for new temp tables will be set to -1 as well.

When a temp table is dropped, we will first check if the table was declared in a savepoint level
equal to the current savepoint level.
	If yes, then we will remove it from the temp tables list for the LanguageConnectionContext .
    eg
    start tran ("current savepoint level = 0")
    set savepoint 1("current savepoint level = 1")
    declare temp table 1 ("declared in savepoint level"=1, "dropped in savepoint level"=-1)
    drop temp table 1 (declared in savepoint level same as current savepoint level and hence will remove it from list of temp tables)
	If no, then we will set the dropped in savepoint level as the current savepoint level of the
		LanguageConnectionContext (which will be 0 in a transaction without savepoints and it also means
		that the table was declared in a previous transaction).

At the time of commit, go through all the temp tables with "dropped in savepoint level" != -1 (meaning dropped in this transaction)
and remove them from the temp tables list for the LanguageConnectionContext. All the rest of the temp tables with
"dropped in savepoint level" = -1, we will set their "declared in savepoint level" to -1 and , "dataModifiedInSavepointLevel" to -1.
eg
  start tran ("current savepoint level = 0)
	  declare temp table t1("declared in savepoint level" = 0, "dropped in savepoint level"=-1)
  commit (temp table 1 ("declared in savepoint level"=-1, "dropped in savepoint level"=-1))
  start tran ("current savepoint level = 0)
	  drop temp table t1 ("declared in savepoint level" = -1, "dropped in savepoint level"=0)
  commit (temp table t1 will be removed from list of temp tables)

At the time of rollback
  if rolling back transaction, first set the "current savepoint level" to 0
  if rolling back to a savepoint, first set the "current savepoint level" to savepoint level returned by Store
    for the rollback to savepoint command
  Now go through all the temp tables.
	If "declared in savepoint level" of temp table is greater than or equal to "current savepoint level"
  (ie table was declared in this unit of work)
    And if table was not dropped in this unit of work ie "dropped in savepoint level" = -1
      Then we should remove the table from the list of temp tables and drop the conglomerate created for it
		  eg
		  start tran ("current savepoint level = 0)
	  	declare temp table t2("declared in savepoint level" = 0, "dropped in savepoint level"=-1)
		  rollback tran
        (temp table t2 will be removed from list of tables and conglomerate associated with it will be dropped)
    And if table was dropped in this unit of work ie "dropped in savepoint level" >= "current savepoint level"
      Then we should remove the table from the list of temp tables
		  eg
		  start tran ("current savepoint level = 0)
      set savepoint 1("current savepoint level = 1")
	  	declare temp table t2("declared in savepoint level" = 1, "dropped in savepoint level"=-1)
      set savepoint 2("current savepoint level = 2")
		  drop temp table t1 ("declared in savepoint level" = 1, "dropped in savepoint level"=2)
		  rollback savepoint 1 ("current savepoint level = 0) temp table t1 will be removed from the list of temp tables
	Else if the "dropped in savepoint level" of temp table is greate than or equal to "current savepoint level"
	 	it mean that table was dropped in this unit of work (and was declared in an earlier savepoint unit / transaction) and we will
    restore it as part of rollback ie replace the existing entry for this table in valid temp tables list with restored temp table.
    At the end of restoring, "declared in savepoint level" will remain unchanged and "dropped in savepoint level" will be -1.
		eg
		  start tran ("current savepoint level = 0)
		  declare temp table t1 with definition 1("declared in savepoint level" = 0, "dropped in savepoint level"=-1, definition 1(stored in table descriptor))
		  commit (temp table t1 "declared in savepoint level" = -1, "dropped in savepoint level"=-1)
		  start tran ("current savepoint level = 0)
      set savepoint 1("current savepoint level = 1")
		  drop temp table t1 ("declared in savepoint level" = -1, "dropped in savepoint level"=1, definition 1(stored in table descriptor))
		  declare temp table t1 with definition 2(say different than definition 1)
        ("declared in savepoint level" = -1, "dropped in savepoint level"=1, definition 1(stored in table descriptor)) ,
			  ("declared in savepoint level" = 1, "dropped in savepoint level"=-1, definition 2(stored in table descriptor))
      set savepoint 2("current savepoint level = 2")
      drop temp table t1("declared in savepoint level" = -1, "dropped in savepoint level"=1, definition 1(stored in table descriptor)) ,
			  ("declared in savepoint level" = 1, "dropped in savepoint level"=2, definition 2(stored in table descriptor))
		  rollback tran
        (Remove : temp table t1("declared in savepoint level" = 1, "dropped in savepoint level"=2, definition 2(stored in table descriptor)
        (Restore : temp table t1"declared in savepoint level" = -1, "dropped in savepoint level"=-1, definition 1(stored in table descriptor))
	Else if the "dataModifiedInSavepointLevel" of temp table is greate than or equal to "current savepoint level"
	 	it means that table was declared in an earlier savepoint unit / transaction and was modified in the current UOW. And hence we will delete all the
    data from it.
*/
class TempTableInfo
{

	private TableDescriptor td;
	private int declaredInSavepointLevel;
	private int droppededInSavepointLevel;
	private int dataModifiedInSavepointLevel;

	TempTableInfo(TableDescriptor td, int declaredInSavepointLevel)
	{
		this.td = td;
		this.declaredInSavepointLevel = declaredInSavepointLevel;
		this.droppededInSavepointLevel = -1;
		this.dataModifiedInSavepointLevel = -1;
	}

	/**
	 * Return the table descriptor
	 */
	TableDescriptor getTableDescriptor() {
    return td;
  }

	/**
	 * Set the table descriptor. Will be called while temporary is being restored
	 */
	void setTableDescriptor(TableDescriptor td) {
    this.td = td;
  }

	/**
	 * Matches by name and only temp tables that have not been dropped (that's when droppededInSavepointLevel will be -1)
	 */
	boolean matches(String tableName) {
    return (td.getName().equals(tableName) && droppededInSavepointLevel == -1);
  }

	/**
	 * Return the savepoint level when the table was last modified
	 */
	int getModifiedInSavepointLevel() {
    return dataModifiedInSavepointLevel;
  }

	/**
	 * Set the savepoint level when the table was last modified
	 */
	void setModifiedInSavepointLevel(int dataModifiedInSavepointLevel) {
    this.dataModifiedInSavepointLevel = dataModifiedInSavepointLevel;
  }

	/**
	 * Return the savepoint level when the table was declared
	 */
	int getDeclaredInSavepointLevel() {
    return declaredInSavepointLevel;
  }

	/**
	 * Set the savepoint level when the table was declared
	 */
	void setDeclaredInSavepointLevel(int declaredInSavepointLevel) {
    this.declaredInSavepointLevel = declaredInSavepointLevel;
  }

	/**
	 * Return the savepoint level when the table was dropped
	 */
	int getDroppedInSavepointLevel() {
    return droppededInSavepointLevel;
  }

	/**
	 * Return the savepoint level when the table was dropped
	 */
	public void setDroppedInSavepointLevel(int droppededInSavepointLevel) {
    this.droppededInSavepointLevel = droppededInSavepointLevel;
  }
}
