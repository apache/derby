/*

   Derby - Class org.apache.derby.impl.sql.execute.xplain.XPLAINUtil

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute.xplain;

import java.util.Properties;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.impl.sql.catalog.XPLAINScanPropsDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINSortPropsDescriptor;
/**
 * This class contains helper methods, which support the System Table Visitor. 
 *
 */
public class XPLAINUtil {
     
     /** isolation level codes */
     public static final String ISOLATION_READ_UNCOMMITED     =   "RU"; // 0
     public static final String ISOLATION_READ_COMMIT         =   "RC"; // 1
     public static final String ISOLATION_REPEAT_READ         =   "RR"; // 2
     public static final String ISOLATION_SERIALIZABLE        =   "SE"; // 3
     
     
     /** lock  modes */
     public static final String LOCK_MODE_EXCLUSIVE              =   "EX";
     public static final String LOCK_MODE_INSTANTENOUS_EXCLUSIVE =   "IX";

     public static final String LOCK_MODE_SHARE                  =   "SH";
     public static final String LOCK_MODE_INSTANTENOUS_SHARE     =   "IS";

     /** lock granularity */
     public static final String LOCK_GRANULARITY_TABLE           =   "T";
     public static final String LOCK_GRANULARITY_ROW             =   "R";
     
     /** the rs operator codes */
     // scan operations
     // ---------------
     public static final String OP_TABLESCAN                  =   "TABLESCAN";
     public static final String OP_INDEXSCAN                  =   "INDEXSCAN";
     public static final String OP_HASHSCAN                   =   "HASHSCAN";
     public static final String OP_DISTINCTSCAN               =   "DISTINCTSCAN";
     public static final String OP_LASTINDEXKEYSCAN           =   "LASTINDEXKEYSCAN";
     public static final String OP_HASHTABLE                  =   "HASHTABLE";
     public static final String OP_ROWIDSCAN                  =   "ROWIDSCAN";
     public static final String OP_CONSTRAINTSCAN             =   "CONSTRAINTSCAN";
     
     // join operations
     // ---------------
     public static final String OP_JOIN_NL                    =   "NLJOIN";
     public static final String OP_JOIN_HASH                  =   "HASHJOIN";
     public static final String OP_JOIN_NL_LO                 =   "LONLJOIN";
     public static final String OP_JOIN_HASH_LO               =   "LOHASHJOIN";
     public static final String OP_UNION                      =   "UNION";
     public static final String OP_SET                        =   "SET";
     
     // set operation details
     public static final String OP_SET_INTERSECT              =   "EXCEPT";
     public static final String OP_SET_EXCEPT                 =   "INTERSECT";
     
     // dml write operations
     // --------------------
     // basic operations
     public static final String OP_INSERT                     =   "INSERT";
     public static final String OP_UPDATE                     =   "UPDATE";
     public static final String OP_DELETE                     =   "DELETE";
     
     // specialized op_details
     public static final String OP_CASCADE                    =   "CASCADE";
     public static final String OP_VTI                        =   "VTI";
     public static final String OP_BULK                       =   "BULK";
     public static final String OP_DISTINCT                   =   "DISTINCT";
     
     // other operations
     // ----------------
     public static final String OP_NORMALIZE                  =   "NORMALIZE";
     public static final String OP_ANY                        =   "ANY";     
     public static final String OP_SCROLL                     =   "SCROLL";     
     public static final String OP_MATERIALIZE                =   "MATERIALIZE";     
     public static final String OP_ONCE                       =   "ONCE";     
     public static final String OP_VTI_RS                     =   "VTI";     
     public static final String OP_ROW                        =   "ROW";     
     public static final String OP_PROJECT                    =   "PROJECTION";     
     public static final String OP_FILTER                     =   "FILTER";     
     public static final String OP_AGGREGATE                  =   "AGGREGATION";     
     public static final String OP_PROJ_RESTRICT              =   "PROJECT-FILTER";
     // sort operations
     // ----------------
     public static final String OP_SORT                       =   "SORT";     
     public static final String OP_GROUP                      =   "GROUPBY";     
     public static final String OP_CURRENT_OF                 =   "CURRENT-OF";
     public static final String OP_ROW_COUNT                  =   "ROW-COUNT";
     public static final String OP_WINDOW                     =   "WINDOW";
     
     /** the scan info codes */
     public static final String SCAN_HEAP                     =   "HEAP";
     public static final String SCAN_BTREE                    =   "BTREE";
     public static final String SCAN_SORT                     =   "SORT";
     public static final String SCAN_BITSET_ALL               =   "ALL";
     
     /** the different statement type constants */
     public static final String SELECT_STMT_TYPE             = "S";
     public static final String SELECT_APPROXIMATE_STMT_TYPE = "SA";
     public static final String INSERT_STMT_TYPE             = "I";
     public static final String UPDATE_STMT_TYPE             = "U";
     public static final String DELETE_STMT_TYPE             = "D";
     public static final String CALL_STMT_TYPE               = "C";
     public static final String DDL_STMT_TYPE                = "DDL";
     
     /** the explain type constants */
     public static final String XPLAIN_ONLY                  = "O";
     public static final String XPLAIN_FULL                  = "F";
     
     /** sort info properties */ 
     public static final String SORT_EXTERNAL                = "EX";
     public static final String SORT_INTERNAL                = "IN";
     
     /** yes no codes */ 
     public static final String YES_CODE                     = "Y";
     public static final String NO_CODE                      = "N";
     
     // ---------------------------------------------
     // utility functions 
     // ---------------------------------------------
     
     public static String getYesNoCharFromBoolean(boolean test){
         if(test){
             return YES_CODE;
         } else {
             return NO_CODE;
         }
     }
     
     public static String getHashKeyColumnNumberString(int[] hashKeyColumns){
        // original derby encoding
        String hashKeyColumnString;
        if (hashKeyColumns.length == 1)
        {
            hashKeyColumnString = MessageService.getTextMessage(
                                                        SQLState.RTS_HASH_KEY) +
                                    " " + hashKeyColumns[0];
        }
        else
        {
            hashKeyColumnString = MessageService.getTextMessage(
                                                    SQLState.RTS_HASH_KEYS) +
                                    " (" + hashKeyColumns[0];
            for (int index = 1; index < hashKeyColumns.length; index++)
            {
                hashKeyColumnString = hashKeyColumnString + "," + hashKeyColumns[index];
            }
            hashKeyColumnString = hashKeyColumnString + ")";
        }
         return hashKeyColumnString;
     }
     
     
     /** util function, to resolve the lock mode, and return a lock mode code */
     public static String getLockModeCode(String lockString){
         lockString = lockString.toUpperCase();
         if(lockString.startsWith("EXCLUSIVE")){
             return LOCK_MODE_EXCLUSIVE;
         } else
         if(lockString.startsWith("SHARE")){
             return LOCK_MODE_SHARE;
         } else
         if(lockString.startsWith("INSTANTANEOUS")){
             int start = "INSTANTANEOUS".length();
             int length = lockString.length();
             String sub = lockString.substring(start+1, length);
             if (sub.startsWith("EXCLUSIVE")){
                 return LOCK_MODE_INSTANTENOUS_EXCLUSIVE;
             } else 
             if (sub.startsWith("SHARE")){
                 return LOCK_MODE_INSTANTENOUS_SHARE;
             } else 
             return null;
         } else
         return null;
     }

     /** util function, to resolve the isolation level and return a isolation level code */
     public static String getIsolationLevelCode(String isolationLevel){
         if(isolationLevel==null) return null;
         if(isolationLevel.equals(MessageService.getTextMessage(
                 SQLState.LANG_SERIALIZABLE))){
             return ISOLATION_SERIALIZABLE;    // 3
         } else 
         if(isolationLevel.equals(MessageService.getTextMessage(
                 SQLState.LANG_REPEATABLE_READ))){
             return ISOLATION_REPEAT_READ;     // 2
         } else 
         if(isolationLevel.equals(MessageService.getTextMessage(
                 SQLState.LANG_READ_COMMITTED))){
             return ISOLATION_READ_COMMIT;     // 1
         } else 
         if(isolationLevel.equals(MessageService.getTextMessage(
                 SQLState.LANG_READ_UNCOMMITTED))){
             return ISOLATION_READ_UNCOMMITED; // 0
         } else
         return null;
   
     }
     
     
     /** util function, to resolve the lock granularity and return a lock granularity code */
     public static String getLockGranularityCode(String lockString){
         lockString = lockString.toUpperCase();
         if(lockString.endsWith("TABLE")){
             return LOCK_GRANULARITY_TABLE;
         } else {
             return LOCK_GRANULARITY_ROW;
         }
     }
     /**
      * This method helps to figure out the statement type and returns
      * an appropriate return code, characterizing the stmt type.
      */
     public static String getStatementType(String SQLText){
         String type = "";
         String text = SQLText.toUpperCase().trim();
         if (text.startsWith("CALL")){
             type = CALL_STMT_TYPE;
         } else 
         if (text.startsWith("SELECT")){
             if (text.indexOf("~")>-1){
                 type = SELECT_APPROXIMATE_STMT_TYPE;
             } else {
                 type = SELECT_STMT_TYPE;
             }
         } else
         if (text.startsWith("DELETE")){
             type = DELETE_STMT_TYPE;
         } else
         if (text.startsWith("INSERT")){
             type = INSERT_STMT_TYPE;
         } else
         if (text.startsWith("UPDATE")){
             type = UPDATE_STMT_TYPE;
         } else
         if (text.startsWith("CREATE") ||
             text.startsWith("ALTER")  ||
             text.startsWith("DROP")     ){
             type = DDL_STMT_TYPE;
         }
         return type;
     }
     
     /** helper method which extracts the right (non-internationalzed) scan
      *  properties of the scan info properties 
      * @param descriptor the descriptor to fill with properties
      * @param scanProps the provided scan props
      * @return the filled descriptor
      */
     public static XPLAINScanPropsDescriptor extractScanProps(
                     XPLAINScanPropsDescriptor descriptor,
                     Properties scanProps){

         
         // Heap Scan Info Properties
         // extract scan type with the help of the international message service 
         String scan_type = "";
         String scan_type_property = scanProps.getProperty(
                 MessageService.getTextMessage(SQLState.STORE_RTS_SCAN_TYPE));
         if (scan_type_property!=null){
             if(scan_type_property.equalsIgnoreCase(
                 MessageService.getTextMessage(SQLState.STORE_RTS_HEAP))){
                 scan_type = SCAN_HEAP;
             } else 
             if(scan_type_property.equalsIgnoreCase(
                 MessageService.getTextMessage(SQLState.STORE_RTS_SORT))){
                 scan_type = SCAN_SORT;
             } else 
             if(scan_type_property.equalsIgnoreCase(
                 MessageService.getTextMessage(SQLState.STORE_RTS_BTREE))){
                 scan_type = SCAN_BTREE;
             }             
         } else {
             scan_type = null;
         }
         descriptor.setScan_type(scan_type);
         
         // extract the number of visited pages 
         String vp_property = scanProps.getProperty(
            MessageService.getTextMessage(SQLState.STORE_RTS_NUM_PAGES_VISITED));
         if(vp_property!=null){
             descriptor.setNo_visited_pages(new Integer(vp_property));
         }
         
         // extract the number of visited rows 
         String vr_property = scanProps.getProperty(
            MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_VISITED));
         if(vr_property!=null){
             descriptor.setNo_visited_rows(new Integer(vr_property));
         }
         
         // extract the number of qualified rows 
         String qr_property = scanProps.getProperty(
            MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_QUALIFIED));
         if(qr_property!=null){
             descriptor.setNo_qualified_rows(new Integer(qr_property));
         }
         
         // extract the number of fetched columns 
         String fc_property = scanProps.getProperty(
            MessageService.getTextMessage(SQLState.STORE_RTS_NUM_COLUMNS_FETCHED));
         if(fc_property!=null){
             descriptor.setNo_fetched_columns(new Integer(fc_property));
         }
         
         // extract the number of deleted visited rows 
         String dvr_property = scanProps.getProperty(
            MessageService.getTextMessage(SQLState.STORE_RTS_NUM_DELETED_ROWS_VISITED));
         if(dvr_property!=null){
             descriptor.setNo_visited_deleted_rows(new Integer(dvr_property));
         }
         
         // extract the btree height 
         String bth_property = scanProps.getProperty(
            MessageService.getTextMessage(SQLState.STORE_RTS_TREE_HEIGHT));
         if(bth_property!=null){
             descriptor.setBtree_height(new Integer(bth_property));
         }
         
         // extract the fetched bit set 
         String bs_property = scanProps.getProperty(
            MessageService.getTextMessage(SQLState.STORE_RTS_COLUMNS_FETCHED_BIT_SET));
         if(bs_property!=null){
             if (bs_property.equalsIgnoreCase(
                 MessageService.getTextMessage(SQLState.STORE_RTS_ALL))){
                 descriptor.setBitset_of_fetched_columns(SCAN_BITSET_ALL);
             } else {
                 descriptor.setBitset_of_fetched_columns(bs_property);
                 
             }
         }
         
         // return the filled descriptor
         return descriptor;
         
     }

     /** helper method which extracts the right (non-internationalzed) sort
      *  properties of the sort info properties object 
      * @param descriptor the descriptor to fill with properties
      * @param sortProps the provided sort props
      * @return the filled descriptor
      */
     public static XPLAINSortPropsDescriptor extractSortProps(
             XPLAINSortPropsDescriptor descriptor,
             Properties sortProps){
         
         String sort_type = null;
         String sort_type_property = sortProps.getProperty(
                 MessageService.getTextMessage(SQLState.STORE_RTS_SORT_TYPE));
         if(sort_type_property!=null){
         if(sort_type_property.equalsIgnoreCase(
             MessageService.getTextMessage(SQLState.STORE_RTS_EXTERNAL))){
             sort_type = SORT_EXTERNAL;
         } else {
             sort_type = SORT_INTERNAL;
         }}
         descriptor.setSort_type(sort_type);

         
         String ir_property = sortProps.getProperty(
                 MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_INPUT));
         if(ir_property!=null){
              descriptor.setNo_input_rows(new Integer(ir_property));
         }

          String or_property = sortProps.getProperty(
                  MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_OUTPUT));
          if(or_property!=null){
               descriptor.setNo_output_rows(new Integer(or_property));
          }
              
          if (sort_type == SORT_EXTERNAL){
              String nomr_property = sortProps.getProperty(
              MessageService.getTextMessage(SQLState.STORE_RTS_NUM_MERGE_RUNS));
              
              if(nomr_property!=null){
                 descriptor.setNo_merge_runs(new Integer(nomr_property));
              }
             
              String nomrd_property = sortProps.getProperty(
              MessageService.getTextMessage(SQLState.STORE_RTS_MERGE_RUNS_SIZE));
              
              if(nomrd_property!=null){
                  descriptor.setMerge_run_details(nomrd_property);
              }
          
         }
         
         return descriptor;
     }
     /**
       * Compute average, avoiding divide-by-zero problems.
       *
      * @param dividend the long value for the dividend (the whole next time)
      * @param divisor the long value for the divisor (the sum of all rows seen)
      * @return the quotient or null
      */
     public static Long getAVGNextTime(long dividend, long divisor){
         if(divisor==0) return null;
         if(dividend==0) return new Long(0);
         return new Long(dividend/divisor);
     }
}
