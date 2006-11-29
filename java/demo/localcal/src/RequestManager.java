/*
 
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

import java.util.*;
import java.security.*;
import java.sql.*;
import javax.sql.*;

/**
 * This class is resonsible for managing requests while offline
 */
public class RequestManager {
    
    private static ArrayList<String> conflicts;
    
    // Request types
    public static final int ADD_EVENT = 1;
    public static final int DELETE_EVENT = 2;
    public static final int UPDATE_EVENT = 3;
    
    private static final String INSERT_REQUEST_SQL =
        "INSERT INTO " + DatabaseManager.REQUESTS_TABLE +
            "(request_type, event_id, date, title, edit_url) " +
            "VALUES(?, ?, ?, ?, ?)";
        
    public static void storeAddEvent(String eventId, String date, 
        String title) throws Exception  {
        Connection conn = DatabaseManager.getConnection();
        
        try {
            // Insert the request to add an event
            PreparedStatement pstmt = conn.prepareStatement(
                INSERT_REQUEST_SQL);

            pstmt.setInt(1, ADD_EVENT);
            pstmt.setString(2, eventId);
            pstmt.setString(3, date);
            pstmt.setString(4, title);
            pstmt.setNull(5, Types.VARCHAR);
            pstmt.executeUpdate();
        } finally {
            DatabaseManager.releaseConnection(conn);
        }
    }
    
    public static void storeUpdateEvent(CalEvent event) 
            throws Exception {
        Connection conn = DatabaseManager.getConnection();
        
        try {
            // First see if this event was added while we were offline.
            // If so, just update that request.  Otherwise add it as a
            // separate request.  Trying to save round trips here, and also
            // avoids the issue around the fact that Google changes the id
            int sequenceId = findAddRequest(event.getId());
            if  ( sequenceId > 0 ) {
                System.out.println("Request to update an event that was added " +
                        "while offline; updating the add event request instead");
                updateAddRequest(sequenceId, event.getTitle());
            } else {
                // insert the request to update an event
                PreparedStatement pstmt = conn.prepareStatement(
                    INSERT_REQUEST_SQL);
                pstmt.setInt(1, UPDATE_EVENT);
                pstmt.setString(2, event.getId());
                pstmt.setNull(3, Types.VARCHAR);
                pstmt.setString(4, event.getTitle());
                pstmt.setString(5, event.getEditURL());

                pstmt.executeUpdate();
            }            
        } finally {
            DatabaseManager.releaseConnection(conn);
        }        
    }
    
    /** 
     * This updates an existing request to add an event, rather than adding
     * a new update request
     */
    private static void updateAddRequest(int sequenceId, String title) 
            throws Exception {
        Connection conn = DatabaseManager.getConnection();
        
        try {
            PreparedStatement pstmt = conn.prepareStatement(
                "UPDATE " + DatabaseManager.REQUESTS_TABLE +
                    " SET title = ? WHERE sequence_id = ?");
            pstmt.setString(1, title);
            pstmt.setInt(2, sequenceId);
            pstmt.executeUpdate();
        } finally {
            DatabaseManager.releaseConnection(conn);
        }        
    }
    
    public static void storeDeleteEvent(String eventId, String editURL) 
            throws Exception {
        Connection conn = DatabaseManager.getConnection();
        
        try {
            // First, see if this event was added while we were offline.
            // If so, just delete the add event request and we're done.
            // Otherwise log the delete event request
            int sequenceId = findAddRequest(eventId);
            if ( sequenceId > 0 ) {
                System.out.println("Request to delete an event that was " +
                        "added while offline -  deleting add event " +
                        "request instead...");
                deleteRequestsForEvent(eventId);
            } else {
                // insert the request to delete an event
                PreparedStatement pstmt = conn.prepareStatement(
                    INSERT_REQUEST_SQL);
                pstmt.setInt(1, DELETE_EVENT);
                pstmt.setString(2, eventId);
                pstmt.setNull(3, Types.VARCHAR);
                pstmt.setNull(4, Types.VARCHAR);
                pstmt.setString(5, editURL);                

                pstmt.executeUpdate();
            }
        } finally {
            DatabaseManager.releaseConnection(conn);
        }        
        
    }
    
    private static int findAddRequest(String eventId) throws Exception {
        Connection conn = DatabaseManager.getConnection();
        
        try {
            PreparedStatement pstmt = conn.prepareStatement(
                "SELECT sequence_id FROM " + DatabaseManager.REQUESTS_TABLE +
                    " WHERE request_type = ? AND event_id = ?");
            pstmt.setInt(1, ADD_EVENT);
            pstmt.setString(2, eventId);
            ResultSet rs = pstmt.executeQuery();
            if ( rs.next() ) {
                return rs.getInt(1);
            } else {
                return 0;
            }
        } finally {
            DatabaseManager.releaseConnection(conn);
        }
    }
    
    private static void deleteRequestsForEvent(String eventId) throws Exception {
        Connection conn = DatabaseManager.getConnection();
        
        try {
            PreparedStatement pstmt = conn.prepareStatement(
                "DELETE FROM " + DatabaseManager.REQUESTS_TABLE +
                    " WHERE event_id = ?");
            pstmt.setString(1, eventId);
            pstmt.executeUpdate();
        } finally {
            DatabaseManager.releaseConnection(conn);
        }
    }

    /** 
     * Submit all stored requests to Google Calendar.  This process
     * is logged to System.out.  If the network goes down during this
     * time, we stop where we were and throw a NetworkDownException.
     *
     * If there is an error communicating with the local database we throw
     * a SQLException.
     *
     * Any other exceptions are logged and we continue on until all
     * requests are processed.  Each request is deleted from the database
     * once it is processed, successfully or not.
     *
     * @return the number of failures during this process
     */
    public static int submitRequests(GCalendar calendar) throws Exception {
        List<GCalendarRequest> requests = getRequests();
        
        //
        // Go through the requests in order
        int failures = 0;
        int totalRequests = requests.size();
        
        System.out.println("");
        System.out.println("==== SUBMITTING PENDING REQUESTS TO GOOGLE CALENDAR ====");
        
        conflicts = new ArrayList<String>();
        
        for ( GCalendarRequest request : requests ) {
            try {
                request.submit(calendar);
                System.out.println(request + " submitted successfully");
                deleteRequest(request);
            } catch ( NetworkDownException nde ) {
                throw nde;
            } catch ( SQLException sqle ) {
                // this is pretty severe, we need to bail
                throw sqle;
            } catch ( Exception e ) {
                System.out.println("ERROR submitting " + request + ": " +
                   e.getMessage());
                failures++;
                deleteRequest(request);
            }
        }
        
        System.out.println("==== DONE - " + totalRequests + " requests submitted ==== ");
        System.out.println("");
                        
        return failures;
    }
    
    public static List<String> getConflicts() {
        return conflicts;
    }
    
    /**
     * Delete a request from the database
     */
    private static void deleteRequest(GCalendarRequest request) throws Exception {
        Connection conn = DatabaseManager.getConnection();
        try {
            PreparedStatement pstmt = conn.prepareStatement(
                "DELETE FROM "  + DatabaseManager.REQUESTS_TABLE + 
                " WHERE sequence_id = ?");
            
            pstmt.setInt(1, request.getId());
            pstmt.executeUpdate();
        } finally {
            DatabaseManager.releaseConnection(conn);
        }
    }
    
    private static List<GCalendarRequest> getRequests() throws Exception {
        Connection conn = DatabaseManager.getConnection();
        
        try {
            ArrayList<GCalendarRequest> requests = 
                    new ArrayList<GCalendarRequest>();
            ResultSet rs = DatabaseManager.executeQueryNoParams(conn,
                "SELECT sequence_id, request_type, event_id, date, title, " +
                    "edit_url FROM " + DatabaseManager.REQUESTS_TABLE +
                    " ORDER BY sequence_id");

            while ( rs.next() ) {
                GCalendarRequest request = createRequestFromRow(rs);
                requests.add(request);
            }

            return requests;
        } finally {
            DatabaseManager.releaseConnection(conn);
        }
    }
    
    /**
     * Get a request from a result set.  Assumes the folowing columns in
     * the result set:
     * <ul>
     * <li>sequence_id
     * <li>request type
     * <li>event id
     * <li>date
     * <li>title
     * </ul>
     */
    private static GCalendarRequest createRequestFromRow(ResultSet rs) throws Exception {
        GCalendarRequest request;
        
        // These might be null, depending on the type, but we can get them anyway
        int sequenceId = rs.getInt(1);
        int type = rs.getInt(2); 
        String eventId = rs.getString(3);
        String date = rs.getString(4);
        String title = rs.getString(5);
        String editURL = rs.getString(6);
        
        switch ( type ) {
            case ADD_EVENT:
                request = new AddEventRequest(sequenceId, eventId, date, title);
                break;
            case UPDATE_EVENT:
                request = new UpdateEventRequest(sequenceId, eventId, title);
                break;
            case DELETE_EVENT:
                request = new DeleteEventRequest(sequenceId, eventId, editURL);
                break;
            default:
                throw new Exception("Unrecognized event type " + type +
                    "for request with sequence id " + sequenceId);
        }
        
        return request;
    }    
}
