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
import java.sql.*;

/**
 * Responsible for managing persistence of events to the database
 */
public class EventManager {
    public static void addEvent(CalEvent event) throws Exception {
        Connection conn = DatabaseManager.getConnection();
        try {
            PreparedStatement pstmt = conn.prepareStatement(
                "INSERT INTO " + DatabaseManager.EVENTS_TABLE + 
                "(event_id, date, title, edit_url, version_id) " +
                "VALUES(?, ?, ?, ?, ?)");
            
            pstmt.setString(1, event.getId());
            pstmt.setString(2, event.getDate());
            pstmt.setString(3, event.getTitle());
            if ( event.getEditURL() == null ) {
                pstmt.setNull(4, Types.VARCHAR);
            } else {
                pstmt.setString(4, event.getEditURL());
            }
            
            if ( event.getVersionId() == null ) {
                pstmt.setNull(5, Types.VARCHAR);
            } else {
                pstmt.setString(5, event.getVersionId());
            }       
            
            pstmt.executeUpdate();
        } finally {
            DatabaseManager.releaseConnection(conn);
        }
    }

    /**
     * Fix the event id in both the events and requests tables.  This
     * happens when we add an event and Google Calendar sends us back
     * a new id
     */
    public static void updateEventId(String oldId, String newId) 
            throws Exception {
        System.out.println("Updating event id in the database");
        Connection conn = DatabaseManager.getConnection();
        
        try {
            PreparedStatement pstmt = conn.prepareStatement(
                "UPDATE " + DatabaseManager.EVENTS_TABLE + 
                " SET event_id = ? WHERE event_id = ?");
            pstmt.setString(1, oldId);
            pstmt.setString(2, newId);
            pstmt.execute();
            
            pstmt = conn.prepareStatement(
                "UPDATE " + DatabaseManager.REQUESTS_TABLE + 
                " SET event_id = ? WHERE event_id = ?");
            pstmt.setString(1, oldId);
            pstmt.setString(2, newId);
            pstmt.execute();
        } finally {
            DatabaseManager.releaseConnection(conn);            
        }
    }

    
    public static void deleteEvent(String eventId) throws Exception {
        Connection conn = DatabaseManager.getConnection();
        try {
            PreparedStatement pstmt = conn.prepareStatement(
                "DELETE FROM " + DatabaseManager.EVENTS_TABLE + 
                " WHERE event_id = ?");
            
            pstmt.setString(1, eventId);
            pstmt.executeUpdate();
        } finally {
            DatabaseManager.releaseConnection(conn);
        }        
    }
    
    public static void updateEvent(CalEvent event) throws Exception {
        Connection conn = DatabaseManager.getConnection();
        try {
            PreparedStatement pstmt = conn.prepareStatement(
                "UPDATE " + DatabaseManager.EVENTS_TABLE + 
                " SET date = ?, title = ?, edit_url = ?, version_id = ? " +
                "WHERE event_id = ?");
            
            pstmt.setString(1, event.getDate());
            pstmt.setString(2, event.getTitle());
            if ( event.getEditURL() == null ) {
                pstmt.setNull(3, Types.VARCHAR);
            } else {
                pstmt.setString(3, event.getEditURL());
            }
            if ( event.getVersionId() == null ) {
                pstmt.setNull(4, Types.VARCHAR);
            } else {
                pstmt.setString(4, event.getVersionId());
            }
            pstmt.setString(5, event.getId());
            if ( pstmt.executeUpdate() == 0 ) {
                throw new Exception("Event not updated - couldn't find event " +
                        "with id " + event.getId());
            }
        } finally {
            DatabaseManager.releaseConnection(conn);
        }
        
    }
        
    public static Collection<CalEvent> getEvents() throws Exception {
        Connection conn = DatabaseManager.getConnection();
        ArrayList<CalEvent> events = new ArrayList<CalEvent>();
        
        try {
            ResultSet rs = DatabaseManager.executeQueryNoParams(conn, 
                    "SELECT event_id, date, title, edit_url, version_id FROM " + 
                    DatabaseManager.EVENTS_TABLE);
            
            // System.out.println("");
            // System.out.println("Getting events from local database");
            while ( rs.next() ) {
                CalEvent event = new CalEvent(
                    rs.getString(1), rs.getString(2), rs.getString(3),
                    rs.getString(4), rs.getString(5));
               
                // System.out.println(event);
                events.add(event);
            }
            // System.out.println("");
        } finally {
            DatabaseManager.releaseConnection(conn);
        }
        
        return events;
    }
    
    /**
     * Get a single event from the database
     */
    public static CalEvent getEvent(String eventId) throws Exception {
        Connection conn = DatabaseManager.getConnection();
        CalEvent event;
        
        try {
            PreparedStatement pstmt = conn.prepareStatement( 
                    "SELECT date, title, edit_url, version_id FROM " + 
                    DatabaseManager.EVENTS_TABLE +
                    " WHERE event_id = ?");
            pstmt.setString(1, eventId);
            ResultSet rs = pstmt.executeQuery();
            
            if ( ! rs.next() )  {
                return null;
            }
            
            event = new CalEvent(
                eventId, rs.getString(1), rs.getString(2),
                rs.getString(3), rs.getString(4));
        } finally {
            DatabaseManager.releaseConnection(conn);
        }
        
        return event;
    }

    /**
     * Refresh the database with a list of events from the Google
     * Calendar.  Google Calendar is The Truth and the local store
     * must submit...
     *
     * Note we also clear out any pending requests, as they are no
     * longer valid once we've refreshed.  
     */
    public static void refresh(Collection<CalEvent> events) 
        throws Exception {
        System.out.println("Refreshing local store with list of events " +
                "from Google Calendar...");
        
        DatabaseManager.clearTables();
        
        for ( CalEvent event : events ) {
            addEvent(event);
        }
    }
}

