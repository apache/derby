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
import com.sun.media.sound.JavaSoundAudioClip;
import java.applet.*;
import java.util.*;
import java.util.concurrent.*;
import java.awt.*;
import java.io.*;
import java.security.*;


import org.json.*;

/**
 * This is the controller for the local calendar application
 */
public class CalendarController extends Applet {
    public static final String DBNAME = "LocalCalDB";
    private String calid;
    private String gmtOffset;
    private String user;
    private String password;
    private String startDay;
    private String endDay;
    
    private static PrintStream console;
    private static final String CONSOLE_FILENAME = "localcal.log";
    
    /** The calendar we're logged in to */
    GCalendar   calendar;
    
    /** Indicates whether we're online or not */
    boolean     online = true;
        
    /**
     * Log in to the Google Calendar service.  
     *
     * @param calid
     *      The calendar id.  If this is your default Google Calendar,
     *      it's the email address you use to login to Google Calendar,
     *      e.g. "david.vancouvering@gmail.com".
     *      <p>
     *      If it's not your default calendar, you can get the calendar id
     *      for the calendar you want by doing the following:
     *      <ul>
     *      <li>Go to your Google Calendar page
     *      <li>On the left pane all your calendars are listed.  Click on the
     *          drop-down menu for the calendar you want, and choose 
     *          "Calendar settings".
     *      <li>Under "Calendar Address" click on the [XML] button.  A window
     *          will pop up that will give you a URL of the form <pre>
     *          "http://www.google.com/calendar/feeds/<token>@group.calendar.google.com/public/basic"
     *          </pre>
     *      <li>Your calendar id is "<token>@group.calendar.google.com"
     *      </ul>
     *
     *  @param gmtOffset
     *      The offset, positive or negative, from GMT for the time zone for
     *      the calendar
     *
     * @param user
     *      your username for your Google Calendar account, e.g.
     *      david.vancouvering@gmail.com
     *
     * @param password
     *      your password for your Google Calendar account
     *
     * @param startDay
     *      The starting day for this calendar in the format
     *      <yyyy>-<mm>-<dd>
     *
     * @param endDay
     *      The ending day inclusive for this calendar, in the format
     *      <yyyy>-<mm>-<dd>
     *
     * @return a JSON Array containing the list of events from Google
     */
    public void login(String calid, String gmtOffset, 
            String user, String password, String startDay,
            String endDay, boolean drop) throws Exception {
        // Create the calendar, and if the login succeeds, start up the thread
        log("DerbyCalendarApplet.login(" + calid + ", " + gmtOffset + ", " +
                user + ", " + startDay + ", " + endDay + ")");
        
        this.calid      = calid;
        this.gmtOffset  = gmtOffset;
        this.user       = user;
        this.password   = password;
        this.startDay   = startDay;
        this.endDay     = endDay;
                        
        goOnline();
    }
    
    private void startConsole(String dir) throws Exception {
        final String path = dir + "/" + CONSOLE_FILENAME;
        AccessController.doPrivileged(
            new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    log("Writing log to " + path);
                    console = new PrintStream(
                        new FileOutputStream(path));
                    System.setOut(console);
                    System.setErr(console);
                    return null;
                }
            }
        );
    }
    
    /**
     * Go online.  This method gets any pending requests and sends
     * them up to Google Calendar.
     */
    public void goOnline() throws Exception {
        log("GOING ONLINE...");
        this.online = true;

        try {
            // Log in to Google Calendar  
            calendar = new GCalendar(calid, gmtOffset, user, password, 
                    startDay, endDay);
            
            RequestManager.submitRequests(calendar);
        } catch ( Exception e ) {
            e.printStackTrace();
            throw e;
        }        
    }

    public void goOffline() {
        log("GOING OFFLINE");
        this.online = false;
    }
    
    public boolean isOnline() {
        return this.online;
    }
    
    /**
     * Refresh our calendar from Google Calendar and return a JSON string that 
     * represents the array of entries for the given date range
     *
     * @return a JSON string that represents an array of all the entries
     *      for the calendar.
     */
    public String refresh() throws Exception {
        log("DerbyCalendarApplet.refresh()");
        

        Collection<CalEvent> events = null;
        if ( isOnline() ) {
            try {
                events = calendar.getEvents();

                // Refresh the database with the events we got from Google
                // Calendar
                EventManager.refresh(events);
            } catch ( NetworkDownException nde ) {
                log("The network is down, going offline");
                goOffline();
            }
        }
        
        if ( ! isOnline() ) {
            events = EventManager.getEvents();
        }

        JSONArray jarray = new JSONArray();
        for ( CalEvent event : events ) {
            
            jarray.put(event.getJSONObject());
        }
                
        return jarray.toString();
    }        
    
    // Return a list of conflicts as a string so it can be reported
    // as an error
    public String getConflicts() {
        java.util.List<String> conflicts = RequestManager.getConflicts();
        if ( conflicts.size() == 1 ) {
            return "There was 1 error during synchronization.  Please " +
                "see the error log for details.";
        } else if ( conflicts.size() > 1 ) {
            return "There were " + conflicts.size() + " errors during " +
                "synchronization.  Please see the error log for details.";            
        } else {
            return null;
        }
    }

    /**
     * Add an entry to the calendar
     *
     * @param id 
     *      The unique identifier for this new entry
     *
     * @param date
     *      The date for this entry, in the form of <yyyy>-<mm>-<dd>
     * 
     * @param title
     *      The title for the entry
     *
     * @return the new id returned by Google Calendar
     */
    public String addEvent(String id, String date, String title) 
            throws Exception {
        log("DerbyCalendarApplet.addEntry(" + id  + 
                ", " + date + ", " + title + ")");
        
        CalEvent event = null;
        
        if ( isOnline() ) {
            try {
                event = calendar.addEvent(date, title);
            } catch ( NetworkDownException nde ) {
                log("The network is down, going offline");
                goOffline();
            }
        }
        
        // Now do the database operations -- store the event
        // locally, and if we're offline, also store the request
        // to add the event so we can ship it to Google Calendar 
        // when we come back online
        try {
            DatabaseManager.beginTransaction();
            
            if ( ! isOnline() ) {
                log("Storing request to add event");
                RequestManager.storeAddEvent(id, date, title);
                event = new CalEvent(id, date, title, null, null);
            }   
            
            log("Storing new event in the local database");            
            EventManager.addEvent(event);
            
            DatabaseManager.commitTransaction();
        } catch ( Exception e ) {
            DatabaseManager.rollbackTransaction();
            throw e;
        }

        return event.getJSONObject().toString();
    }
    
    public void updateEvent(String id, String title) throws Exception {
        log("DerbyCalendarApplet.updateEntry(" + id  + ", " + title + ")");
        CalEvent event = EventManager.getEvent(id);
        event.setTitle(title);

        if ( isOnline() ) {
            try {
                event = calendar.updateEvent(event);
            } catch ( NetworkDownException nde ) {
                log("The network is down, going offline");
                goOffline();
            }
        }

        // Now do the database operations -- store the event
        // locally, and if we're offline, also store the request
        // to add the event so we can ship it to Google Calendar 
        // when we come back online
        try {
            DatabaseManager.beginTransaction();
            
            if ( ! isOnline() ) {
                log("Storing request to update event");
                RequestManager.storeUpdateEvent(event);
            }   
            
            log("Updating event in the local database");            
            EventManager.updateEvent(event);
            
            DatabaseManager.commitTransaction();
        } catch ( Exception e ) {
            DatabaseManager.rollbackTransaction();
            throw e;
        }

    }
    
    public void deleteEvent(String id) throws Exception {
        log("DerbyCalendarApplet.deleteEntry(" + id  + ")");
        CalEvent event = EventManager.getEvent(id);
        
        if ( isOnline() ) {
            try {
                if ( event == null ) {
                    throw new Exception("Can't find even in the database: " +
                            id);
                }
                
                calendar.deleteEvent(event.getEditURL());
            } catch ( NetworkDownException nde ) {
                log("The network is down, going offline");
                goOffline();
            }
        }

        // Now do the database operations -- store the event
        // locally, and if we're offline, also store the request
        // to add the event so we can ship it to Google Calendar 
        // when we come back online
        try {
            DatabaseManager.beginTransaction();
            
            if ( ! isOnline() ) {
                log("Storing request to delete event");
                RequestManager.storeDeleteEvent(id, event.getEditURL());
            }   
            
            log("Deleting event in the local database");            
            EventManager.deleteEvent(id);
            
            DatabaseManager.commitTransaction();
        } catch ( Exception e ) {
            DatabaseManager.rollbackTransaction();
            throw e;
        }

    }
    
    /** 
     * Empty out the calendar.  Used mostly for testing 
     */
    public void clearCalendar() throws Exception {
        DatabaseManager.clearTables();
        calendar.clearCalendar();
    }
    
    private void log(String str) {
        System.out.println(str);
    }
        
    /**
     * Call this to turn on logging of SQL to derby.log,
     * for debuggig
     */
    public void logSql() throws Exception {
        DatabaseManager.logSql();
    }
    
    public void init() {
        log("Applet init, applet is " + this.hashCode());
        try {
            AccessController.doPrivileged(
                new PrivilegedExceptionAction<Object>() {
                    public Object run() throws Exception {
                        String userdir = System.getProperty("user.home");
                        String dbname = userdir + "/" + DBNAME;
                        
                        startConsole(userdir);
                        
                        DatabaseManager.initDatabase(dbname, 
                            "user", "secret", false); 
                        log("Database initialized, " +
                            "database directory is " + dbname); 
                        
                        return null;
                    }
                }
            );
        } catch ( PrivilegedActionException e ) {
            e.getException().printStackTrace();
        }         
    }
    
    public void start() {
        log("Applet start, applet is " + this.hashCode());
    }
       
    public void stop() {
        log("Applet stop, applet is " + this.hashCode());
    }
    
    public void destroy() {
        log("Applet destroy, applet is " + this.hashCode());
    }
    
    /** Still need to figure this one out...
    public void paint(Graphics g) {
        g.drawString("Repainting", 50, 25);
        g.drawString(consoleStream.toString(), 50, 35);
    }
     */
}
