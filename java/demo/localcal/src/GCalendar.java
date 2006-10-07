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
import java.util.concurrent.*;
import java.net.URL;
import java.security.*;
import java.text.SimpleDateFormat;

import com.google.gdata.client.calendar.*;
import com.google.gdata.data.extensions.*;
import com.google.gdata.data.*;
import com.google.gdata.data.calendar.*;
import com.google.gdata.util.*;
import com.google.gdata.client.*;

/**
 * This class provides a simple abstraction for interacting with the
 * Google Calendar Data API.  It runs as a separate thread so that requests
 * can be posted asynchronously.
 *
 * All requests are shipped through the requestQueue.  The run method
 * reads from the request queue and ships the requests to Google Calendar.
 * Results are then fed back on the response queue.
 */
public class GCalendar {
    private static String GOOGLE_CAL_URL_PREFIX = 
            "http://www.google.com/calendar/feeds/";
    
    private static String AUTHOR_NAME="David Van Couvering";
    private static String AUTHOR_EMAIL="david.vancouvering@gmail.com";
            
    private String          calendarId;    
    private URL             feedUrl;
    private EventFeed       feed;
    private CalendarService service;
    private String          gmtOffset;
    private String          startDay;
    private String          endDay;
    
    /** 
     * Creates a new instance of GCalendarService 
     *
     * @param calendarId
     *      The special token that identifies the calendar.  
     *      @see DerbyCalendarApplet.connect() for a full description of this.
     * 
     * @param user
     *      Your Google Calendar account name, e.g. david.vancouvering@gmail.com
     *
     * @param password
     *      Your Google Calendar password
     *
     * * @param startDay
     *      the starting day for the calendar, in the format <yyyy>-<mm>-<dd>
     *
     * @param endDay
     *      the starting day for the calendar, in the format <yyyy>-<mm>-<dd>

     */
    public GCalendar(String calendarId, String gmtOffset, final String user, 
            final String password, String startDay, String endDay) 
            throws Exception {
        this.calendarId     = calendarId;
        this.gmtOffset      = gmtOffset;
        this.startDay       = startDay;
        this.endDay         = endDay;
        
        // We're accessing a read-write feed, and we want full information
        this.feedUrl = new URL(GOOGLE_CAL_URL_PREFIX + calendarId + 
                "/private/full");   

        try {
            AccessController.doPrivileged(
                new PrivilegedExceptionAction<Object>() {
                    public Object run() throws Exception {
                        service = new CalendarService("DerbyCalendar Demo");
                        service.setUserCredentials(user, password);

                        // Send the request and receive the response:
                        feed = service.getFeed(feedUrl, EventFeed.class);

                        return null;
                    }
                }
            );
        } catch (PrivilegedActionException e) {
            handleException(e.getException(), 
                    "Unable to get Google Calendar feed",
                    null);
        }
        
    }
    
    /**
     * Add an event to the Google Calendar
     *
     * @param event
     *      The event to add
     *
     * @return 
     *      The event that was added, modified as needed by what was
     *      returned from Google Calendar.  In particular, the id has
     *      been updated to be the id provided by Google Calendar.
     */
    public CalEvent addEvent(String date, String title) 
        throws Exception {                
        EventEntry newEntry = new EventEntry();

        newEntry.setTitle(new PlainTextConstruct(title));

        Person author = new Person(AUTHOR_NAME, null, AUTHOR_EMAIL);
        newEntry.getAuthors().add(author);

        DateTime startTime = DateTime.parseDateTime(date + "T00:00:00" + 
                gmtOffset);
        startTime.setDateOnly(true);
        
        // An all day event has an end time of the next day.  Need to
        // calculate that using the wonderful Java date APIs...
        String endTimeString = getNextDay(date);
        DateTime endTime = DateTime.parseDateTime(endTimeString + "" +
                "T00:00:00" + gmtOffset);
        endTime.setDateOnly(true);

        When eventTimes = new When();
        eventTimes.setStartTime(startTime);
        eventTimes.setEndTime(endTime);
        newEntry.addTime(eventTimes);
        

        // Need a 'final' version of the entry so it can be used
        // inside the privileged block. 
        final EventEntry finalEntry = newEntry;
        
        // Send the request.  The response contains the final version of the
        // entry.  In particular, we need to store the edit URL for future
        // updates
        EventEntry addedEntry = null;        
        try {
            addedEntry = AccessController.doPrivileged(
                new PrivilegedExceptionAction<EventEntry>() {
                    public EventEntry run() throws Exception {
                        return service.insert(feedUrl, finalEntry);
                    }
                }
            );
        } catch (PrivilegedActionException e) {
            handleException(e.getException(), "Unable to add event",
                    finalEntry);
        }
        
        return new CalEvent(addedEntry.getId(), date, title,
                addedEntry.getEditLink().getHref(),
                addedEntry.getVersionId());
    }
    
    /**
     * Get the next day given a day string of the format <yyyy>-<mm>-<dd>
     */
    private String getNextDay(String day) throws Exception {
        // See how simple it is to add a day to a date? :)
        // Thanks be to Google for finding this code...
        java.text.SimpleDateFormat sdf = 
            new java.text.SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse(day);
        
        Calendar cal = Calendar.getInstance(); 
        cal.setTime(date); 
        cal.add(Calendar.DATE, 1);
        return sdf.format(cal.getTime());
    }
    
    /**
     * Update an event
     *
     * @param id
     *      The id of the event to be updated
     *
     * @param title
     *      The new title for the event - that's the only thing we
     *      allow to be changed at this time -- keeping things simple.
     *
     * @return
     *  A new instance of an event, with fields updated as necessary
     *  from Google (e.g. version id has changed)
     */
    public CalEvent updateEvent(CalEvent event) throws 
            Exception {
        final EventEntry entry = createEventEntry(event);
        final URL editURL = new URL(event.getEditURL());
        
        // Send the request. 
        EventEntry updatedEntry = null;        
        try {
            updatedEntry = AccessController.doPrivileged(
                new PrivilegedExceptionAction<EventEntry>() {
                    public EventEntry run() throws Exception {
                        return service.update(editURL, entry);
                    }
                }
            );
        } catch (PrivilegedActionException e) {
            handleException(e.getException(), "Unable to update event",
                    entry);
        }     
        
        return createDerbyCalEvent(updatedEntry);
    }
    
    /**
     * Delete an event
     *
     * @param editURLString
     *      The string representation of the edit URL
     */
    public void deleteEvent(final String editURLString) throws Exception {
        final URL editURL = new URL(editURLString);
        
        // Send the request. ry;        
        try {
            AccessController.doPrivileged(
                new PrivilegedExceptionAction<Object>() {
                    public Object run() throws Exception {
                        service.delete(editURL);
                        return null;
                    }
                }
            );
        } catch (PrivilegedActionException e) {
            handleException(e.getException(),
                "Unable to delete event with edit URL " + editURLString, null);
        }        
    }

    /**
     * Get the list of events.  You would want to
     * use this, for example, if you are trying to get the calendar when
     * the application first starts up.
     *
     * @return a HashMap of events for the calendar, where the hash id
     *      is the id of the event
     */
    public Collection<CalEvent> getEvents() throws Exception {
        
        CalendarEventFeed resultFeed = null;

        // Do this in a privileged block, as it connects to the Internet
        try {
            resultFeed = AccessController.doPrivileged(
                new PrivilegedExceptionAction<CalendarEventFeed>() {
                    public CalendarEventFeed run() throws Exception {
                        CalendarQuery myQuery = new CalendarQuery(feedUrl);

                        myQuery.setMinimumStartTime(DateTime.parseDateTime(
                                startDay + "T00:00:00" + gmtOffset));
                        myQuery.setMaximumStartTime(DateTime.parseDateTime(
                                endDay + "T23:59:59" + gmtOffset));

                        // Send the request and receive the response:
                        return service.query(myQuery, CalendarEventFeed.class);
                    }
                }
            );
        } catch (PrivilegedActionException e) {
            handleException(e.getException(), "Unable to get entries from " +
                    "Google Calendar", null);
        }
        
        // Build a list of DerbyCal events based on these entries
        ArrayList<CalEvent> calentries = new ArrayList<CalEvent>();

        // debugging
        // System.out.println("Entries from Google Calendar:");
                
        for ( EventEntry entry : resultFeed.getEntries() ) {
            calentries.add(createDerbyCalEvent(entry));            
        }

        return calentries;
    }
    
    public void clearCalendar() throws Exception {
        // Get the latest list of entries
        Collection<CalEvent> events = getEvents();
        
        // Delete the entries one at a time
        for ( CalEvent event : events ) {
            deleteEvent(event.getEditURL());
        }
    }
    
    /**
     * Create a Google EventEntry from an event object
     */
    private EventEntry createEventEntry(CalEvent event) throws Exception {
        EventEntry entry = new EventEntry();
        entry.setTitle(new PlainTextConstruct(event.getTitle()));
        entry.setId(event.getId());

        Person author = new Person(AUTHOR_NAME, null, AUTHOR_EMAIL);
        entry.getAuthors().add(author);

        DateTime startTime = DateTime.parseDateTime(event.getDate() + "T00:00:00" + 
                gmtOffset);
        startTime.setDateOnly(true);
        
        // An all day event has an end time of the next day.  Need to
        // calculate that using the wonderful Java date APIs...
        String endTimeString = getNextDay(event.getDate());
        DateTime endTime = DateTime.parseDateTime(endTimeString + "" +
                "T00:00:00" + gmtOffset);
        endTime.setDateOnly(true);

        When eventTimes = new When();
        eventTimes.setStartTime(startTime);
        eventTimes.setEndTime(endTime);
        entry.addTime(eventTimes);
        
        return entry;
    }
    
    /**
     * Create a DerbyCalEvent from an EventEntry
     */
    private CalEvent createDerbyCalEvent(EventEntry entry) throws Exception {
        String title = entry.getTitle().getPlainText();

        // Simplifying assumption: all entries are day-long events.
        // No more, no less
        List<When> times = entry.getTimes();
        String start = times.get(0).getStartTime().toString();

        String editURL = entry.getEditLink().getHref();

        CalEvent event = new CalEvent(entry.getId(), start, title, 
            editURL, entry.getVersionId());
        
        return event;
    }
    
    /**
     * Handle an exception received when invoking a method on Google Calendar
     *
     * @param e
     *      The exception that was encountered.  Obtain this by calling
     *      getException() on the PrivilegedActionException
     *
     * @param message
     *      This is the message specific to the invocation that got the exception,
     *      such as "unable to update event".  This is prepended upon the details
     *      provided by this method
     *
     * @param entry
     *      The event entry associated with this message, if available.  Otherwise
     *      set this to null.
     *
     * @throws Exception
     *      When the exception encountered should result in an exception being thrown
     *      to the caller.  This is almost always the case.
     */
    private void handleException(Exception e, String message, EventEntry entry) 
        throws Exception {

        String entryInfo = "";
        String id;
        String title;
        String date;
        if ( entry != null ) {
            id = parseId(entry.getId());
            title = entry.getTitle().getPlainText();
            List<When> times = entry.getTimes();
            date = times.get(0).getStartTime().toString();
                        
            entryInfo = date + ": " + title;
        }
        if ( e instanceof java.net.NoRouteToHostException ) {
            throw new NetworkDownException(e);
        }
        if ( e instanceof ResourceNotFoundException ) {
            message += ": the event " + entryInfo + " does not appear " +
                    "to exist.  Perhaps somebody else deleted it?";
        }
        else if ( e instanceof ServiceException ) {
            ServiceException se = (ServiceException)e;

            // Let's see if this is a conflict where we are trying to
            // update an entry that has been modified.  The error code
            // is 400, which is not helpful, it's supposed to be 403,
            // "conflict", but oh well...
            String responseBody = se.getResponseBody();
            
            if ( responseBody != null && 
                    responseBody.contains("Sequence ids from event") &&
                    responseBody.contains("do not match")) {
                message += ": unable to update event " +
                        entryInfo + ", it appears someone else has updated it." +
                        " The event will be updated to contain the latest " +
                        " version from Google Calendar.";
            }
            else {
                message += ": exception from Google Calendar.  HTTP error code " +
                        "is " + se.getHttpErrorCodeOverride() + "; response body is: " +
                        se.getResponseBody();
            }
        } else {
            message += ": please see next exception for details.";
        }
        
        throw new Exception(message, e);

     }
    
    /** 
     * Strip off the unique id of the entry from the full Google
     * Calendar id, which includes the Feed URL
     */
    private String parseId(String id) {
        if ( id == null ) {
            return null;
        }
        return id.substring(id.lastIndexOf("/") + 1, id.length());
    }
                
}
