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

import java.sql.*;

/**
 * Encapsulates a request to add an event
 */
public class AddEventRequest extends GCalendarRequest {
    
    private String eventId;
    private String date;
    private String title;
            
    /** Creates a new instance of AddEventRequest */
    protected AddEventRequest(int requestId, String eventId, String date, 
            String title) {
        super(requestId);
        setEventId(eventId);
        setDate(date);
        setTitle(title);
    }
        
    public String toString() {
        return "Request # " + getId() + " to add event (" + getDate() + ": " +
                getTitle() + ")";
    }
    
    public void submit(GCalendar calendar) throws Exception {
        CalEvent event = calendar.addEvent(getDate(), getTitle());
        
        // Fix the database so that the id returned by Google Calendar
        // is used.
        EventManager.updateEventId(eventId, event.getId());
    }
    
    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
}
