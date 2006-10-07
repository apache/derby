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

/**
 * encapsulates a request to delete an event
 */
public class DeleteEventRequest extends GCalendarRequest {
    private String eventId;
    private String editURL;
            
    /** Creates a new instance of AddEventRequest */
    protected DeleteEventRequest(int requestId, String eventId, String editURL) {
        super(requestId);
        this.setEventId(eventId);
        this.setEditURL(editURL);
    }
        
    public String toString() {
        return "Request # " + getId() + " to delete event id " + getEventId()
            + " with edit URL " + editURL;
    }
    
    public void submit(GCalendar calendar) throws Exception {
        calendar.deleteEvent(editURL);
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }    

    public String getEditURL() {
        return editURL;
    }

    public void setEditURL(String editURL) {
        this.editURL = editURL;
    }
}
