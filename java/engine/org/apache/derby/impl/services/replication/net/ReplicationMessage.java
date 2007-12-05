/*
 
   Derby - Class org.apache.derby.impl.services.replication.net.ReplicationMessage
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derby.impl.services.replication.net;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This message is used for the communication between the master and the
 * slave during Replication. The message is composed of a type flag
 * and corresponding object. Each type flag indicating the type of the 
 * content bundled inside the message.
 *
 * For now the following message types are defined
 *
 * TYPE_LOG - This flag will be used for all messages will carry LogRecords.
 * TYPE_ACK - this flag is used to send a acknowledgment of successful
 *            completion of a requested operation. It will however not
 *            be used to signify reception for every message transmission
 *            since tcp would automatically take care of this.
 * TYPE_ERROR - Indicates that the requested operation was not able to be
 *              completed successfully.
 * TYPE_INITIATE - used during the intial handshake between the master and
 *                 the slave. The initial handshake helps to negotiate the
 *                 message UIDs and send a appropriate error or acknowledgment.
 */
public class ReplicationMessage implements Externalizable {
    /**
     * The version number is determined based on the database version
     * of the current database. This would help to establish if the 
     * master and the slave in replication are at compatible database
     * versions.
     */
    public static final long serialVersionUID = 1L;
    
    /**
     * This is the content of the replication message.
     */
    private Object message;
    
    /**
     * The valid values for the type of the content of the message
     * are listed below.
     */
    private int type;
    
    /**
     * This flag will be used for all messages that carry log records.
     * The Object this message type contains will be a <code>byte</code>
     * array. The content of the byte array is the log records in the
     * binary form.
     */
    public static final int TYPE_LOG = 0;
    
    /**
     * This flag is used to send an acknowledgment of successful completion
     * of a requested operation. It will however not be used to signify
     * reception for every message transmission. The object this message
     * type contains will be a <code>String</code>. The SQLState of the
     * error message can be used here.
     */
    public static final int TYPE_ACK = 1;
    
    /**
     * Indicates that the requested operation was not able to be
     * completed successfully. The object this message type contains
     * will be a <code>String</code>.
     */
    public static final int TYPE_ERROR = 2;
    
    /**
     * used during the intial handshake between the master and
     * the slave. The initial handshake helps to negotiate the
     * message UIDs and send a appropriate error or acknowledgment.
     * The object this message contains will be a <code>Long</code>.
     */
    public static final int TYPE_INITIATE = 3;
    
    /**
     * Used to send a stop replication signal to the slave. Since this
     * is a control signal the object this message contains will be null.
     */
    public static final int TYPE_STOP = 4;
    
    /**
     * Used to signal the slave that it must failover. The object associated
     * with this message will be null.
     */
    public static final int TYPE_FAILOVER = 5;
    
    /**
     * public No args constructor required with Externalizable.
     */
    public ReplicationMessage() {}
    
    /**
     * Constructor used to set the <code>type</code> and <code>message</code>.
     * This is used while creating messages for sending while readExternal is
     * used on the receiving end.
     *
     * @param type      The type of this message. Must be one of the message
     *                  type constants of this class (TYPE_LOG, TYPE_ACK,
     *                  TYPE_ERROR, TYPE_INITIATE).
     *
     * @param message   The message to be transmitted.
     *
     */
    public ReplicationMessage(int type, Object message){
        this.type = type;
        this.message = message;
    }
    
    /**
     * Used to get the actual message that is wrapped inside the
     * ReplicationMessage object.
     *
     * @return The object contained in the message
     */
    public Object getMessage(){
        return message;
    }
    
    /**
     * Used to get the type of this <code>ReplicationMessage</code>.
     *
     * @return The type of the message.
     */
    public int getType(){
        return type;
    }
    
    /**
     * Used to restore the contents of this object.
     *
     * @param in the stream to read data from in order to restore the object.
     *
     * @throws IOException If an exception occurs while reading from the
     *                     <code>InputStream</code>.
     *
     * @throws ClassNotFoundException Class of a serialized object cannot
     *                                be found.
     *
     */
    public void readExternal(ObjectInput in) throws IOException,
        ClassNotFoundException {
        switch ((int)in.readLong()) {
            case 1: {
                type = in.readInt();
                message = in.readObject();
                break;
            }
        }
    }
    
    /**
     * Used to save the contents of this Object.
     *
     * @param out the stream to write the object to.
     *
     * @throws IOException if an exception occurs while writing to the
     *                     <code>OutputStream</code>.
     *
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(serialVersionUID);
        out.writeInt(type);
        out.writeObject(message);
    }
}
