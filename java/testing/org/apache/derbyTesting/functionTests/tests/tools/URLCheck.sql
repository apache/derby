-- Use a valid attribute, but do not display message.
connect 'jdbc:derby:wombat;dataEncryption=true';
-- Find an unknown attribute.
connect 'jdbc:derby:wombat;unknown=x';
-- Check for duplicate.
connect 'jdbc:derby:wombat;dataEncryption=true;dataEncryption=false';
-- Perform case check.
connect 'jdbc:derby:wombat;dataencryption=true';
-- Check for true/false.
connect 'jdbc:derby:wombat;dataEncryption=x';
