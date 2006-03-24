# Print out anything that creates a MessageId, this is a situation
# where message translation is likely happening
#
# Print out a label in a comment to assist further activity
/new MessageId.*);/p
# Catch situation where the MessageId parameter is on the next line
/new MessageId[^;]*$/,/);/p

