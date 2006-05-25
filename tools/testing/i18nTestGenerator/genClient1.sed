# Print out anything that creates a ClientMessageId, this is a situation
# where message translation is likely happening
#
# Print out a label in a comment to assist further activity
/new ClientMessageId.*);/p
# Catch situation where the ClientMessageId parameter is on the next line
/new ClientMessageId[^;]*$/,/);/p

# This is a wrapper used in net/Reply.java that we need to test
/zThrowSyntaxError[^;]*$/,/);/p

