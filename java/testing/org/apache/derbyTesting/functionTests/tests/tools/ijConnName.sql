-- assumes the connections connOne, connTwo are set up already
-- and that connThree, connFour failed to be setup correctly (bad URLs)

-- expect connOne to be active
show connections;
set connection connOne;
values 1;
set connection connTwo;
values 1;
-- connThree doesn't exist, it failed at boot time
set connection connThree;
-- connFour doesn't exist, it failed at boot time
set connection connFour;
-- connTwo is still active
show connections;
-- no such connection to disconnect
disconnect noName;
disconnect connOne;
-- connOne no longer exists
set connection connOne;

disconnect current;

-- see no more connections to use
show connections;
