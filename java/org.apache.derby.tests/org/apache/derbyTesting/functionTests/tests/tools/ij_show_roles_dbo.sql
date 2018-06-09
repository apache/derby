-- Run via ToolScripts.java
show roles;
set role b;
show enabled_roles;
select * from table(syscs_diag.contained_roles(current_role, 0)) t order by roleid;
select * from table(syscs_diag.contained_roles(current_role)) t order by roleid;
select * from table(syscs_diag.contained_roles(current_role, 1)) t order by roleid;
select * from table(syscs_diag.contained_roles('a', 0)) t order by roleid;
select * from table(syscs_diag.contained_roles('a')) t order by roleid;
select * from table(syscs_diag.contained_roles('a', 1)) t order by roleid;
set role none;
show enabled_roles;
show settable_roles;
