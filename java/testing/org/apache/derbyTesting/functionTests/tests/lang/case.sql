
--
-- this test is for keyword case insensitivity
--

-- Try some of the keywords with mixed case. Don't do all of the keywords, as
-- that would be overkill (either that, or I'm too lazy).

cReAtE tAbLe T (x InT);

CrEaTe TaBlE s (X iNt);

iNsErT iNtO t VaLuEs (1);

InSeRt InTo S vAlUeS (2);

sElEcT * fRoM t;

SeLeCt * FrOm s;

drop table s;

drop table t;
