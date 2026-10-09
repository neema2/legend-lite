select "t_0".id as "id", "t_0".grp as "grp", "t_0".name as "name", "t_0".flag as "flag", sum("t_0".id order by "t_0".id desc) over (partition by "t_0".grp) as "s" from T as "t_0"
