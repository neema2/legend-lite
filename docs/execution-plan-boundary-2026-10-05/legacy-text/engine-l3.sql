select "t_0".id as "id", "t_0".grp as "grp", "t_0".name as "name", "t_0".flag as "flag", sum("t_0".id) over (partition by "t_0".grp order by "t_0".id desc) as "s" from T as "t_0"
