select "t_0".grp as "grp", sum("t_0".id order by "t_0".id desc) as "s" from T as "t_0" group by "grp"
