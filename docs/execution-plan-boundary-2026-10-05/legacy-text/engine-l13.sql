select "t_0".grp as "grp", sum("t_0".id order by "t_0".id desc nulls last, "t_0".name asc) as "s" from T as "t_0" group by "grp"
