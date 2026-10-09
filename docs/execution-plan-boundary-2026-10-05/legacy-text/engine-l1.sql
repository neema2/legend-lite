select "t_0".grp as "grp", listagg("t_0".name, ',') within group (order by "t_0".id asc) as "names" from T as "t_0" group by "grp"
