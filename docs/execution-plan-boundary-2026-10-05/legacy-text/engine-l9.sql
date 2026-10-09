select "t_0".grp as "grp", listagg("t_0".name, ',') as "names" from T as "t_0" group by "grp"
