select "t_0".grp as "grp", sum("t_0".id) as "s", count("t_0".id) as "c", avg(1.0 * "t_0".id) as "a", max("t_0".id) as "mx" from T as "t_0" group by "grp"
