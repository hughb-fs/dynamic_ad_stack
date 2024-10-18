select t1.rn rn_1, t2.rn - t1.rn rn_1_2,
    --concat(t1.rn, '_', t2.rn - t1.rn) name,
  safe_divide(abs(t1.rps - t2.rps), sqrt(pow(t1.rps_std, 2) + pow(t2.rps_std, 2))) z_score

from `streamamp-qa-239417.DAS_increment.{tablename}` t1
join `streamamp-qa-239417.DAS_increment.{tablename}` t2
using (date {dims})
where t2.rn > t1.rn and t2.rn - t1.rn <= 4 and t1.rps_std > 0 and t2.rps_std > 0
order by 2, 1

