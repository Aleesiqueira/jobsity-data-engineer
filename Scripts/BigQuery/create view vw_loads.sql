SELECT * EXCEPT(row_id)
FROM (SELECT *
        , row_number() over(partition by id_load order by dtinsert desc) row_id
      FROM `jobsity-317503.log.loads`)
WHERE row_id = 1