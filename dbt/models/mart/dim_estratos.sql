-- Dimensión de estratos de tamaño empresarial (6 registros).
select id_estrato, descripcion from {{ ref("stg_ce__estratos") }} order by id_estrato
