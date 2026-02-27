-- Tabla de hechos de asignaciones federales (DOF).
-- Grano: año × ramo × anexo × fondo × entidad.
-- Incluye monto anual y desglose mensual.
select
    asignacion_sk,
    anio,
    cve_ent,
    ramo,
    anexo,
    fondo,
    anual,
    enero,
    febrero,
    marzo,
    abril,
    mayo,
    junio,
    julio,
    agosto,
    septiembre,
    octubre,
    noviembre,
    diciembre
from {{ ref("int_dof__asignaciones") }}
