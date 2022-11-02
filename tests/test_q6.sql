select count(*) as ct
from {{ref('q6')}}
having ct <> 1