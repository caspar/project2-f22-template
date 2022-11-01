select count(*) as ct
from {{ref('q2')}}
having ct <> 5