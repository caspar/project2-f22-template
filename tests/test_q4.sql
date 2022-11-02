select count(*) as ct
from {{ref('q4')}}
having ct <> 1