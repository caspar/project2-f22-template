select count(*) as ct
from {{ref('q5')}}
having ct <> 1