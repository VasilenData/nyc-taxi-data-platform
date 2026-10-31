{% test accepted_range(model, column_name, min_value=none, max_value=none, inclusive=true) %}
-- Fails for every row outside [min_value, max_value]. Null values are ignored (use not_null for those).
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and (
    false
    {% if min_value is not none %} or {{ column_name }} {{ '<' if inclusive else '<=' }} {{ min_value }} {% endif %}
    {% if max_value is not none %} or {{ column_name }} {{ '>' if inclusive else '>=' }} {{ max_value }} {% endif %}
  )
{% endtest %}
