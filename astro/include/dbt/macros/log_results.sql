{% macro log_results() %}
  {% set query %}
    SELECT 
      "🎯 ANSWER: Launches Needed" as launches_needed,
      "🎯 ANSWER: Months to 42K" as months_needed,
      "🎯 ANSWER: Est. Completion Date" as completion_date,
      "CONFIDENCE: Projection Reliability" as confidence
    FROM gold.starlink_42k_projection
    LIMIT 1
  {% endset %}

  {% set results = run_query(query) %}
  
  {% if results %}
    {% for row in results %}
      {{ log("🚀 BUSINESS QUESTION RESULTS:", info=true) }}
      {{ log("   Additional launches needed: " ~ row[0], info=true) }}
      {{ log("   Months to 42,000 satellites: " ~ row[1], info=true) }}
      {{ log("   Estimated completion date: " ~ row[2], info=true) }}
      {{ log("   Projection confidence: " ~ row[3], info=true) }}
    {% endfor %}
  {% else %}
    {{ log("❌ No results found in starlink_42k_projection table", info=true) }}
  {% endif %}
{% endmacro %}