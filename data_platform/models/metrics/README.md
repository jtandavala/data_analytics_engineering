# Metrics Usage Guide

## Querying Metrics via SQL

-- Query a metric directly
SELECT * FROM {{ metrics.calculate(
    metric('total_revenue'),
    grain='month',
    dimensions=['product_category']
) }}

-- Multiple metrics
SELECT * FROM {{ metrics.calculate(
    [metric('total_revenue'), metric('order_count')],
    grain='day',
    dimensions=['customer_state']
) }}

### Using Metrics in Python

```python
from dbt_semantic_interfaces import MetricFlow

# Initialize MetricFlow
mf = MetricFlow()

# Query metrics
result = mf.query(
    metrics=['total_revenue', 'order_count'],
    group_by=['product_category'],
    time_granularity='month'
)
```

### Available Metrics

- Revenue: total_revenue, delivered_revenue, net_revenue
- Volume: order_count, items_sold, unique_customers
- Averages: average_order_value, average_item_value
- Conversions: delivery_rate, cancellation_rate
- Derived: revenue_per_customer items_per_order
- Growth: revenue_growth


## 5. Exposures para conectar com BI

Crie exposures que usam essas métricas:

### `models/exposures.yml`
```yaml
version: 2

exposures:
  - name: revenue_dashboard
    type: dashboard
    description: "Revenue analytics dashboard using semantic metrics"
    owner:
      name: Finance Team
      email: finance@company.com
    depends_on:
      - metric: total_revenue
      - metric: delivered_revenue
      - metric: net_revenue
      - ref: fact_orders
    url: "https://looker.company.com/dashboards/revenue"
    meta:
      tool: "Looker"
      refresh_frequency: "daily"
  
  - name: sales_kpi_dashboard
    type: dashboard
    description: "Sales KPIs using order and revenue metrics"
    owner:
      name: Sales Team
    depends_on:
      - metric: order_count
      - metric: average_order_value
      - metric: revenue_per_customer
      - ref: fact_sales_daily
    url: "https://tableau.company.com/views/sales-kpis"
    meta:
      tool: "Tableau"
```


### Benefícios
- Reutilização: métricas definidas uma vez, usadas em vários lugares
- Consistência: mesma definição em toda a organização
- Governança: documentação centralizada
- Performance: otimização via semantic layer
- Integração: compatível com ferramentas BI (Looker, Tableau, Power BI)

### Comandos úteis

```python
# Validar semantic models e metrics
dbt parse

# Ver métricas disponíveis
dbt list --select metric:*

# Compilar semantic layer
dbt compile --select semantic:*
```