Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test

### Troubleshooting Deadlocks

If you encounter PostgreSQL deadlock errors, try:

1. **Use the safe run script** (recommended):
   ```bash
   ./scripts/dbt_run_safe.sh "run"
   ```
   This script:
   - Runs dbt with 2 threads by default to reduce deadlock risk
   - Automatically retries up to 3 times if a deadlock is detected
   - Waits 2 seconds between retries to allow locks to clear

2. **Reduce concurrency manually**:
   ```bash
   dbt run --threads 2
   ```

3. **Run models sequentially** (if deadlocks persist):
   ```bash
   dbt run --threads 1
   ```

4. **Run specific model** (to isolate the issue):
   ```bash
   dbt run --select stg_order_items
   ```

5. **Check for blocking queries** in PostgreSQL:
   ```sql
   SELECT * FROM pg_stat_activity WHERE wait_event_type = 'Lock';
   ```

6. **Kill blocking processes** if needed:
   ```sql
   SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event_type = 'Lock';
   ```

**Note:** Deadlocks are common when multiple models try to create/update tables simultaneously. Using fewer threads (2 or 1) significantly reduces this risk.

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
