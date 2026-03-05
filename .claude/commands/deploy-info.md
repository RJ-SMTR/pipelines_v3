Show deployment configuration for a pipeline.

Given the pipeline name (argument): $ARGUMENTS

1. Find and read the pipeline's `prefect.yaml`:
   ```
   pipelines/$ARGUMENTS/prefect.yaml
   ```

2. Display a summary:
   - Deployment names (staging/prod)
   - Schedules (cron expressions with timezone)
   - Work pool
   - Docker image
   - Secret configuration
   - Entrypoint

3. If no argument provided, list all available pipelines:
   ```bash
   ls -d pipelines/*/prefect.yaml | sed 's|pipelines/||;s|/prefect.yaml||'
   ```
