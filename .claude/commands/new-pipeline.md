Create a new pipeline using the cookiecutter template.

1. Ask the user for the pipeline type (capture or treatment) and name
2. Run the cookiecutter command:
   ```bash
   cd /home/botelho/prefeitura_rio/pipelines_v3
   uv run --package cookiecutter templates --output-dir=pipelines
   ```
3. After generation, review the created files and adjust based on user requirements
4. Remind the user to:
   - Edit `constants.py` with their data sources
   - Configure schedules in `prefect.yaml`
   - Run `uv sync --all-packages` to register the new package
