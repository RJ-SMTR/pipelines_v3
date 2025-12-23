from pathlib import Path
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from prefect import flow, task
from os import environ

from iplanrio.pipelines_utils.env import inject_bd_credentials_task


@flow(log_prints=True)
def materialization__test_dbt():
    """
    Flow to execute dbt materialization tests.
    """
    inject_bd_credentials_task()
    profiles_dir = '/opt/prefect/pipelines_v3/queries'
    project_dir = '/opt/prefect/pipelines_v3/queries'

    #set envs
    # environ['DBT_PROFILES_DIR'] = profiles_dir
    # environ['DBT_PROJECT_DIR'] = project_dir

    files = [f for f in Path(profiles_dir).glob('**/*.yml')]
    print(f"Files in profiles_dir ({profiles_dir}): {[str(f) for f in files]}")

    settings = PrefectDbtSettings(
        profiles_dir=Path(profiles_dir),
        project_dir=Path(project_dir),
    )
    runner = PrefectDbtRunner(settings=settings)
    
    run_result = runner.invoke(['compile', '--select', 'models/br_rj_riodejaneiro_brt_gps'])
    print(run_result)  # trigger cd