import asyncio
from datetime import datetime, timedelta, timezone
from prefect import task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType, FlowRunFilterStartTime
from prefect.client.schemas.objects import StateType
from prefect.exceptions import ObjectNotFound


@task
async def delete_stale_pending_runs(
    threshold_hours: int = 1,
    batch_size: int = 200
):
    """Delete completed flow runs older than specified days."""
    # logger = get_run_logger()

    async with get_client() as client:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=threshold_hours)

        # Create filter for pending runs older than threshold
        # Note: Using start_time because created time filtering is not available
        flow_run_filter = FlowRunFilter(
            start_time=FlowRunFilterStartTime(before_=cutoff),
            state=FlowRunFilterState(
                type=FlowRunFilterStateType(
                    any_=[StateType.PENDING]
                )
            )
        )

        # Get flow runs to delete
        flow_runs = await client.read_flow_runs(
            flow_run_filter=flow_run_filter,
            limit=batch_size
        )

        deleted_total = 0

        while flow_runs:
            batch_deleted = 0
            failed_deletes = []

            # Delete each flow run through the API
            for flow_run in flow_runs:
                try:
                    await client.delete_flow_run(flow_run.id)
                    deleted_total += 1
                    batch_deleted += 1
                    print(f"Deleted pending flow run: {flow_run.name}")
                except ObjectNotFound:
                    # Already deleted (e.g., by concurrent cleanup) - treat as success
                    deleted_total += 1
                    batch_deleted += 1
                except Exception as e:
                    print(f"Failed to delete flow run {flow_run.name}: {e}")
                    failed_deletes.append(flow_run.id)

                # # Rate limiting - adjust based on your API capacity
                # if batch_deleted % 10 == 0:
                #     await asyncio.sleep(0.5)

            print(f"Deleted {batch_deleted}/{len(flow_runs)} flow runs (total: {deleted_total})")
            if failed_deletes:
                print(f"Failed to delete {len(failed_deletes)} flow runs")

            # Get next batch
            flow_runs = await client.read_flow_runs(
                flow_run_filter=flow_run_filter,
                limit=batch_size
            )

            # Delay between batches to avoid overwhelming the API
            await asyncio.sleep(0.5)

        print(f"Janitoring complete. Total deleted: {deleted_total}")