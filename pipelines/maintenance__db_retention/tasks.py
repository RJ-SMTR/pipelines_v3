# -*- coding: utf-8 -*-
import asyncio
from datetime import datetime, timedelta, timezone

import asyncpg
from prefect import task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterStartTime,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import StateType
from prefect.exceptions import ObjectNotFound


@task(log_prints=True)
async def delete_old_flow_runs(days_to_keep: int = 25, batch_size: int = 200):
    """Delete completed flow runs older than specified days."""
    # logger = get_run_logger()

    async with get_client() as client:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_keep)

        # Create filter for old completed flow runs
        # Note: Using start_time because created time filtering is not available
        flow_run_filter = FlowRunFilter(
            start_time=FlowRunFilterStartTime(before_=cutoff),
            state=FlowRunFilterState(
                type=FlowRunFilterStateType(
                    any_=[
                        StateType.COMPLETED,
                        StateType.FAILED,
                        StateType.CANCELLED,
                        StateType.CRASHED,
                    ]
                )
            ),
        )

        # Get flow runs to delete
        flow_runs = await client.read_flow_runs(flow_run_filter=flow_run_filter, limit=batch_size)
        to_delete_dates = [run.start_time if run.start_time else run.created for run in flow_runs]
        to_delete_dates.sort()
        print(to_delete_dates)
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
                except ObjectNotFound:
                    # Already deleted (e.g., by concurrent cleanup) - treat as success
                    deleted_total += 1
                    batch_deleted += 1
                except Exception as e:
                    print(f"Failed to delete flow run {flow_run.id}: {e}")
                    failed_deletes.append(flow_run.id)

                # # Rate limiting - adjust based on your API capacity
                # if batch_deleted % 10 == 0:
                #     await asyncio.sleep(0.5)

            print(f"Deleted {batch_deleted}/{len(flow_runs)} flow runs (total: {deleted_total})")
            if failed_deletes:
                print(f"Failed to delete {len(failed_deletes)} flow runs")
                if batch_deleted == 0:
                    print("No successful deletions in this batch, stopping to avoid infinite loop.")
                    break

            # Get next batch
            flow_runs = await client.read_flow_runs(
                flow_run_filter=flow_run_filter, limit=batch_size
            )

            # Delay between batches to avoid overwhelming the API
            await asyncio.sleep(0.5)

        print(f"Retention complete. Total deleted: {deleted_total}")


@task(log_prints=True)
async def optimize_database_if_needed(db_url: str, bloat_threshold: float = 30.0):
    """
    Verifica o nível de inchaço (bloat) das tabelas e roda VACUUM ANALYZE
    apenas naquelas que ultrapassarem o limite configurado (padrão 30%).
    """
    print(f"Verificando tabelas com mais de {bloat_threshold}% de bloat...")

    # Query adaptada da documentação oficial do Prefect para calcular o Bloat
    bloat_query = """
        SELECT
            relname AS tablename,
            CASE WHEN n_live_tup > 0
                THEN round(100.0 * n_dead_tup / n_live_tup, 2)
                ELSE 0
            END AS bloat_percent
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
          AND n_dead_tup > 10
    """

    # Conecta em modo autocommit (necessário para rodar o VACUUM)
    conn = await asyncpg.connect(db_url)

    try:
        # Busca a situação de todas as tabelas
        tables_stats = await conn.fetch(bloat_query)

        tables_to_vacuum = []
        for row in tables_stats:
            table_name = row["tablename"]
            bloat_pct = float(row["bloat_percent"])
            print(f"Tabela '{table_name}' tem {bloat_pct}% de bloat.")
            if bloat_pct > bloat_threshold:
                tables_to_vacuum.append((table_name, bloat_pct))

        if not tables_to_vacuum:
            print("Nenhuma tabela ultrapassou o limite de inchaço. Nenhuma ação necessária.")
            return

        # Executa o VACUUM ANALYZE dinamicamente nas tabelas afetadas
        for table_name, bloat_pct in tables_to_vacuum:
            print(f"Executando VACUUM ANALYZE na tabela '{table_name}' (Bloat: {bloat_pct}%)...")
            # IMPORTANTE: Adicione ANALYZE para atualizar as estatísticas do Query Planner!
            await conn.execute(f"VACUUM ANALYZE {table_name};")

        print("Manutenção do banco concluída com sucesso!")

    except Exception as e:
        print(f"Erro durante a verificação/manutenção do banco: {e}")
    finally:
        await conn.close()
