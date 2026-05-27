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
async def delete_old_flow_runs(days_to_keep: int = 25, batch_size: int = 100):
    """Delete completed flow runs older than specified days."""
    # logger = get_run_logger()
    print(f'Running with batch size: {batch_size} and days to keep: {days_to_keep}')
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
async def vacuum_index_bloat(db_url: str, bloat_threshold: float = 30.0):
    """
    Verifica o nível de inchaço (bloat) dos índices e roda REINDEX
    apenas naqueles que ultrapassarem o limite configurado (padrão 30%).
    """
    print("Verificando índices com mais de 30% de bloat...")

    # query = """
    # SELECT
    #     schemaname,
    #     relname AS tablename,
    #     indexrelname AS indexname,
    #     pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    #     idx_scan AS index_scans,
    #     idx_tup_read AS tuples_read,
    #     idx_tup_fetch AS tuples_fetched
    # FROM pg_stat_user_indexes
    # WHERE schemaname = 'public'
    # ORDER BY pg_relation_size(indexrelid) DESC
    # LIMIT 20;"""
    # print("Verificando índices com maior bloat...")
    # conn = await asyncpg.connect(getenv("PREFECT_DB_URL"))
    # try:
    #     index_stats = await conn.fetch(query)
    #     for row in index_stats:
    #         print(f"Índice '{row['indexname']}' na tabela '{row['tablename']}' tem tamanho {row['index_size']} e {row['index_scans']} scans.")
    # except Exception as e:
    #     print(f"Erro ao verificar índices: {e}")
    # finally:
    #     await conn.close()

    # Query adaptada da documentação oficial do Prefect para calcular o Bloat dos índices
    bloat_query = """
    SELECT
        indexrelname AS indexname,
        si.relname AS tablename,
        CASE WHEN idx_scan > 0
            THEN round(100.0 * idx_tup_read / idx_scan, 2)
            ELSE 0
        END AS bloat_percent
    FROM pg_stat_user_indexes si
    JOIN pg_index i ON i.indexrelid = si.indexrelid
    JOIN pg_class c ON c.oid = i.indrelid
    WHERE schemaname = 'public'
    AND idx_scan > 10
    """
    conn = await asyncpg.connect(db_url)
    try:
        indexes_stats = await conn.fetch(bloat_query)

        indexes_to_reindex = []
        for row in indexes_stats:
            index_name = row["indexname"]
            table_name = row["tablename"]
            bloat_pct = float(row["bloat_percent"])
            print(f"Índice '{index_name}' da tabela '{table_name}' tem {bloat_pct}% de bloat.")
            if bloat_pct > bloat_threshold:
                indexes_to_reindex.append((index_name, table_name, bloat_pct))

        if not indexes_to_reindex:
            print("Nenhum índice ultrapassou o limite de inchaço. Nenhuma ação necessária.")
            return

        for index_name, _table_name, bloat_pct in indexes_to_reindex:
            print(f"Executando REINDEX no índice '{index_name}' (Bloat: {bloat_pct}%)...")
            await conn.execute(f"REINDEX INDEX CONCURRENTLY {index_name};")

        print("Manutenção dos índices concluída com sucesso!")
    except Exception as e:
        print(f"Erro durante a verificação/manutenção dos índices: {e}")
    finally:
        await conn.close()


@task(log_prints=True)
async def vacuum_tables(db_url: str, bloat_threshold: float = 30.0):
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
