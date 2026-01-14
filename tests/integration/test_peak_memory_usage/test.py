import re
import tempfile

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.uclient import client, prompt

cluster = ClickHouseCluster(__file__)

shard_1 = cluster.add_instance(
    "shard_1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={
        "shard": "shard_1",
    },
)
shard_2 = cluster.add_instance(
    "shard_2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={
        "shard": "shard_2",
    },
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        shard_1.query(
            "CREATE TABLE fixed_numbers ON CLUSTER 'cluster' ("
            "number UInt64"
            ") ENGINE=MergeTree()"
            "ORDER BY number"
        )

        shard_1.query(
            "CREATE TABLE fixed_numbers_2 ON CLUSTER 'cluster' ("
            "number UInt64"
            ") ENGINE=Memory ()"
        )

        shard_1.query(
            "CREATE TABLE distributed_fixed_numbers (number UInt64) ENGINE=Distributed('cluster', 'default', 'fixed_numbers')"
        )
        shard_1.query("INSERT INTO fixed_numbers SELECT number FROM numbers(0, 100)")

        shard_2.query("INSERT INTO fixed_numbers SELECT number FROM numbers(100, 200)")

        shard_1.query("INSERT INTO fixed_numbers_2 SELECT number FROM numbers(0, 10)")

        shard_2.query(
            "INSERT INTO fixed_numbers_2 SELECT number FROM numbers(0, 120000)"
        )

        yield cluster
    finally:
        cluster.shutdown()


def get_memory_usage_from_client_output_and_close(client_output):
    client_output.seek(0)
    peek_memory_usage_str_found = False
    query_id = ""
    peak_memory_usage = ""
    
    for line in client_output:
        print(f"'{line}'\n")
        
        # Extract query ID
        if not query_id and "Query id:" in line:
            query_id_match = re.search(r"Query id:\s*([a-f0-9\-]+)", line)
            if query_id_match:
                query_id = query_id_match.group(1)
                print(f"query_id {query_id}")
        
        if not peek_memory_usage_str_found:
            # Can be both Peak/peak
            peek_memory_usage_str_found = "eak memory usage" in line

        if peek_memory_usage_str_found and not peak_memory_usage:
            search_obj = re.search(r"[+-]?[0-9]+\.[0-9]+", line)
            if search_obj:
                peak_memory_usage = search_obj.group()
                print(f"peak_memory_usage {peak_memory_usage}")

    client_output.close()
    
    if not peak_memory_usage:
        print(f"peak_memory_usage not found")
    if not query_id:
        print(f"query_id not found")
    
    return query_id, peak_memory_usage


def test_clickhouse_client_max_peak_memory_usage_distributed(started_cluster):
    client_output = tempfile.TemporaryFile(mode="w+t")
    command_text = (
        f"{started_cluster.get_client_cmd()} --host {shard_1.ip_address} --port 9000"
    )
    with client(name="client1>", log=client_output, command=command_text) as client1:
        client1.expect(prompt)
        client1.send(
            "SELECT COUNT(*) FROM distributed_fixed_numbers JOIN fixed_numbers_2 ON distributed_fixed_numbers.number=fixed_numbers_2.number SETTINGS query_plan_join_swap_table = 'false', join_algorithm='hash'",
        )
        client1.expect("Peak memory usage", timeout=60)
        client1.expect(prompt)

    query_id, peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert query_id
    assert peak_memory_usage
    
    # Find the actual query_id on shard_2 by searching for initial_query_id
    shard_2_log = shard_2.grep_in_log(f"initial_query_id: {query_id}")
    print(f"shard_2_log: {shard_2_log}")
    assert shard_2_log, f"Could not find initial_query_id {query_id} in shard_2 logs"
    
    # Extract the actual query_id from curly braces at the beginning of the log line
    actual_query_id_match = re.search(r"\{([a-f0-9\-]+)\}", shard_2_log)
    print(f"actual_query_id_match: {actual_query_id_match}")
    assert actual_query_id_match, f"Could not extract query_id from log line: {shard_2_log}"
    actual_query_id = actual_query_id_match.group(1)
    print(f"actual_query_id: {actual_query_id}")
    
    assert shard_2.contains_in_log(f"{{{actual_query_id}}} <Debug> MemoryTracker: Query peak memory usage {peak_memory_usage}")


def test_clickhouse_client_max_peak_memory_single_node(started_cluster):
    client_output = tempfile.TemporaryFile(mode="w+t")

    command_text = (
        f"{started_cluster.get_client_cmd()} --host {shard_1.ip_address} --port 9000"
    )
    with client(name="client1>", log=client_output, command=command_text) as client1:
        client1.expect(prompt)
        client1.send(
            "SELECT COUNT(*) FROM (SELECT number FROM numbers(1,300000) INTERSECT SELECT number FROM numbers(10000,1200000))"
        )
        client1.expect("Peak memory usage", timeout=60)
        client1.expect(prompt)

    _, peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert peak_memory_usage
    assert shard_1.contains_in_log(f"Query peak memory usage: {peak_memory_usage}")

    