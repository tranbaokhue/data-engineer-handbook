import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_session_events_sink_postgres(t_env):
    table_name = 'session_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_session_events_referrer_sink_postgres(t_env):
    table_name = 'session_events_aggregated_source'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            referrer VARCHAR,
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = 'session-analysis-group',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_sessionization():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_processed_events_source_kafka(t_env)

        session_table = create_session_events_sink_postgres(t_env)
        session_referrer_table = create_session_events_referrer_sink_postgres(t_env)

        # Session aggregation by IP and host
        t_env.from_path(source_table) \
            .window(
            Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w")
        ).group_by(
            col("w"),
            col("ip"),
            col("host")
        ) \
            .select(
            col("w").start.alias("session_start"),
            col("w").end.alias("session_end"),
            col("ip"),
            col("host"),
            col("ip").count.alias("num_hits")
        ) \
            .execute_insert(session_table)

        # Session aggregation by IP, host, and referrer
        t_env.from_path(source_table).window(
            Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w")
        ).group_by(
            col("w"),
            col("ip"),
            col("host"),
            col("referrer")
        ) \
            .select(
            col("w").start.alias("session_start"),
            col("w").end.alias("session_end"),
            col("ip"),
            col("host"),
            col("referrer"),
            col("ip").count.alias("num_hits")
        ) \
            .execute_insert(session_referrer_table) \
            .wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_sessionization()