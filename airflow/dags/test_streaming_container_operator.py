from datetime import datetime

import pendulum

from airflow import DAG

from operators.streaming_container_operator import (
    StreamingContainerOperator,
)


KST = pendulum.timezone(
    "Asia/Seoul"
)


# ============================================================
# Test Containers
# ============================================================

# 실제 Spark Streaming Container
#
# running / exited / paused 테스트
STREAMING_CONTAINER = (
    "spark-streaming"
)

# 존재하지 않는 Container
#
# NotFound 테스트
NOT_FOUND_CONTAINER = (
    "spark-streaming-not-exist"
)

# 의도적으로 restart loop를 발생시키는
# 테스트 전용 Container
#
# restarting 테스트
RESTARTING_CONTAINER = (
    "test-restarting"
)


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id=(
        "test_streaming_container_operator"
    ),
    start_date=datetime(
        2026,
        9,
        1,
        tzinfo=KST,
    ),
    schedule=None,
    catchup=False,
    tags=[
        "test",
        "docker",
        "custom-operator",
    ],
) as dag:

    # --------------------------------------------------------
    # A. running + start
    #    -> no-op / success
    #
    # B. exited + start
    #    -> running / success
    # --------------------------------------------------------

    test_start = (
        StreamingContainerOperator(
            task_id="test_start",
            container_name=(
                STREAMING_CONTAINER
            ),
            action="start",
            check_interval=2,
            wait_timeout=20,
            retries=0,
        )
    )


    # --------------------------------------------------------
    # C. running + stop
    #    -> exited / success
    #
    # D. exited + stop
    #    -> no-op / success
    #
    # G. paused + stop
    #    -> exited / success
    # --------------------------------------------------------

    test_stop = (
        StreamingContainerOperator(
            task_id="test_stop",
            container_name=(
                STREAMING_CONTAINER
            ),
            action="stop",
            check_interval=2,
            wait_timeout=20,
            retries=0,
        )
    )


    # --------------------------------------------------------
    # E. NotFound + start
    #    -> fail
    # --------------------------------------------------------

    test_notfound_start = (
        StreamingContainerOperator(
            task_id=(
                "test_notfound_start"
            ),
            container_name=(
                NOT_FOUND_CONTAINER
            ),
            action="start",
            retries=0,
        )
    )


    # --------------------------------------------------------
    # F. NotFound + stop
    #    -> success
    #
    # Streaming이 실행되지 않는 상태이므로
    # batch 진행 가능
    # --------------------------------------------------------

    test_notfound_stop = (
        StreamingContainerOperator(
            task_id=(
                "test_notfound_stop"
            ),
            container_name=(
                NOT_FOUND_CONTAINER
            ),
            action="stop",
            retries=0,
        )
    )


    # --------------------------------------------------------
    # H. restarting + start
    #
    # restarting 상태에서는
    # start()를 다시 호출하지 않고
    # running 상태까지 기다린다.
    #
    # 테스트용 restart loop에서는
    # 20초 후 timeout / fail 예상
    # --------------------------------------------------------

    test_restarting_start = (
        StreamingContainerOperator(
            task_id=(
                "test_restarting_start"
            ),
            container_name=(
                RESTARTING_CONTAINER
            ),
            action="start",
            check_interval=2,
            wait_timeout=20,
            retries=0,
        )
    )


    # --------------------------------------------------------
    # I. restarting + stop
    #
    # restart loop 상태에서도
    # stop()을 실행하고 exited 상태까지 확인
    # --------------------------------------------------------

    test_restarting_stop = (
        StreamingContainerOperator(
            task_id=(
                "test_restarting_stop"
            ),
            container_name=(
                RESTARTING_CONTAINER
            ),
            action="stop",
            check_interval=2,
            wait_timeout=20,
            retries=0,
        )
    )