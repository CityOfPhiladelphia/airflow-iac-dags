#
# Most Airflow tasks are short lived, and can easily be retried on a failure.
# It is important to remember that Karpenter + Spot instances means that
# a job can fail anytime due to a node going away.
# Any job that can't be easily restarted or runs for a long time
# can set up special parameters
#
from airflow import DAG
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s
from datetime import datetime
from pytz import timezone

executor_config_do_not_disrupt = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            # Karpenter will ensure that this pod goes on an on-demand node
            # so it can't be shut down by spot interrupts
            node_selector={"karpenter.sh/capacity-type": "on-demand"}
        ),
        metadata=k8s.V1ObjectMeta(
            annotations={
                # Karpenter will not delete the node that this pod is on just for
                # cost optimization reasons
                "karpenter.sh/do-not-disrupt": "true"
            }
        ),
    )
}

default_args = {
    "owner": "airflow",
    "start_date": datetime.now(timezone("US/Eastern")),
}

# Create the DAG
with DAG(
    # Name of the DAG, must be globally unique
    dag_id="do_not_disrupt_test",
    default_args=default_args,
    # Cron schedule, this runs every 15 minutes. Set to 'None' for manual only
    schedule=None,
    catchup=False,
    # Tags are useful for filtering
    tags=["example"],
) as dag:
    sleep_task = BashOperator(
        task_id="sleep",
        bash_command="sleep 60",
        executor_config=executor_config_do_not_disrupt,
    )
