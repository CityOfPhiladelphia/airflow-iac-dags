#
# Most Airflow tasks are short lived, and can easily be retried on a failure.
# It is important to remember that Karpenter + Spot instances means that
# a job can fail anytime due to a node going away.
# Any job that can't be easily restarted or runs for a long time
# can set up special parameters
#
from airflow.sdk import dag, task
from kubernetes.client import models as k8s
from datetime import datetime
from pytz import timezone

executor_config_do_not_disrupt = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            # This is required
            containers=[
                k8s.V1Container(
                    name="base",
                )
            ],
            # Karpenter will ensure that this pod goes on an on-demand node
            # so it can't be shut down by spot interrupts
            node_selector={"karpenter.sh/capacity-type": "on-demand"},
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


@dag(tags=["example"], catchup=False)
def do_not_disrupt_test():
    @task.bash
    def sleep_bash():
        return "sleep 60"

    sleep_bash.override(executor_config=executor_config_do_not_disrupt)()


do_not_disrupt_test()
