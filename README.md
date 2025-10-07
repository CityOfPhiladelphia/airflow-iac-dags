## Assigning resource requests and limits to tasks

By default, tasks are allocated ,a moderate amount of memory
and a moderate amount of cpu.

### Allocating memory consequences

**Allocating memory too low causes out of memory errors and your DAG will fail**, whereas allocating memory too high is just inefficient. It's more important that the DAG works than that it is heavily optimized, but please put effort in if possible.

Allocating memory does not need to be a power of 2, though it looks nice to developers. You can assign 321Mi, 727Mi, etc.

### Allocating cpu consequences

Most of the time, the pod can temporarily grab more CPU than it is assigned. Never lower the CPU allocation, as it could result in the pod taking a very long time to start up.

For tasks that are computationally heavy (e.g. Python scripts that have big nested loops), you should increase the CPU allocation to guarantee that the job completes timely.

### The default

The default is 512Mi of memory, and 150m (0.15) cpu

### Going below the default

For very small tasks, like basic bash commands, sometimes you can go below the default.

Do not drop memory below 256Mi

```py
executor_config_small_resources = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={
                            "cpu": "150m",  # also valid, cpu: 0.15
                            "memory": "300Mi",
                        },
                        limits={
                            "memory": "300Mi",  # request_memory and limit_memory should always be the same
                        },
                    ),
                )
            ]
        )
    )
}
```

### Going above the default

For big tasks, like anything with lots of loops or calculations, you can go above the default

```py
executor_config_small_resources = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={
                            "cpu": "400m",  # also valid, cpu: 0.4
                            "memory": "800Mi",
                        },
                        limits={
                            "memory": "800Mi",  # request_memory and limit_memory should always be the same
                        },
                    ),
                )
            ]
        )
    )
}
```
