from parsl.config import Config
from parsl.providers import SlurmProvider
from parsl.launchers import SingleNodeLauncher, SrunLauncher
from parsl.addresses import address_by_hostname
from parsl.executors import HighThroughputExecutor
from parsl.monitoring.monitoring import MonitoringHub

config = Config(
    executors=[
        HighThroughputExecutor(
            worker_debug=False,
            address=address_by_hostname(),
            cores_per_worker=40,
            provider=SlurmProvider(
                'daenerys',
                launcher=SingleNodeLauncher(),
                # launcher=SrunLauncher(),
                nodes_per_block=1,
                init_blocks=1,
                min_blocks=0,
                # max_blocks=2,
                max_blocks=15,
                scheduler_options='#SBATCH --mem=200G\n#SBATCH --exclude=kg15-[7-8]',
                worker_init='export PYTHONPATH=$PYTHONPATH:/cephfs/users/annawoodard/.local/lib/python3.7/site-packages; export PYTHONPATH=$PYTHONPATH:/cephfs/users/annawoodard/.local/lib/python3.7/site-packages',
                walltime='48:00:00'
            ),
        )
    ],
   monitoring=MonitoringHub(
       hub_address=address_by_hostname(),
       hub_port=55055,
       monitoring_debug=False,
       resource_monitoring_interval=10,
   ),
   retries=10,
   strategy='simple'
)
