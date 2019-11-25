import os
import glob

import parsl
from parsl.app.app import bash_app
from igsb import config

parsl.set_stream_logger()
parsl.load(config)


@bash_app(walltime=6 * 60 * 60)
def run_star(raw_data, base_dir, sample, genome_lib, output, container='trinityctat/starfusion:1.8.0', memory=200, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    import os

    command = ''
    if not os.path.isfile('{base_dir}/data/interim/{sample}/merged.R1.fastq.gz'.format(
                base_dir=base_dir,
                sample=sample
                )
            ):
        command = (
            'mkdir -p {base_dir}/data/interim/{sample}; '
            'cat {raw_data}/{sample}/*R1*.fastq.gz > {base_dir}/data/interim/{sample}/merged.R1.fastq.gz; '
            'cat {raw_data}/{sample}/*R2*.fastq.gz > {base_dir}/data/interim/{sample}/merged.R2.fastq.gz; '
        )

    command += (
        'echo $HOSTNAME; '
        'docker pull {container}; '
        'docker run '
        # '--memory={memory}gb '
        '-v {base_dir}/data/interim/{sample}:/data '
        '-v {genome_lib}:/genome-lib '
        '-v {output}:/output '
        'trinityctat/starfusion:1.8.0 '
        ' /usr/local/src/STAR-Fusion/STAR-Fusion '
        '--left_fq /data/merged.R1.fastq.gz '
        '--right_fq /data/merged.R2.fastq.gz '
        '--genome_lib_dir /genome-lib '
        '-O /output '
        '--FusionInspector validate '
        '--examine_coding_effect '
        '--denovo_reconstruct '
    )

    return command.format(
        raw_data=raw_data,
        base_dir=base_dir,
        sample=sample,
        container=container,
        genome_lib=genome_lib,
        output=output,
        memory=memory
    )

genome_lib = '/cephfs/users/annawoodard/gene-fusion/GRCh38_gencode_v31_CTAT_lib_Oct012019.plug-n-play/ctat_genome_lib_build_dir'
outdir = '/cephfs/users/annawoodard/gene-fusion/star-fusion/results_v3'
raw_data = '/cephfs/PROJECTS/FO_NBCP_Novartis/wabcs_rna/'
base_dir = '/'.join(os.path.abspath(__file__).split('/')[:-2])
sample_dirs = glob.glob(os.path.join(raw_data, 'LIB-*'))

for sample_dir in sample_dirs:
    sample_id = os.path.split(sample_dir)[-1]
    output = os.path.join(outdir, sample_id)
    os.makedirs(os.path.dirname(output), exist_ok=True)

    run_star(raw_data, base_dir, sample_id, genome_lib, output)

parsl.wait_for_current_tasks()

print('finished processing!')
