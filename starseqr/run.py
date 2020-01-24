import os
import glob

import parsl
from parsl.app.app import bash_app
from igsb import config

parsl.set_stream_logger()
parsl.load(config)

@bash_app(cache=True)
def run_starseqr(
            raw_data,
            base_dir,
            sample,
            output,
            star_index,
            gtf,
            assembly,
            container='eagenomics/starseqr:0.6.7',
            stderr=parsl.AUTO_LOGNAME,
            stdout=parsl.AUTO_LOGNAME):
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
        '-v {output}:/output '
        '-v {star_index}:/star_index '
        '-v {gtf}:/gencode.gtf '
        '-v {assembly}:/assembly.fa '
        '-v {base_dir}/data/interim:/data '
        '{container} '
        'starseqr.py '
        '-1 /data/{sample}/merged.R1.fastq.gz '
        '-2 /data/{sample}/merged.R2.fastq.gz '
        '-p /output/ss '
        '-i /star_index '
        '-g /gencode.gtf '
        '-r /assembly.fa '
        '-m 1 '
        '-vv'
    )

    return command.format(
        raw_data=raw_data,
        base_dir=base_dir,
        sample=sample,
        output=output,
        star_index=star_index,
        gtf=gtf,
        assembly=assembly,
        container=container,
    )

star_index = '/cephfs/users/annawoodard/gene-fusion/arriba/references/STAR_index_GRCh38_GENCODE28'
gtf = '/cephfs/users/annawoodard/gene-fusion/arriba/references/GENCODE28.gtf'
assembly = '/cephfs/users/annawoodard/gene-fusion/arriba/references/GRCh38.fa'
outdir = '/cephfs/users/annawoodard/gene-fusion/starseqr/results_v3'
raw_data = '/cephfs/PROJECTS/FO_NBCP_Novartis/wabcs_rna/'
base_dir = '/'.join(os.path.abspath(__file__).split('/')[:-2])
sample_dirs = glob.glob(os.path.join(raw_data, 'LIB-*'))

for sample_dir in sample_dirs:
    sample_id = os.path.split(sample_dir)[-1]
    output = os.path.join(outdir, sample_id)
    os.makedirs(os.path.dirname(output), exist_ok=True)

    run_starseqr(raw_data, base_dir, sample_id, output, star_index, gtf, assembly)

parsl.wait_for_current_tasks()

print('finished processing!')
