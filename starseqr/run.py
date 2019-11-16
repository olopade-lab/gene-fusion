import os
import glob

import parsl
from parsl.app.app import bash_app
from igsb import config

parsl.set_stream_logger()
parsl.load(config)

@bash_app
def run_starseqr(
            output,
            left_fq,
            right_fq,
            star_index,
            gtf,
            assembly,
            container='eagenomics/starseqr:0.6.7',
            stderr=parsl.AUTO_LOGNAME,
            stdout=parsl.AUTO_LOGNAME):
    command = (
        'echo $HOSTNAME; '
        'docker pull {container}; '
        'docker run '
        # '-it '
        '-v {output}:/output '
        '-v {star_index}:/star_index '
        '-v {gtf}:/gencode.gtf '
        '-v {assembly}:/assembly.fa '
        '-v {left_fq}:/read1.fastq.gz '
        '-v {right_fq}:/read2.fastq.gz '
        '{container} '
        'starseqr.py '
        '-1 /read1.fastq.gz '
        '-2 /read2.fastq.gz '
        '-p /output/ss '
        '-i /star_index '
        '-g /gencode.gtf '
        '-r /assembly.fa '
        '-m 1 '
        '-vv'
    )

    return command.format(
        output=output,
        left_fq=left_fq,
        right_fq=right_fq,
        star_index=star_index,
        gtf=gtf,
        assembly=assembly,
        container=container,
    )

star_index = '/cephfs/users/annawoodard/gene-fusion/arriba/references/STAR_index_GRCh38_GENCODE28'
gtf = '/cephfs/users/annawoodard/gene-fusion/arriba/references/GENCODE28.gtf'
assembly = '/cephfs/users/annawoodard/gene-fusion/arriba/references/GRCh38.fa'
sample_dirs = glob.glob('/cephfs/PROJECTS/FO_NBCP_Novartis/wabcs_rna/LIB-*')
outdir = '/cephfs/users/annawoodard/gene-fusion/starseqr/results_v1'

for sample_dir in sample_dirs:
    sample_id = os.path.split(sample_dir)[-1]
    lanes = sorted(glob.glob(os.path.join(sample_dir, '*fastq.gz')))
    pairs = [lanes[i:i + 2] for i in range(0, len(lanes), 2)]

    for left_fq, right_fq in pairs:
        lane = os.path.basename(left_fq).split('_')[2]
        output = os.path.join(outdir, sample_id, lane)
        os.makedirs(os.path.dirname(output), exist_ok=True)

        run_starseqr(output, left_fq, right_fq, star_index, gtf, assembly)

parsl.wait_for_current_tasks()

print('finished processing!')
