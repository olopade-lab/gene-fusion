import os
import glob

import parsl
from parsl.app.app import bash_app
from igsb import config

parsl.set_stream_logger()
parsl.load(config)

@bash_app
def build_index(references, image='uhrigs/arriba:1.1.0', assembly_and_annotation='GRCh38+GENCODE28', stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    command = (
        'docker run '
        '--rm '
        '-v {references}:/references '
        '{image} '
        'download_references.sh '
        '{assembly_and_annotation} '
    )

    return command.format(
        references=references,
        image=image,
        assembly_and_annotation=assembly_and_annotation
    )


@bash_app
def run_arriba(references, output, left_fq, right_fq, container='uhrigs/arriba:1.1.0', stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    import os

    command = (
        'echo $HOSTNAME; '
        'docker pull {container}; '
        'docker run '
        '--rm '
        '-v {output}:/output '
        '-v {references}:/references:ro '
        '-v {left_fq}:/read1.fastq.gz:ro '
        '-v {right_fq}:/read2.fastq.gz:ro '
        '{container} '
        'arriba.sh'
    )

    return command.format(
        container=container,
        output=output,
        references=references,
        left_fq=left_fq,
        right_fq=right_fq,
    )

references = '/cephfs/users/annawoodard/gene-fusion/arriba/references'
print('starting to build index')
build_index(references).result()
print('finished building index')

sample_dirs = glob.glob('/cephfs/PROJECTS/FO_NBCP_Novartis/wabcs_rna/LIB-*')
outdir = '/cephfs/users/annawoodard/gene-fusion/arriba/results_v1'

for sample_dir in sample_dirs:
    sample_id = os.path.split(sample_dir)[-1]
    lanes = sorted(glob.glob(os.path.join(sample_dir, '*fastq.gz')))
    pairs = [lanes[i:i + 2] for i in range(0, len(lanes), 2)]

    for left_fq, right_fq in pairs:
        lane = os.path.basename(left_fq).split('_')[2]
        output = os.path.join(outdir, sample_id, lane)
        os.makedirs(os.path.dirname(output), exist_ok=True)

        run_arriba(references, output, left_fq, right_fq)

parsl.wait_for_current_tasks()

print('finished processing!')
