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


@bash_app(cache=True)
def run_arriba(base_dir, raw_data, references, sample, output, container='uhrigs/arriba:1.1.0', stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
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
        '--rm '
        '-v {base_dir}/data/interim:/data '
        '-v {output}:/output '
        '-v {references}:/references:ro '
        '-v {base_dir}/data/interim/{sample}/merged.R1.fastq.gz:/read1.fastq.gz:ro '
        '-v {base_dir}/data/interim/{sample}/merged.R2.fastq.gz:/read2.fastq.gz:ro '
        '{container} '
        'arriba.sh'
    )

    return command.format(
        base_dir=base_dir,
        raw_data=raw_data,
        sample=sample,
        container=container,
        output=output,
        references=references
    )

references = '/cephfs/users/annawoodard/gene-fusion/arriba/references'
# print('starting to build index')
# build_index(references).result()
# print('finished building index')

outdir = '/cephfs/users/annawoodard/gene-fusion/arriba/results_v3'
raw_data = '/cephfs/PROJECTS/FO_NBCP_Novartis/wabcs_rna/'
base_dir = '/'.join(os.path.abspath(__file__).split('/')[:-2])
sample_dirs = glob.glob(os.path.join(raw_data, 'LIB-*'))

for sample_dir in sample_dirs:
    sample_id = os.path.split(sample_dir)[-1]
    output = os.path.join(outdir, sample_id)
    os.makedirs(os.path.dirname(output), exist_ok=True)

    run_arriba(base_dir, raw_data, references, sample_id, output)

parsl.wait_for_current_tasks()

print('finished processing!')
