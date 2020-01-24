import os
import glob

import pandas as pd
import parsl
from parsl.app.app import bash_app
from igsb import config

parsl.set_stream_logger()
parsl.load(config)

@bash_app
def run_fusion_inspector(base_dir, raw_data, interim_data, genome_lib, sample, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    """Run fusion inspector on an RNA-seq sample

    This app will merge individual lane files before running fusion inspector. If the merged file already exists, that
    step will be skipped. This is slightly dangerous, because it is possible a merge job was killed before completion.
    If you have doubts, delete all the merged files to force re-merging.
    """

    import os

    command = ''
    if not os.path.isfile('{interim_data}/{sample}/merged.R1.fastq.gz'.format(
                interim_data=interim_data,
                sample=sample
                )
            ):
        command = (
            'mkdir -p {interim_data}/{sample}; '
            'cat {raw_data}/{sample}/*R1*.fastq.gz > {interim_data}/{sample}/merged.R1.fastq.gz; '
            'cat {raw_data}/{sample}/*R2*.fastq.gz > {interim_data}/{sample}/merged.R2.fastq.gz; '
        )

    command += (
        'echo hostname is: $HOSTNAME; '
        'docker run '
        '-v {base_dir}/data/interim:/data '
        '-v {genome_lib}:/genome-lib '
        '-v {base_dir}/visualization:/base '
        '--rm trinityctat/fusioninspector:2.2.1 FusionInspector '
        '--fusions /base/targets/{sample}.txt '
        '-O /base/fusion-inspector-outputs/{sample} '
        '--left_fq /data/{sample}/merged.R1.fastq.gz '
        '--right_fq /data/{sample}/merged.R2.fastq.gz '
        '--genome_lib /genome-lib '
        '--out_prefix finspector '
        '--vis '
        # '--include_Trinity '
        '--examine_coding_effect '
        '--extract_fusion_reads_file /base/pe_samples/fusion_reads/{sample} '
        '--annotate '
    )

    return command.format(
        base_dir=base_dir,
        raw_data=raw_data,
        interim_data=interim_data,
        genome_lib=genome_lib,
        sample=sample
    )

genome_lib = '/cephfs/users/annawoodard/gene-fusion/GRCh38_gencode_v31_CTAT_lib_Oct012019.plug-n-play/ctat_genome_lib_build_dir'
raw_data = '/cephfs/PROJECTS/FO_NBCP_Novartis/wabcs_rna/'
base_dir = '/'.join(os.path.abspath(__file__).split('/')[:-2])
interim_data = os.path.join(base_dir, 'data/interim')

data = pd.read_parquet(os.path.join(base_dir, 'data/processed/fusions.parquet'))
for sample in data['sample'].unique():
    unique_fusions = data[data['sample'] == sample].name.unique().tolist()
    with open('{}/visualization/targets/{}.txt'.format(base_dir, sample), 'w') as f:
        f.write('\n'.join(unique_fusions))
    run_fusion_inspector(base_dir, raw_data, interim_data, genome_lib, sample)

parsl.wait_for_current_tasks()

print('finished processing!')
