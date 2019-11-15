import os
import glob

import parsl
from parsl.app.app import bash_app
from igsb import config

parsl.set_stream_logger()
parsl.load(config)


@bash_app
def run_star(genome_lib, output, left_fq, right_fq, container='trinityctat/starfusion:1.8.0', memory=200, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    import os

    data, left_fq = os.path.split(left_fq)
    _, right_fq = os.path.split(right_fq)

    command = (
        'echo $HOSTNAME; '
        'docker pull {container}; '
        'docker run '
        '--memory={memory}gb '
        '-v {data}:/data '
        '-v {genome_lib}:/genome-lib '
        '-v {output}:/output '
        'trinityctat/starfusion:1.8.0 '
        ' /usr/local/src/STAR-Fusion/STAR-Fusion '
        '--left_fq /data/{left_fq} '
        '--right_fq /data/{right_fq} '
        '--genome_lib_dir /genome-lib '
        '-O /output '
        '--FusionInspector validate '
        '--examine_coding_effect '
        '--denovo_reconstruct '
    )

    return command.format(
        container=container,
        data=data,
        genome_lib=genome_lib,
        output=output,
        left_fq=left_fq,
        right_fq=right_fq,
        memory=memory
    )

patient_dirs = glob.glob('/cephfs/PROJECTS/FO_NBCP_Novartis/wabcs_rna/LIB-*')
genome_lib = '/cephfs/users/annawoodard/gene-fusion/GRCh38_gencode_v31_CTAT_lib_Oct012019.plug-n-play/ctat_genome_lib_build_dir'
outdir = '/cephfs/users/annawoodard/gene-fusion/results_v1'

for patient_dir in patient_dirs:
    patient_id = os.path.split(patient_dir)[-1]
    samples = sorted(glob.glob(os.path.join(patient_dir, '*fastq.gz')))
    pairs = [samples[i:i + 2] for i in range(0, len(samples), 2)]

    for left_fq, right_fq in pairs:
        sample_id = os.path.basename(left_fq).split('_')[2]
        output = os.path.join(outdir, patient_id, sample_id)
        os.makedirs(os.path.dirname(output), exist_ok=True)

        run_star(genome_lib, output, left_fq, right_fq)

parsl.wait_for_current_tasks()

print('finished processing!')
