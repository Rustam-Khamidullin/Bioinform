from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

AIRFLOW_DATA = os.environ.get('AIRFLOW_DATA', '/home/rustam/airflow/data/')
REFERENCE = os.path.join(AIRFLOW_DATA, 'ecoli_ref.fna')
READS = os.path.join(AIRFLOW_DATA, 'SRR33602302.fastq')
ALIGNED_READS = os.path.join(AIRFLOW_DATA, 'aligned_reads.sam')
FLAGSTAT_LOGS = os.path.join(AIRFLOW_DATA, 'flagstat.log')

default_args = {
    'owner': 'bioinformatics',
    'start_date': datetime(2025, 1, 1)
}


def analyze_mapping(**context):
    try:
        with open(FLAGSTAT_LOGS, 'r') as f:
            flagstat_log = f.read()

        prcntg = None
        for line in flagstat_log.split('\n'):
            if 'mapped (' in line:
                prcntg = float(line.split('(')[1].split('%')[0].strip())
                break

        if prcntg is None:
            print(f"Error: No mapping percentage found in flagstat")
            return "Error"

        print(f"Mapped {prcntg}%")
        print("OK" if prcntg > 90.0 else "Not OK")
        return "OK" if prcntg > 90.0 else "Not OK"

    except Exception as e:
        print(f"Error reading flagstat file: {e}")
        return "Error"


with DAG(
        'alignment_quality_check',
        default_args=default_args,
        schedule="@once",
        catchup=False,
) as dag:
    index_genome = BashOperator(
        task_id='index_genome',
        bash_command='bwa index "{{ params.ref_genome }}"',
        params={'ref_genome': REFERENCE},
    )

    align_reads = BashOperator(
        task_id='align_reads',
        bash_command='bwa mem "{{ params.ref_genome }}" "{{ params.reads }}" > "{{ params.aligned_reads }}"',
        params={
            'ref_genome': REFERENCE,
            'reads': READS,
            'aligned_reads': ALIGNED_READS
        },
    )

    run_flagstat = BashOperator(
        task_id='run_flagstat',
        bash_command=f'samtools flagstat {ALIGNED_READS} > {FLAGSTAT_LOGS}'
    )

    analyze_results = PythonOperator(
        task_id='analyze_results',
        python_callable=analyze_mapping
    )

    index_genome >> align_reads >> run_flagstat >> analyze_results
