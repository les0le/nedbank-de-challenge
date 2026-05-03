"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""

from ingest import run_ingestion
from transform import run_transformation
from provision import run_provisioning
import sys
from datetime import datetime


if __name__ == "__main__":
    try:
        print(f"Ingestion Start: {datetime.now()}")
        run_ingestion()
        print(f"Ingestion Ended: {datetime.now()}")
        
        print(f"Transformation Start: {datetime.now()}")
        run_transformation()
        print(f"Transformation Ended: {datetime.now()}")

        print(f"Provisioning Start: {datetime.now()}")
        run_provisioning()
        print(f"Provisioning Ended: {datetime.now()}")  
        sys.exit(0)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
