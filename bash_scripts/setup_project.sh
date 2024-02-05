#!/bin/bash

# Create project folders
mkdir -p large-scale-data-processing/{data/{raw,processed,output},notebooks,src/{etl,utils},scripts}

# Create new folder within output for storing data
mkdir large-scale-data-processing/data/raw/output/joined_table_a

# Create Python files
touch large-scale-data-processing/src/{etl/__init__.py,etl/extract.py,etl/transform.py,etl/load.py,utils/__init__.py,utils/config.py,utils/logging.py,main.py}

# Create Jupyter notebooks
touch large-scale-data-processing/notebooks/exploratory_analysis.ipynb
touch large-scale-data-processing/notebooks/preprocessing.ipynb
touch large-scale-data-processing/notebooks/analysis.ipynb

# Create requirements.txt
touch large-scale-data-processing/requirements.txt

# Create README.md
touch large-scale-data-processing/README.md

# Create .gitignore
echo "venv/" > large-scale-data-processing/.gitignore

echo "Estrutura de pastas costruída com sucesso!"
