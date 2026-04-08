set -e

# Creating venv and loading all dependencies
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Running scripts for upload everything to PostgreSQL
echo "Running preprocess.py..."
python scripts/preprocess.py

echo "Running build_projectdb.py..."
python scripts/build_projectdb.py
