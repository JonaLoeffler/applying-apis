#!/bin/sh

pip install virtualenv

python -m venv ./env

source ./env/bin/activate

pip install -r requirements.txt

# Parsing the repositories may take a very long time, rather download and extract them
# mvn clean compile exec:java -Dexec.mainClass="de.uni_koblenz.gorjatschev.applyingapis.Application"

curl -sS https://cloud.uni-koblenz.de/s/BtxQzwkREJXHEfR > ./output/data.zip
unzip ./output/data.zip -d ./output

python src/main/python/package_analyzer.py
python src/main/python/dependencies_counter.py
python src/main/python/repositories_analyzer.py
