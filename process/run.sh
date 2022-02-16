#!/bin/sh

pip install virtualenv #Optional

python -m venv ./env #Optional

source ./env/bin/activate #Optional

pip install -r requirements.txt

mvn clean compile exec:java -Dexec.mainClass="de.uni_koblenz.gorjatschev.applyingapis.Application"

python src/main/python/dependencies_counter.py
