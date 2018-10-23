SHELL := /bin/bash
.PHONY: check_env test

.env:
	cat .env.dist > .env; vim .env

venv: venv/bin/activate

venv/bin/activate: requirements.txt
	test -d venv || python3 -m venv venv
	venv/bin/pip install -Ur requirements.txt
	touch venv/bin/activate

check_env: venv .env
	@which python | grep `pwd` || source ./venv/bin/activate;

test: check_env
	source .env; \
	pytest -vv etl.py

data: check_env
	source .env; \
	python etl.py