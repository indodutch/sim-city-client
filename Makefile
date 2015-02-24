.PHONY: all requirements test-requirements test clean pyflakes pyflakes-exists unittest unittest-coverage fulltest install reinstall

all: install

requirements:
	@pip install -r requirements.txt

test-requirements:
	@pip install -r test_requirements.txt > /dev/null

install: requirements
	@pip install .
	
reinstall:
	@pip install --upgrade --no-deps .

pyflakes:
	@echo "======= PyFlakes ========="
	@find simcity -name '*.py' -exec pyflakes {} \;
	@find scripts -name '*.py' -exec pyflakes {} \;
	@find tests -name '*.py' -exec pyflakes {} \;

unittest:
	@echo "======= Unit Tests ========="
	@nosetests

test: test-requirements pyflakes unittest

unittest-coverage:
	@echo "======= Unit Tests ========="
	@nosetests --with-coverage

fulltest: test-requirements pyflakes unittest-coverage

clean: 
	rm -rf build/
	find . -name *.pyc -delete
	find . -name *.pyo -delete
