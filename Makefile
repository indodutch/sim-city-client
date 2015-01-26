.PHONY: all test clean pyflakes pyflakes-exists unittest install reinstall

all: install

install:
	pip install .
	
reinstall:
	pip install --upgrade --no-deps .

pyflakes-exists: ; @which pyflakes > /dev/null

pyflakes: pyflakes-exists 
	@echo "======= PyFlakes ========="
	@find . -name *.py -exec pyflakes {} \;

unittest:
	@echo "======= Unit Tests ========="
	@python -m unittest discover tests

test: pyflakes unittest

clean: 
	rm -rf build/
	find . -name *.pyc -delete
	find . -name *.pyo -delete
