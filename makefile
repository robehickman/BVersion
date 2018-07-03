VIRTUALENV = virtualenv --python=python2.7

build-requirements:
	$(eval TEMPDIR := $(shell mktemp -d))
	$(VIRTUALENV) $(TEMPDIR)
	$(TEMPDIR)/bin/pip install -U pip  # Upgrade pip
	$(TEMPDIR)/bin/pip install -Ue .   # Develop the current project locally
	# Freeze the dependencies ignoring the dependency links.
	$(TEMPDIR)/bin/pip freeze | grep -v -- '^-e' > requirements.txt
