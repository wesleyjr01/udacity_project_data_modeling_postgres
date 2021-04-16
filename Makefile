usr=$(USER)
install: python3-7 virtualenv jupyter
	. venv/bin/activate; \
	pip install -r requirements.txt; \
	python -m ipykernel install --user --name=venv

jupyter: python3-7 virtualenv
	command -v jupyter-notebook || sudo apt install jupyter-notebook; \
	sudo chown -R $(usr): ~/.local/share/jupyter; \


python3-7:
	python3.7 --version || sudo apt update; \
	python3.7 --version || sudo apt install software-properties-common; \
	python3.7 --version || sudo add-apt-repository ppa:deadsnakes/ppa; \
	python3.7 --version || sudo apt install python3.7; \


virtualenv:
	virtualenv --version || sudo apt install virtualenv; \
	test -d venv || virtualenv venv -p python3.7