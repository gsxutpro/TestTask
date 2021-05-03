ifeq ($(OS),Windows_NT)
    detected_OS := Windows
    build_script := win_build.bat
    run_script := win_run.bat
else
    detected_OS := $(shell uname)
    build_script := ./build.sh
    run_script := ./run.sh
endif

help:
	@echo "build -- build environment and install requirements"
	@echo "run -- run services & console"
	@echo "os_ver -- system type"
build:
	$(build_script)
run:
	$(run_script)
os_ver:
	$(info    System is $(detected_OS))