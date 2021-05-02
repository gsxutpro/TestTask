#!/bin/bash
./venv/bin/python3 ./services/transport_hub.py &
./venv/bin/python3 ./services/data_gateway.py &
./venv/bin/python3 ./services/logger.py &
./venv/bin/python3 ./console.py