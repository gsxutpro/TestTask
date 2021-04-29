#!/bin/bash
./venv/bin/python3 ./services/TransportHub.py &
./venv/bin/python3 ./services/data_gateway.py &
./venv/bin/python3 ./Console.py