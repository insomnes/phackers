# Protohackers Challenges Solutions
https://protohackers.com

# Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/insomnes/phackers.git
   cd phackers
   ```

2. Install package:
   ```bash
   uv sync
   ```

# Echo Server
[Echo Server](https://protohackers.com/problem/0) - A simple echo server that listens for incoming TCP connections and echoes back any data it receives.

Solution: [echo.py](solutions/echo.py)

To run: `uv run ph-echo [-v] [--host HOST] [--port PORT]`
