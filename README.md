# CS214 Nim Game Server

This project implements a networked Nim game server in C, supporting the Nim Game Protocol (NGP) for client-server communication. The server manages multiple games concurrently using POSIX threads and handles client connections, game state, and protocol compliance.

## Features
- Supports two-player Nim games over TCP sockets
- Implements the NGP protocol for robust client-server messaging
- Handles client connection, waiting, game setup, and game play
- Validates moves, manages game state, and handles errors/forfeits
- Concurrent game and client management using pthreads

## Directory Structure
- `nimd.c` — Main server implementation
- `nimd` — Compiled server binary
- `src/` — Supporting C source and header files
- `testc` — Compiled C client for protocol testing
- `tests/` — Automated and manual test scripts and Python client
    - `ngp_client.py` — Python client for protocol and edge-case testing
    - `test_all.sh` — Runs all test scripts
    - `test_basic.sh` — Basic gameplay and protocol compliance tests
    - `test_forfeit.sh` — Tests forfeit scenarios and disconnects
    - `test_invalid.sh` — Tests invalid protocol messages and error handling
    - `test_match.sh` — Tests matching and game setup
    - `rawc` — Raw client file used by the test scripts (proper NGP protocol)

## How to Run
1. **Build the server and client:**
   ```bash
   make
   ```
2. **Start the server:**
   ```bash
   ./nimd 15000
   ```
3. **Connect clients:**
   - Python client: `python3 ngp_client.py <name> localhost 15000`

## Testing Strategy
Testing is performed using both automated shell scripts and manual client connections:

### Automated Tests
- Located in the `tests/` directory.
- Run all tests with:
  ```bash
  cd tests
  ./test_all.sh
  ```
- Individual scripts (`test_basic.sh`, `test_forfeit.sh`, etc.) target specific protocol features and edge cases.
- Scripts launch the server, connect clients, simulate moves, and check for correct protocol responses and error handling.

### Manual/Interactive Testing
- Use `ngp_client.py` to connect to the server and play games interactively.
- Useful for debugging, protocol exploration, and edge-case validation.

### Python Client
- `ngp_client.py` is a flexible tool for sending custom protocol messages and testing server robustness against malformed or unexpected input.

## Notes
- The server must be running before executing any test scripts or connecting clients.
- All protocol messages and errors are logged to stdout for debugging.
- Resource cleanup and error handling are robust to prevent crashes and leaks.

## Authors
- See `AUTHOR` file for contributors.

## License
- See project root for license information.
