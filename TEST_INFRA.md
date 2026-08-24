# E2E Test Infra: Real-Time Ride-Hailing System

## Test Philosophy
- Opaque-box, requirement-driven. No dependency on internal class design or private APIs.
- Operates via public interfaces: Kafka topics (`driver-locations`, `ride-requests`, `ride-matches`), Redis CLI / connection, and process execution.
- Methodology: Category-Partition + Boundary Value Analysis (BVA) + Pairwise Combinatorial Testing + Real-World Workload Testing.

## Feature Inventory & Test Matrix
| # | Feature | Source (Requirement) | Tier 1 (Feature) | Tier 2 (BVA) | Tier 3 (Pairwise) | Tier 4 (Scenario) |
|---|---------|---------------------|:----------------:|:------------:|:-----------------:|:-----------------:|
| F1 | Kafka KRaft & Redis Docker Startup | ORIGINAL_REQUEST §R1 | 5 | 5 | ✓ | ✓ |
| F2 | Automated Topic Initialization | ORIGINAL_REQUEST §R1 | 5 | 5 | ✓ | ✓ |
| F3 | Driver Location Ingestion to Redis | ORIGINAL_REQUEST §R2 | 5 | 5 | ✓ | ✓ |
| F4 | H3 Resolution 8 Cell Set Indexing | ORIGINAL_REQUEST §R2 | 5 | 5 | ✓ | ✓ |
| F5 | Dynamic Cell Migration Handling | ORIGINAL_REQUEST §R2 | 5 | 5 | ✓ | ✓ |
| F6 | Driver TTL Eviction (15s Window) | ORIGINAL_REQUEST §R2 | 5 | 5 | ✓ | ✓ |
| F7 | Ride Request Ingestion & Parsing | ORIGINAL_REQUEST §R3 | 5 | 5 | ✓ | ✓ |
| F8 | 7-Cell Neighborhood Expansion | ORIGINAL_REQUEST §R3 | 5 | 5 | ✓ | ✓ |
| F9 | Haversine Proximity Ranking | ORIGINAL_REQUEST §R3 | 5 | 5 | ✓ | ✓ |
| F10 | Closest Driver Dispatch Match Output | ORIGINAL_REQUEST §R3 | 5 | 5 | ✓ | ✓ |
| F11 | Stale/Offline Driver Exclusion | ORIGINAL_REQUEST §R3 | 5 | 5 | ✓ | ✓ |
| F12 | Ingestion Latency SLA (<10ms) | ORIGINAL_REQUEST §R4 / plan.md | 5 | 5 | ✓ | ✓ |

## Test Architecture
- **Runner**: Non-interactive verification script `simulation/simulate_and_verify.py` (wrapped by `simulation/verify.sh`).
- **Invocation**: `python3 simulation/simulate_and_verify.py` or `./simulation/verify.sh`.
- **Pass/Fail Semantics**: Exits with returncode `0` if all assertions pass; `1` otherwise. Outputs structured summary to stdout and writes machine-readable `verification_results.json`.
- **Directory Layout**:
  - `simulation/simulate_and_verify.py`: Simulation and automated test runner.
  - `simulation/verify.sh`: Executable wrapper for automated CI execution.
  - `verification_results.json`: Output test metrics and assertion logs.

## Real-World Application Scenarios (Tier 4)
| # | Scenario | Features Exercised | Complexity |
|---|----------|--------------------|------------|
| S1 | Single Driver Same-Cell Immediate Match | F1, F2, F3, F4, F7, F8, F9, F10 | Low |
| S2 | Multi-Driver Competitive Proximity Match across Adjacent Hex Cells | F3, F4, F8, F9, F10 | Medium |
| S3 | Moving Driver Crossing H3 Boundary Matched in New Cell | F3, F4, F5, F7, F8, F9, F10 | Medium |
| S4 | Stale Driver Ping Cessation & Graceful Next-Available Match | F3, F4, F6, F7, F8, F9, F10, F11 | High |
| S5 | High-Frequency Concurrent Requests with Driver Reservation Mutex | F7, F8, F9, F10 | High |
| S6 | Complete Out-of-Range Request Graceful Skipping (No False Match) | F7, F8, F10 | Low |

## Coverage Thresholds
- Tier 1 (Feature Coverage): ≥5 tests per feature (≥60 tests total across isolation cases)
- Tier 2 (Boundary & Corner Cases): ≥5 tests per feature (coordinate boundaries, max timestamps, cell edges, TTL limits)
- Tier 3 (Pairwise Interactions): Major interaction pairs tested
- Tier 4 (Real-World Scenarios): ≥6 realistic end-to-end workload simulations
