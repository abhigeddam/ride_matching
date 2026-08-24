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
