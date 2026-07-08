---
status: proposed
---

# Start With Batch Data Movement Before CDC

Data Spark should start with batch data movement and postpone CDC or near-real-time replication until the batch model is proven. This keeps the first version focused on portable BI-ready loads, while avoiding early commitment to state durability, delivery guarantees, log-based database capture, and a much larger connector test matrix.
