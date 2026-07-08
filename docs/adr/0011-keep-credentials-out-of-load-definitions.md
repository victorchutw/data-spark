---
status: accepted
---

# Keep Credentials Out of Load Definitions

Data Spark load definitions will contain connection references rather than credential values, so YAML can be safely versioned and shared. Credentials resolve from environment variables, local connection profiles, or secure prompts, and generated YAML skeletons must not include secret-bearing values.
