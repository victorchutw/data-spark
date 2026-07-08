---
status: accepted
---

# Version Load Definitions and Load Reports

Data Spark v1 will require YAML load definitions to declare `version: 1` and JSON load reports to declare `report_version: 1`. Versioned contracts let load definitions live safely in git and let external orchestrators parse load reports without depending on unstable implicit formats as the product evolves.
