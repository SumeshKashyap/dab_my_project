1. Hatchling issue leading to module not found error
Root cause summary: Hatchling can't handle two separate source roots (src/ and src/ingestion/) — it always computed the common
prefix src/ and installed the ingestion package as ingestion/dab_my_project_ingestion/ instead of dab_my_project_ingestion/.
Switching to setuptools with where = ["src", "src/ingestion"] solves this cleanly. The stale egg-info from previous builds was
also causing dab_my_project_etl to leak in (now cleaned up).
also causing dab_my_project_etl to leak in (now cleaned up).