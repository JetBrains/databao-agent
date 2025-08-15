# Portus: NL queries for `pandas`

## `sqlalchemy` + `pandas`

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://readonly_role:>sU9y95R(e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix"
)
df = pd.read_sql(
    """
    SELECT *
    FROM netflix_shows
    WHERE country = 'Germany'
    """,
    engine
)
print(df)
```

## `sqlalchemy` + `pandas` + `portus`

```python
import portus
portus.init()

df = pd.read_ai("list all german shows", engine)
print(df)
```

