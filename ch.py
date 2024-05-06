
import app
from fastapi.testclient import TestClient
from datetime import datetime
import requests

client = TestClient(app.app)

user_id = 1000
time = datetime(2022, 12, 20)

try:
    r = client.get(
        "/post/recommendations/",
        params={"id": user_id, "time": time, "limit": 5}
    )
except Exception as e:
    raise ValueError(f"X ошибка при выполнении запроса {type(e)} {str(e)}") from e

print(r.json())
