import uuid
from http import HTTPStatus
from typing import Callable
from unittest.mock import ANY

import pytest
from httpx import AsyncClient

from app.core.models import OrderStatusEnum
from app.presentation.api import OrderCreateRequest


@pytest.mark.asyncio
async def test_create_order(
    test_async_client: AsyncClient,
    item_factory: Callable,
):
    # Given
    items = [item_factory() for _ in range(3)]
    user_id = str(uuid.uuid4())
    req = OrderCreateRequest(user_id=user_id, items=items)

    # When
    response = await test_async_client.post("/orders", json=req.model_dump(mode="json"))

    # Then
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data["id"] is not None
    assert data["user_id"] == user_id
    assert data["amount"] == "31.50"
    assert data["status"] == OrderStatusEnum.NEW
    assert data["status_history"] == [
        {"status": str(OrderStatusEnum.NEW), "created_at": ANY}
    ]
