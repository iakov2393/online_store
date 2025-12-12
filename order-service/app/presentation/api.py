from http import HTTPStatus

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.application.container import ApplicationContainer
from app.application.create_order import CreateOrderUseCase, OrderDTO
from app.core.models import Order

router = APIRouter()


class OrderCreateRequest(OrderDTO):
    pass


class OrderResponseModel(Order):
    pass


@router.post(
    "/orders",
    status_code=HTTPStatus.CREATED,
    response_model=OrderResponseModel,
)
@inject
async def create_order(
    order: OrderCreateRequest,
    create_order_use_case: CreateOrderUseCase = Depends(
        Provide[ApplicationContainer.create_order_use_case]
    ),
):
    try:
        return await create_order_use_case(order=order)
    except Exception:
        return JSONResponse(
            content={"message": "Internal server error while creating order"},
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        )
