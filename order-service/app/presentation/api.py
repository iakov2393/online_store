from http import HTTPStatus

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.application.container import ApplicationContainer
from app.application.create_order import CreateOrderUseCase, OrderDTO
from app.core.models import Order
from app.infrastructure.unit_of_work import UnitOfWork

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


@router.get(
    "/orders/{order_id}",
    status_code=HTTPStatus.OK,
    response_model=OrderResponseModel,
)
@inject
async def get_order(
    order_id: str,
    unit_of_work: UnitOfWork = Depends(
        Provide[ApplicationContainer.infrastructure_container.unit_of_work]
    ),
):
    try:
        async with unit_of_work() as uow:
            order = await uow.orders.get_by_id(order_id)
            return order
    except ValueError:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=f"Order {order_id} not found"
        )
    except Exception as e:
        return JSONResponse(
            content={"message": f"Internal server error: {str(e)}"},
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        )
