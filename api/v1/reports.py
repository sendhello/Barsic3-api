from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from starlette import status

from models import User
from schemas import UserCreated

router = APIRouter()


@router.post("/signup", response_model=UserCreated, status_code=status.HTTP_201_CREATED)
async def create_user(user_create: UserCreated) -> UserCreated:
    user_dto = jsonable_encoder(user_create)
    try:
        raw_user = await User.create(**user_dto)

    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with such login is registered already",
        )

    return raw_user
