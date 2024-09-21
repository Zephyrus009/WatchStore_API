import pandas as pd
from fastapi import Depends, Security, status, APIRouter,HTTPException
from datetime import datetime, timedelta, timezone
from typing import Annotated
import nest_asyncio
import IPython
from standard_func import read_data, push_data, update_data, drop_data
from security import get_password_hash,get_current_user, User, verify_password

nest_asyncio.apply()

router = APIRouter(tags=["API Operations"])


###################### User Registration ################################ 

# register user
@router.post("/register/")
async def add_user(username: str, fullname: str, email: str, password:str, disabled: bool):
    user_frame = {
        "username":username,
        "full_name":fullname,
        "email":email,
        "hashed_password":get_password_hash(password),
        "disabled":disabled
    }
    user_frame = pd.DataFrame(user_frame, index=[0])
    await push_data(data = user_frame, table_name="User_Watch")
    return {"Successful":"User Created"}, status.HTTP_200_OK

# password reset
@router.post("/password_reset/")
async def password_reset(password:str,updated_password:str,current_user: Annotated[User, Depends(get_current_user)]):
    if current_user:
        user_data = await read_data(f'select * from "User_Watch" where user_id = {current_user.user_id}')
        if verify_password(password,user_data['hashed_password'].iloc[0]):
            user_data['hashed_password'] = get_password_hash(updated_password)
            await update_data(data=user_data,table_name="User_Watch",condition=f"user_id = {current_user.user_id}")
            return {"message": "Successfully Updated password"},status.HTTP_201_CREATED
        else:
            return {"message": "Incorrect Password"}, status.HTTP_203_NON_AUTHORITATIVE_INFORMATION
    else:
        return {"message": "User Not Found"},status.HTTP_401_UNAUTHORIZED
    
##################### Profile Management ######################################

#Products Home
@router.get("/home/")
async def home(limit : int, offset: int):
    data = await read_data(f'SELECT "ProductID" as product_id,"Brand" AS brand,"Current Price" AS curr_price,"Original Price" AS org_price, ROUND("Discount Percentage"::NUMERIC, 2) AS Disc_Perc FROM "Smartwatches" LIMIT {limit} OFFSET {offset}')
    data = [row.to_dict() for _,row in data.iterrows()]
    return data

#Product Details Page
@router.get("/watches/{id}")
async def watch_details(id:str, current_user: Annotated[User, Depends(get_current_user)]):
    if current_user:
        data = await read_data(f"""SELECT "ProductID" as product_id,"Brand" AS brand,"Current Price" AS curr_price,"Original Price" AS org_price, ROUND("Discount Percentage"::NUMERIC, 2) AS Disc_Perc FROM "Smartwatches" where "ProductID" = '{id}'""")
        data = [row.to_dict() for _,row in data.iterrows()]
        return data
    else:
        return {"message": "User Not Found"},status.HTTP_401_UNAUTHORIZED

# Brand Watch Details
@router.get("/brand_watch/{brand}/{limit}/{offset}")
async def brand_watch(brand:str,limit:int,offset:int,current_user: Annotated[User, Depends(get_current_user)]):
    if current_user:
        data = await read_data(f"""SELECT "ProductID" as product_id,"Brand" AS brand,"Current Price" AS curr_price,"Original Price" AS org_price, ROUND("Discount Percentage"::NUMERIC, 2) AS Disc_Perc FROM "Smartwatches" where "Brand" = '{brand}' LIMIT {limit} OFFSET {offset}""")
        data = [row.to_dict() for _,row in data.iterrows()]
        return data
    else:
        return {"message": "User Not Found"},status.HTTP_401_UNAUTHORIZED
    
# Add to Cart
@router.post("/add_to_cart/{product_id}/{quantity}/{price}")
async def add_to_cart(product_id:str,quantity:int,price:float,current_user: Annotated[User, Depends(get_current_user)]):
    if current_user:
        cart_item = pd.DataFrame({
            "user_id":current_user.user_id,
            "product_id":product_id,
            "quantity":quantity,
            "price":price
        },index=[0])
        await push_data(data=cart_item,table_name="Cart")
        return {"message":f"Item with Product ID : {product_id}, Added to Cart Successful"}, status.HTTP_201_CREATED
    else:
        return {"message": "User Not Found"},status.HTTP_401_UNAUTHORIZED
    

# Filter Proucts on Brand, Strap Material,  Current Price, Rating,Battery Life,Display Size
@router.get("/filter_watches/{brand}/{strap_material}/{price}/{rating}/{battery_life}/{limit}/{offset}") 
async def  filter_watches(
    current_user: Annotated[User, Depends(get_current_user)],
    limit:int,
    offset:int,
    brand:str | None = None,
    strap_material:str | None = None,
    price :str | None = None,
    rating:str | None = None,
    battery_life:str | None = None,
    ):
    if current_user:
        brand = [str(br) for br in brand.split(",")]
        strap_material = [str(strap) for strap in strap_material.split(",")]
        price = [float(str(pr).strip()) for pr in price.split(',')]
        rating = [float(str(ra).strip()) for ra in rating.split(',')]
        battery_life = [float(str(bat).strip()) for bat in battery_life.split(',')]

        if brand is None:
            data = await read_data(f"""SELECT "ProductID" as product_id,"Brand" AS brand,"Current Price" AS curr_price,"Original Price" AS org_price, ROUND("Discount Percentage"::NUMERIC, 2) AS Disc_Perc,"Rating" as rating,"Battery Life" as battery_life,"Display Size" as display_size FROM "Smartwatches" where "Strap Material" IN {tuple(strap_material)} LIMIT {limit} OFFSET {offset}""")
        
        elif strap_material is None:
            data = await read_data(f"""SELECT "ProductID" as product_id,"Brand" AS brand,"Current Price" AS curr_price,"Original Price" AS org_price, ROUND("Discount Percentage"::NUMERIC, 2) AS Disc_Perc,"Rating" as rating,"Battery Life" as battery_life,"Display Size" as display_size FROM "Smartwatches" where "Brand" IN {tuple(brand)} LIMIT {limit} OFFSET {offset}""")
        elif brand is None and strap_material is None:
            data = await read_data(f"""SELECT "ProductID" as product_id,"Brand" AS brand,"Current Price" AS curr_price,"Original Price" AS org_price, ROUND("Discount Percentage"::NUMERIC, 2) AS Disc_Perc,"Rating" as rating,"Battery Life" as battery_life,"Display Size" as display_size FROM "Smartwatches" LIMIT {limit} OFFSET {offset}""")
        else:
            data = await read_data(f"""SELECT "ProductID" as product_id,"Brand" AS brand,"Current Price" AS curr_price,"Original Price" AS org_price, ROUND("Discount Percentage"::NUMERIC, 2) AS Disc_Perc,"Rating" as rating,"Battery Life" as battery_life,"Display Size" as display_size FROM "Smartwatches" where "Brand" IN {tuple(brand)} and "Strap Material" IN {tuple(strap_material)} LIMIT {limit} OFFSET {offset}""")


        if price:
            data = data[(data["org_price"] >= float(price[0])) & (data["org_price"] <= float(price[1]))]
        
        if rating:
            data = data[(data["rating"] >= float(rating[0])) & (data["rating"] <= float(rating[1]))]

        if battery_life:
            data = data[(data["battery_life"] >= float(battery_life[0])) & (data["battery_life"] <= float(battery_life[1]))]
        
        data = [row.to_dict() for _,row in data.iterrows()]
        
        return data
    else:
        return {"message": "User Not Found"},status.HTTP_401_UNAUTHORIZED

###################### Payments API ###############################################
@router.get("/pay/")
async def payments(current_user: Annotated[User, Depends(get_current_user)]):
    if current_user:
        user_data = await read_data(f'select * from "User_Watch" where user_id = {current_user.user_id}')
        cart_data = await read_data(f'select "product_id","quantity","price" from "Cart" where user_id = {current_user.user_id}')
        cart_data["final_price"] = cart_data["price"].astype(float) * cart_data["quantity"].astype(int)
        products = ','.join(list(cart_data["product_id"]))
        amount = cart_data["final_price"].sum()

        if float(user_data["balance"].iloc[0]) >= amount:
            user_data["balance"] = round((float(user_data["balance"].iloc[0]) - amount),2)
            transaction_data = pd.DataFrame({
                "user_id": current_user.user_id,
                "product_ids": products,
                "amount": amount,
                "status": "success"
            },index=[0])
            await push_data(data=transaction_data,table_name="Transaction")
            await update_data(data=user_data,table_name="User_Watch",condition=f'user_id = {current_user.user_id}')
            await drop_data(table_name="Cart",condition=f'user_id = {current_user.user_id}')
            return {"payment_status":"Successful","user_id":current_user.user_id,"current_balance":user_data["balance"].iloc[0]}
        else:
            transaction_data = pd.DataFrame({
                "user_id": current_user.user_id,
                "product_ids": products,
                "amount": amount,
                "status": "failed"
            },index=[0])
            await push_data(data=transaction_data,table_name="Transaction")
            return {"payment_status":"Payment failed due to insufficient balance","user_id":current_user.user_id,"current_balance":user_data["balance"].iloc[0]}
        

@router.get("/status")
async def read_system_status(current_user: Annotated[User, Depends(get_current_user)]):
    return {"status": "ok","user":current_user.username}
