from fastapi import FastAPI
import apis, security

app = FastAPI()

app.include_router(apis.router)
app.include_router(security.router)




