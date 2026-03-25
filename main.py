from contextlib import asynccontextmanager
from fastapi import FastAPI, Security, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi_azure_auth import SingleTenantAzureAuthorizationCodeBearer
from settings import Settings
from typing import AsyncGenerator
from api.v1.main_simulations import predictive_attrition_router
import uvicorn

settings = Settings()

azure_scheme = SingleTenantAzureAuthorizationCodeBearer(
    app_client_id=settings.APP_CLIENT_ID,
    tenant_id=settings.TENANT_ID,
    scopes=settings.SCOPES,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Load OpenID config on startup.
    """
    await azure_scheme.openid_config.load_config()
    yield


app = FastAPI(
    title="Machine Learning Services",
    swagger_ui_oauth2_redirect_url="/oauth2-redirect",
    swagger_ui_init_oauth={
        "usePkceWithAuthorizationCodeGrant": True,
        "clientId": settings.OPENAPI_CLIENT_ID,
        "scopes": [
            "openid",
            "profile",
            f"api://{settings.APP_CLIENT_ID}/user_impersonation",
        ],
        "appName": "RSM Analytics",
    },
    swagger_ui_parameters={"persistAuthorization": True},
)

if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            str(origin) for origin in settings.BACKEND_CORS_ORIGINS
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


protected = APIRouter(
    dependencies=[Security(azure_scheme, scopes=["user_impersonation"])]
)
public = APIRouter()


@public.get("/", include_in_schema=False)
async def root():
    """
    Root endpoint that redirects to API documentation.

    This endpoint serves as the entry point for the API. When accessed,
    it automatically redirects users to the interactive API documentation
    page provided by FastAPI's Swagger UI.

    Returns:
        RedirectResponse: A redirect response pointing to the Swagger UI
                        documentation interface at /docs endpoint.

    Note:
        - This endpoint is excluded from the OpenAPI schema
        to avoid duplication in the API documentation.
        - The hardcoded localhost URL assumes the application is running
        on the local machine at port 8000. Consider using environment
        variables or configuration for production deployments.
        - HTTP GET method is used as this is a read-only redirect operation.

    Example:
        Accessing GET / will automatically redirect the browser to
        http://localhost:8000/docs
    """
    return RedirectResponse(url="http://localhost:8001/docs")


app.include_router(protected)
app.include_router(
    predictive_attrition_router,
    prefix="/predictive_attrition",
    tags=["Predictive Attrition"],
)
app.include_router(public)

uvicorn.run(app, host="localhost", port=8001, log_level="debug", reload=False)
