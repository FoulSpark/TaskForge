"""Authentication principal introspection route for Phase 10 Web Console."""

from fastapi import APIRouter, Depends

from taskforge.api.dependencies import get_current_principal
from taskforge.api.schemas import AuthPrincipalResponse
from taskforge.core.auth import AuthenticatedPrincipal

router = APIRouter(prefix="/auth", tags=["Auth"])


@router.get(
    "/me",
    response_model=AuthPrincipalResponse,
    summary="Inspect authenticated API key principal",
    description="Returns key_id, tenant_id, name, and granted RBAC scopes without echoing secrets.",
)
def get_auth_me(
    principal: AuthenticatedPrincipal = Depends(get_current_principal),
):
    return AuthPrincipalResponse(
        key_id=principal.key_id,
        tenant_id=principal.tenant_id,
        name=principal.name,
        scopes=list(principal.scopes),
    )
