from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from neo4j import AsyncSession

from icarus.dependencies import CurrentUser, get_session
from icarus.models.investigation import (
    Annotation,
    AnnotationCreate,
    InvestigationCreate,
    InvestigationListResponse,
    InvestigationResponse,
    InvestigationUpdate,
    Tag,
    TagCreate,
)
from icarus.services import investigation_service as svc
from icarus.services.neo4j_service import execute_query_single
from icarus.services.pdf_service import render_investigation_pdf

router = APIRouter(tags=["investigations"])


@router.post(
    "/api/v1/investigations/",
    response_model=InvestigationResponse,
    status_code=201,
)
async def create_investigation(
    body: InvestigationCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentUser,
) -> InvestigationResponse:
    return await svc.create_investigation(session, body.title, body.description, user.id)


@router.get("/api/v1/investigations/", response_model=InvestigationListResponse)
async def list_investigations(
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentUser,
    page: Annotated[int, Query(ge=1)] = 1,
    size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> InvestigationListResponse:
    investigations, total = await svc.list_investigations(session, page, size, user.id)
    return InvestigationListResponse(investigations=investigations, total=total)


@router.get(
    "/api/v1/investigations/{investigation_id}",
    response_model=InvestigationResponse,
)
async def get_investigation(
    investigation_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> InvestigationResponse:
    result = await svc.get_investigation(session, investigation_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return result


@router.patch(
    "/api/v1/investigations/{investigation_id}",
    response_model=InvestigationResponse,
)
async def update_investigation(
    investigation_id: str,
    body: InvestigationUpdate,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentUser,
) -> InvestigationResponse:
    result = await svc.update_investigation(
        session, investigation_id, body.title, body.description, user.id
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return result


@router.delete(
    "/api/v1/investigations/{investigation_id}",
    status_code=204,
)
async def delete_investigation(
    investigation_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentUser,
) -> None:
    deleted = await svc.delete_investigation(session, investigation_id, user.id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Investigation not found")


@router.post(
    "/api/v1/investigations/{investigation_id}/entities/{entity_id}",
    status_code=201,
)
async def add_entity(
    investigation_id: str,
    entity_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentUser,
) -> dict[str, str]:
    added = await svc.add_entity_to_investigation(
        session, investigation_id, entity_id, user.id
    )
    if not added:
        raise HTTPException(status_code=404, detail="Investigation or entity not found")
    return {"investigation_id": investigation_id, "entity_id": entity_id}


@router.post(
    "/api/v1/investigations/{investigation_id}/annotations",
    response_model=Annotation,
    status_code=201,
)
async def create_annotation(
    investigation_id: str,
    body: AnnotationCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentUser,
) -> Annotation:
    return await svc.create_annotation(
        session, investigation_id, body.entity_id, body.text, user.id
    )


@router.get(
    "/api/v1/investigations/{investigation_id}/annotations",
    response_model=list[Annotation],
)
async def list_annotations(
    investigation_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[Annotation]:
    return await svc.list_annotations(session, investigation_id)


@router.post(
    "/api/v1/investigations/{investigation_id}/tags",
    response_model=Tag,
    status_code=201,
)
async def create_tag(
    investigation_id: str,
    body: TagCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentUser,
) -> Tag:
    return await svc.create_tag(session, investigation_id, body.name, body.color, user.id)


@router.get(
    "/api/v1/investigations/{investigation_id}/tags",
    response_model=list[Tag],
)
async def list_tags(
    investigation_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[Tag]:
    return await svc.list_tags(session, investigation_id)


@router.post(
    "/api/v1/investigations/{investigation_id}/share",
    response_model=dict[str, str],
)
async def generate_share_link(
    investigation_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentUser,
) -> dict[str, str]:
    token = await svc.generate_share_token(session, investigation_id, user.id)
    if token is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return {"share_token": token}


@router.get("/api/v1/shared/{token}", response_model=InvestigationResponse)
async def get_shared_investigation(
    token: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> InvestigationResponse:
    result = await svc.get_by_share_token(session, token)
    if result is None:
        raise HTTPException(status_code=404, detail="Shared investigation not found")
    return result


@router.get("/api/v1/investigations/{investigation_id}/export")
async def export_investigation(
    investigation_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> JSONResponse:
    investigation = await svc.get_investigation(session, investigation_id)
    if investigation is None:
        raise HTTPException(status_code=404, detail="Investigation not found")

    annotations = await svc.list_annotations(session, investigation_id)
    tags = await svc.list_tags(session, investigation_id)

    export_data = {
        "investigation": investigation.model_dump(),
        "annotations": [a.model_dump() for a in annotations],
        "tags": [t.model_dump() for t in tags],
    }
    return JSONResponse(content=export_data)


@router.get("/api/v1/investigations/{investigation_id}/export/pdf")
async def export_investigation_pdf(
    investigation_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentUser,
    lang: Annotated[Literal["pt", "en"], Query()] = "pt",
) -> Response:
    investigation = await svc.get_investigation(session, investigation_id)
    if investigation is None:
        raise HTTPException(status_code=404, detail="Investigation not found")

    annotations = await svc.list_annotations(session, investigation_id)
    tags = await svc.list_tags(session, investigation_id)

    entities: list[dict[str, str]] = []
    for entity_id in investigation.entity_ids:
        record = await execute_query_single(session, "entity_by_id", {"id": entity_id})
        if record is not None:
            node = record["e"]
            labels = record["entity_labels"]
            entities.append({
                "name": str(node.get("name", "")),
                "type": labels[0] if labels else "",
                "document": str(node.get("cpf", node.get("cnpj", ""))),
            })

    pdf_bytes = await render_investigation_pdf(
        investigation, annotations, tags, entities, lang=lang
    )

    filename = f"{investigation.title}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
