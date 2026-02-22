const API_BASE = import.meta.env.VITE_API_URL ?? "";
const STORAGE_KEY = "icarus_auth";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

function getAuthHeaders(): Record<string, string> {
  try {
    const token = localStorage.getItem(STORAGE_KEY);
    if (token) return { Authorization: `Bearer ${token}` };
  } catch {
    // localStorage unavailable
  }
  return {};
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const url = `${API_BASE}${path}`;
  const response = await fetch(url, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...getAuthHeaders(),
      ...init?.headers,
    },
  });

  if (!response.ok) {
    throw new ApiError(response.status, `API error: ${response.statusText}`);
  }

  return response.json() as Promise<T>;
}

export interface SearchResult {
  id: string;
  name: string;
  type: string;
  document?: string;
  sources: string[];
  score: number;
}

export interface SearchResponse {
  results: SearchResult[];
  total: number;
  page: number;
  size: number;
}

export interface EntityDetail {
  id: string;
  name: string;
  type: string;
  document?: string;
  properties: Record<string, unknown>;
  sources: string[];
}

export interface GraphNode {
  id: string;
  label: string;
  type: string;
  properties: Record<string, unknown>;
}

export interface GraphEdge {
  source: string;
  target: string;
  type: string;
  properties: Record<string, unknown>;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export function searchEntities(
  query: string,
  type?: string,
  page = 1,
  size = 20,
): Promise<SearchResponse> {
  const params = new URLSearchParams({ q: query, page: String(page), size: String(size) });
  if (type && type !== "all") {
    params.set("type", type);
  }
  return apiFetch<SearchResponse>(`/api/v1/search?${params}`);
}

export function getEntity(id: string): Promise<EntityDetail> {
  return apiFetch<EntityDetail>(`/api/v1/entity/${encodeURIComponent(id)}`);
}

export interface PatternInfo {
  id: string;
  name_pt: string;
  name_en: string;
  description_pt: string;
  description_en: string;
}

export interface PatternListResponse {
  patterns: PatternInfo[];
}

export interface PatternResult {
  pattern_id: string;
  pattern_name: string;
  description: string;
  data: Record<string, unknown>;
  entity_ids: string[];
  sources: { database: string }[];
}

export interface PatternResponse {
  entity_id: string | null;
  patterns: PatternResult[];
  total: number;
}

export function listPatterns(): Promise<PatternListResponse> {
  return apiFetch<PatternListResponse>("/api/v1/patterns/");
}

export function getEntityPatterns(
  entityId: string,
  lang = "pt",
): Promise<PatternResponse> {
  const params = new URLSearchParams({ lang });
  return apiFetch<PatternResponse>(
    `/api/v1/patterns/${encodeURIComponent(entityId)}?${params}`,
  );
}

export function getGraphData(
  entityId: string,
  depth = 1,
  types?: string[],
): Promise<GraphData> {
  const params = new URLSearchParams({ depth: String(depth) });
  if (types?.length) {
    params.set("types", types.join(","));
  }
  return apiFetch<GraphData>(`/api/v1/graph/${encodeURIComponent(entityId)}?${params}`);
}

// --- Investigations ---

export interface Investigation {
  id: string;
  title: string;
  description: string;
  created_at: string;
  updated_at: string;
  entity_ids: string[];
  share_token: string | null;
}

export interface InvestigationListResponse {
  investigations: Investigation[];
  total: number;
}

export interface Annotation {
  id: string;
  entity_id: string;
  investigation_id: string;
  text: string;
  created_at: string;
}

export interface Tag {
  id: string;
  investigation_id: string;
  name: string;
  color: string;
}

export function listInvestigations(
  page = 1,
  size = 20,
): Promise<InvestigationListResponse> {
  const params = new URLSearchParams({ page: String(page), size: String(size) });
  return apiFetch<InvestigationListResponse>(`/api/v1/investigations?${params}`);
}

export function getInvestigation(id: string): Promise<Investigation> {
  return apiFetch<Investigation>(`/api/v1/investigations/${encodeURIComponent(id)}`);
}

export function createInvestigation(
  title: string,
  description?: string,
): Promise<Investigation> {
  return apiFetch<Investigation>("/api/v1/investigations", {
    method: "POST",
    body: JSON.stringify({ title, description: description ?? "" }),
  });
}

export function updateInvestigation(
  id: string,
  data: { title?: string; description?: string },
): Promise<Investigation> {
  return apiFetch<Investigation>(
    `/api/v1/investigations/${encodeURIComponent(id)}`,
    { method: "PATCH", body: JSON.stringify(data) },
  );
}

export function deleteInvestigation(id: string): Promise<void> {
  return apiFetch<void>(`/api/v1/investigations/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
}

export function addEntityToInvestigation(
  investigationId: string,
  entityId: string,
): Promise<void> {
  return apiFetch<void>(
    `/api/v1/investigations/${encodeURIComponent(investigationId)}/entities/${encodeURIComponent(entityId)}`,
    { method: "POST" },
  );
}

export function listAnnotations(investigationId: string): Promise<Annotation[]> {
  return apiFetch<Annotation[]>(
    `/api/v1/investigations/${encodeURIComponent(investigationId)}/annotations`,
  );
}

export function createAnnotation(
  investigationId: string,
  entityId: string,
  text: string,
): Promise<Annotation> {
  return apiFetch<Annotation>(
    `/api/v1/investigations/${encodeURIComponent(investigationId)}/annotations`,
    { method: "POST", body: JSON.stringify({ entity_id: entityId, text }) },
  );
}

export function listTags(investigationId: string): Promise<Tag[]> {
  return apiFetch<Tag[]>(
    `/api/v1/investigations/${encodeURIComponent(investigationId)}/tags`,
  );
}

export function createTag(
  investigationId: string,
  name: string,
  color?: string,
): Promise<Tag> {
  return apiFetch<Tag>(
    `/api/v1/investigations/${encodeURIComponent(investigationId)}/tags`,
    { method: "POST", body: JSON.stringify({ name, color: color ?? "#e07a2f" }) },
  );
}

export function generateShareLink(
  investigationId: string,
): Promise<{ share_token: string }> {
  return apiFetch<{ share_token: string }>(
    `/api/v1/investigations/${encodeURIComponent(investigationId)}/share`,
    { method: "POST" },
  );
}

export function exportInvestigation(investigationId: string): Promise<Blob> {
  const url = `${API_BASE}/api/v1/investigations/${encodeURIComponent(investigationId)}/export`;
  return fetch(url, { headers: getAuthHeaders() }).then((res) => {
    if (!res.ok) throw new ApiError(res.status, `API error: ${res.statusText}`);
    return res.blob();
  });
}

export function exportInvestigationPDF(
  investigationId: string,
  lang = "pt",
): Promise<Blob> {
  const params = new URLSearchParams({ lang });
  const url = `${API_BASE}/api/v1/investigations/${encodeURIComponent(investigationId)}/export/pdf?${params}`;
  return fetch(url, { headers: getAuthHeaders() }).then((res) => {
    if (!res.ok) throw new ApiError(res.status, `API error: ${res.statusText}`);
    return res.blob();
  });
}
