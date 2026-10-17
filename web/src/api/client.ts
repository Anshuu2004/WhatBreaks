import type {
  ApiError,
  DiscoverySyncResponse,
  ImpactAnswer,
  ImpactRequest,
  ListResourcesResponse,
  ResourceDetailResponse,
  ResourceIdentity,
} from "../types";

export const API_BASE_URL =
  (import.meta.env.VITE_API_URL as string) || "";

export const DEFAULT_WORKSPACE_ID =
  (import.meta.env.VITE_WORKSPACE_ID as string) ||
  "00000000-0000-0000-0000-000000000001";

let cachedCSRFToken: string | null = null;

export async function fetchCSRFToken(): Promise<string> {
  try {
    const res = await fetch(`${API_BASE_URL}/api/csrf-token`, {
      method: "GET",
      credentials: "include",
    });
    if (res.ok) {
      const data = await res.json();
      if (data && data.csrfToken) {
        cachedCSRFToken = data.csrfToken;
        return data.csrfToken;
      }
    }
  } catch (err) {
    console.warn("Could not fetch CSRF token from API server", err);
  }
  return "";
}

async function handleResponse<T>(res: Response): Promise<T> {
  if (!res.ok) {
    let errBody: ApiError = {
      error: `HTTP error ${res.status}: ${res.statusText}`,
      code: "HTTP_ERROR",
    };
    try {
      const json = await res.json();
      if (json && json.error) {
        errBody = json;
      }
    } catch {
      // Body not JSON
    }
    const error = new Error(errBody.error);
    (error as unknown as { code: string; status: number }).code = errBody.code;
    (error as unknown as { code: string; status: number }).status = res.status;
    throw error;
  }
  return (await res.json()) as T;
}

export async function checkHealth(): Promise<{ status: string; environment?: string }> {
  const res = await fetch(`${API_BASE_URL}/health`, {
    method: "GET",
    credentials: "include",
  });
  return handleResponse<{ status: string; environment?: string }>(res);
}

export async function getSystemHealth(): Promise<import("../types").SystemHealthResponse> {
  const res = await fetch(`${API_BASE_URL}/api/v1/system/health`, {
    method: "GET",
    credentials: "include",
  });
  return handleResponse<import("../types").SystemHealthResponse>(res);
}

export async function listClusters(
  workspaceId: string = DEFAULT_WORKSPACE_ID
): Promise<import("../types").ListClustersResponse> {
  const params = new URLSearchParams({ workspace_id: workspaceId });
  const res = await fetch(`${API_BASE_URL}/api/v1/clusters?${params.toString()}`, {
    method: "GET",
    headers: {
      "X-Workspace-ID": workspaceId,
    },
    credentials: "include",
  });
  return handleResponse<import("../types").ListClustersResponse>(res);
}

export async function listResources(
  workspaceId: string = DEFAULT_WORKSPACE_ID,
  filter?: { type?: string; search?: string; cluster?: string }
): Promise<ListResourcesResponse> {
  const params = new URLSearchParams();
  params.set("workspace_id", workspaceId);
  if (filter?.type && filter.type !== "all") {
    params.set("type", filter.type);
  }
  if (filter?.search) {
    params.set("search", filter.search.trim());
  }
  if (filter?.cluster && filter.cluster !== "all") {
    params.set("cluster", filter.cluster.trim());
  }

  const res = await fetch(`${API_BASE_URL}/api/v1/resources?${params.toString()}`, {
    method: "GET",
    headers: {
      "X-Workspace-ID": workspaceId,
    },
    credentials: "include",
  });

  return handleResponse<ListResourcesResponse>(res);
}

export async function listDependencies(
  workspaceId: string = DEFAULT_WORKSPACE_ID,
  cluster?: string
): Promise<import("../types").ListDependenciesResponse> {
  const params = new URLSearchParams();
  params.set("workspace_id", workspaceId);
  if (cluster && cluster !== "all") {
    params.set("cluster", cluster.trim());
  }

  const res = await fetch(`${API_BASE_URL}/api/v1/dependencies?${params.toString()}`, {
    method: "GET",
    headers: {
      "X-Workspace-ID": workspaceId,
    },
    credentials: "include",
  });

  return handleResponse<import("../types").ListDependenciesResponse>(res);
}

export async function getResourceDetail(
  workspaceId: string = DEFAULT_WORKSPACE_ID,
  identity: ResourceIdentity
): Promise<ResourceDetailResponse> {
  const params = new URLSearchParams({
    workspace_id: workspaceId,
    provider: identity.provider,
    resource_type: identity.resource_type,
    provider_id: identity.provider_id,
  });

  const res = await fetch(`${API_BASE_URL}/api/v1/resources/detail?${params.toString()}`, {
    method: "GET",
    headers: {
      "X-Workspace-ID": workspaceId,
    },
    credentials: "include",
  });

  return handleResponse<ResourceDetailResponse>(res);
}

export async function syncDiscovery(
  workspaceId: string = DEFAULT_WORKSPACE_ID
): Promise<DiscoverySyncResponse> {
  if (!cachedCSRFToken) {
    await fetchCSRFToken();
  }

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-Workspace-ID": workspaceId,
  };
  if (cachedCSRFToken) {
    headers["X-CSRF-Token"] = cachedCSRFToken;
  }

  const res = await fetch(`${API_BASE_URL}/api/v1/discovery/sync?workspace_id=${workspaceId}`, {
    method: "POST",
    headers,
    credentials: "include",
  });

  return handleResponse<DiscoverySyncResponse>(res);
}

export async function analyzeImpact(
  request: ImpactRequest
): Promise<ImpactAnswer> {
  if (!cachedCSRFToken) {
    await fetchCSRFToken();
  }

  const wsID = request.workspace_id || DEFAULT_WORKSPACE_ID;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-Workspace-ID": wsID,
  };
  if (cachedCSRFToken) {
    headers["X-CSRF-Token"] = cachedCSRFToken;
  }

  const res = await fetch(`${API_BASE_URL}/api/v1/impact`, {
    method: "POST",
    headers,
    credentials: "include",
    body: JSON.stringify(request),
  });

  return handleResponse<ImpactAnswer>(res);
}
