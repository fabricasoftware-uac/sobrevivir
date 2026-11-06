import { useCallback, useEffect, useRef, useState } from 'react';
import { io, type Socket } from 'socket.io-client';
import type { FamilyCommand, FamilyState, StationId } from '@sobrevivir/engine';
import { newOpId } from './router';

export type Session =
  | { role: 'facilitador'; label: string }
  | { role: 'familia'; familyId: string; label: string }
  | { role: 'estacion'; stationId: StationId; label: string };

export interface PresenceEntry {
  role: Session['role'];
  familyId?: string;
  stationId?: string;
  since: string;
}

export interface RoomInfo {
  code: string;
  status: 'activa' | 'pausada';
  familyIds: string[];
  createdAt: string;
  store: string;
  log?: { at: string; actor: string; text: string }[];
  observations?: Record<string, { familyId: string; criteria: Record<string, string>; notes: string; updatedAt: string }>;
}

export interface Snapshot {
  room: RoomInfo;
  session: Session;
  families: FamilyState[];
  presence?: PresenceEntry[];
}

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message);
  }
}

export const SERVER_URL = ((import.meta.env.VITE_SERVER_URL as string | undefined) ?? '').replace(/\/$/, '');

export async function api<T>(method: string, path: string, body?: unknown, key?: string): Promise<T> {
  let res: Response;
  const fullPath = path.startsWith('http') ? path : `${SERVER_URL}${path}`;
  try {
    res = await fetch(fullPath, {
      method,
      headers: { 'content-type': 'application/json', ...(key ? { authorization: `Bearer ${key}` } : {}) },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch {
    throw new ApiError(0, 'red', 'No hay conexión con el servidor.');
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new ApiError(res.status, data.error ?? 'error', data.message ?? 'Error del servidor.');
  return data as T;
}

export interface Health {
  ok: boolean;
  connectedMode: boolean;
  store: string;
  message?: string;
  rulesVersion: string;
}

export async function fetchHealth(): Promise<Health | null> {
  try {
    return await api<Health>('GET', '/api/health');
  } catch {
    return null;
  }
}

// ----------------------------------------------------------------- accesos guardados en este dispositivo

export interface StoredAccess {
  code: string;
  key: string;
  session: Session;
  savedAt: string;
}
export interface StoredRoomKeys {
  code: string;
  keys: { facilitador: string; familias: Record<string, string>; estaciones: Record<string, string> };
}

const ACCESS_KEY = 'sobrevivir:accesos';
const ROOMKEYS_KEY = 'sobrevivir:salas-creadas';

const readJSON = <T,>(k: string, fallback: T): T => {
  try {
    return JSON.parse(localStorage.getItem(k) ?? '') as T;
  } catch {
    return fallback;
  }
};

export function listAccesses(): StoredAccess[] {
  return readJSON<StoredAccess[]>(ACCESS_KEY, []);
}
export function saveAccess(a: StoredAccess) {
  const list = listAccesses().filter((x) => !(x.code === a.code && x.key === a.key));
  list.unshift(a);
  localStorage.setItem(ACCESS_KEY, JSON.stringify(list.slice(0, 20)));
}
export function findAccess(code: string, pred: (s: Session) => boolean) {
  return listAccesses().find((a) => a.code === code && pred(a.session));
}
export function forgetAccess(code: string, key: string) {
  localStorage.setItem(ACCESS_KEY, JSON.stringify(listAccesses().filter((x) => !(x.code === code && x.key === key))));
}
export function saveRoomKeys(r: StoredRoomKeys) {
  const list = readJSON<StoredRoomKeys[]>(ROOMKEYS_KEY, []).filter((x) => x.code !== r.code);
  list.unshift(r);
  localStorage.setItem(ROOMKEYS_KEY, JSON.stringify(list.slice(0, 10)));
}
export function getRoomKeys(code: string): StoredRoomKeys | undefined {
  return readJSON<StoredRoomKeys[]>(ROOMKEYS_KEY, []).find((x) => x.code === code);
}

export function accessLink(code: string, key: string) {
  return `${window.location.origin}${window.location.pathname}#/acceso/${code}/${key}`;
}

// ----------------------------------------------------------------- conexión en vivo

export type ConnectionState = 'conectando' | 'conectado' | 'reconectando' | 'desconectado';

/** Mantiene una instantánea de la sala sincronizada por Socket.IO. El servidor es la autoridad. */
export function useRoom(code: string, key: string | undefined) {
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [connection, setConnection] = useState<ConnectionState>('conectando');
  const socketRef = useRef<Socket | null>(null);

  const refresh = useCallback(async () => {
    if (!key) return;
    try {
      const s = await api<Snapshot>('GET', `/api/rooms/${code}/state`, undefined, key);
      setSnap(s);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [code, key]);

  useEffect(() => {
    if (!key) return;
    refresh();
    const socket = io(SERVER_URL || undefined, { auth: { code, key }, transports: ['websocket', 'polling'] });
    socketRef.current = socket;
    socket.on('connect', () => {
      setConnection('conectado');
      refresh(); // al reconectar se recupera una instantánea válida
    });
    socket.on('disconnect', () => setConnection('reconectando'));
    socket.on('connect_error', () => setConnection('desconectado'));
    socket.io.on('reconnect_attempt', () => setConnection('reconectando'));
    socket.on('family', (state: FamilyState) => {
      setSnap((prev) => {
        if (!prev) return prev;
        const families = prev.families.some((f) => f.id === state.id)
          ? prev.families.map((f) => (f.id === state.id && f.version <= state.version ? state : f))
          : [...prev.families, state];
        return { ...prev, families };
      });
    });
    socket.on('presence', (presence: PresenceEntry[]) => setSnap((prev) => (prev ? { ...prev, presence } : prev)));
    socket.on('room-changed', () => refresh());
    return () => {
      socket.close();
      socketRef.current = null;
    };
  }, [code, key, refresh]);

  /** Envía un comando con identificador de operación; ante fallos de red se reintenta con el mismo identificador. */
  const send = useCallback(
    async (familyId: string, command: FamilyCommand) => {
      const opId = newOpId();
      let lastErr: unknown;
      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          const r = await api<{ state: FamilyState }>('POST', `/api/rooms/${code}/families/${familyId}/commands`, { opId, command }, key);
          setSnap((prev) =>
            prev ? { ...prev, families: prev.families.map((f) => (f.id === familyId && f.version <= r.state.version ? r.state : f)) } : prev,
          );
          return r.state;
        } catch (e) {
          lastErr = e;
          if (!(e instanceof ApiError) || e.status !== 0) break;
          await new Promise((r) => setTimeout(r, 600 * (attempt + 1)));
        }
      }
      if (lastErr instanceof ApiError && lastErr.code === 'conflicto') refresh();
      throw lastErr;
    },
    [code, key, refresh],
  );

  return { snap, error, connection, refresh, send };
}
