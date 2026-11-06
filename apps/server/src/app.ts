import { existsSync } from 'node:fs';
import { createServer } from 'node:http';
import path from 'node:path';
import express, { type NextFunction, type Request, type Response } from 'express';
import cors from 'cors';
import { Server as IOServer } from 'socket.io';
import { z } from 'zod';
import { RULES_VERSION, operationSchema } from '@sobrevivir/engine';
import { GameService, HttpError, toHttpError, type Session } from './service';
import type { Store } from './store/types';

const keyFrom = (req: Request) => {
  const h = req.header('authorization');
  return h?.startsWith('Bearer ') ? h.slice(7).trim() : undefined;
};

type Presence = { socketId: string; role: Session['role']; familyId?: string; stationId?: string; since: string };

export interface AppOptions {
  store: Store | null;
  storeError?: string;
  staticDir?: string;
  corsOrigin?: string;
}

export function createApp(opts: AppOptions) {
  const app = express();
  app.disable('x-powered-by');

  const rawOrigin = opts.corsOrigin ?? process.env.CORS_ORIGIN ?? '*';
  const allowedOrigins = rawOrigin === '*' ? '*' : rawOrigin.split(',').map((s) => s.trim());
  const corsConfig = rawOrigin === '*' ? { origin: '*' } : { origin: allowedOrigins, credentials: true };

  app.use(cors(corsConfig));
  app.use(express.json({ limit: '64kb' }));
  const http = createServer(app);
  const io = new IOServer(http, { cors: corsConfig, serveClient: false });
  const service = opts.store ? new GameService(opts.store) : null;
  const presence = new Map<string, Map<string, Presence>>(); // roomId → socketId → presencia

  const needService = (): GameService => {
    if (!service)
      throw new HttpError(
        503,
        'sin_base_de_datos',
        opts.storeError ??
          'El modo conectado requiere MongoDB. Configure MONGODB_URI en el archivo .env del servidor y reinicie. La demostración local funciona sin servidor.',
      );
    return service;
  };

  const wrap =
    (fn: (req: Request, res: Response) => Promise<unknown>) => async (req: Request, res: Response, next: NextFunction) => {
      try {
        await fn(req, res);
      } catch (e) {
        next(e);
      }
    };

  app.get('/api/health', (_req, res) => {
    res.json({
      ok: true,
      rulesVersion: RULES_VERSION,
      connectedMode: Boolean(service),
      store: service?.store.kind ?? 'sin_configurar',
      message: service ? undefined : opts.storeError ?? 'MongoDB no está configurado.',
    });
  });

  app.post(
    '/api/rooms',
    wrap(async (req, res) => {
      const svc = needService();
      const body = z.object({ families: z.array(z.number().int().min(1).max(6)).min(1).max(6) }).parse(req.body);
      res.status(201).json(await svc.createRoom(body.families));
    }),
  );

  app.post(
    '/api/rooms/:code/session',
    wrap(async (req, res) => {
      const svc = needService();
      const body = z.object({ key: z.string().min(8).max(64) }).parse(req.body);
      const { room, session } = await svc.authenticate(String(req.params.code), body.key);
      res.json({ code: room.code, session });
    }),
  );

  app.get(
    '/api/rooms/:code/state',
    wrap(async (req, res) => {
      const svc = needService();
      const { room, session } = await svc.authenticate(String(req.params.code), keyFrom(req));
      const snap = await svc.snapshot(room, session);
      res.json({ ...snap, presence: session.role === 'familia' ? undefined : presenceOf(room._id) });
    }),
  );

  app.post(
    '/api/rooms/:code/families/:familyId/commands',
    wrap(async (req, res) => {
      const svc = needService();
      const body = operationSchema.parse(req.body);
      const result = await svc.command(String(req.params.code), keyFrom(req), String(req.params.familyId), body.opId, body.command as never);
      res.json(result);
    }),
  );

  app.post(
    '/api/rooms/:code/pause',
    wrap(async (req, res) => {
      const svc = needService();
      const body = z.object({ paused: z.boolean() }).parse(req.body);
      const room = await svc.setPaused(String(req.params.code), keyFrom(req), body.paused);
      res.json({ status: room.status });
    }),
  );

  app.post(
    '/api/rooms/:code/observations',
    wrap(async (req, res) => {
      const svc = needService();
      const body = z
        .object({
          familyId: z.string().max(4),
          criteria: z.record(z.string().max(40), z.string().max(40)),
          notes: z.string().max(2000),
        })
        .parse(req.body);
      await svc.saveObservation(String(req.params.code), keyFrom(req), body);
      res.json({ ok: true });
    }),
  );

  app.use('/api', (_req, res) => res.status(404).json({ error: 'ruta', message: 'Ruta no encontrada.' }));

  if (opts.staticDir && existsSync(opts.staticDir)) {
    app.use(express.static(opts.staticDir, { index: 'index.html', maxAge: '1h' }));
    app.get(/^\/(?!api|socket\.io).*/, (_req, res) => res.sendFile(path.join(opts.staticDir!, 'index.html')));
  }

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    const e = toHttpError(err);
    res.status(e.status).json({ error: e.code, message: e.message });
  });

  // ------------------------------------------------------------------ tiempo real
  function presenceOf(roomId: string) {
    return [...(presence.get(roomId)?.values() ?? [])].map(({ role, familyId, stationId, since }) => ({ role, familyId, stationId, since }));
  }
  const staffRoom = (roomId: string) => `staff:${roomId}`;
  const familyRoom = (roomId: string, fid: string) => `fam:${roomId}:${fid}`;

  if (service) {
    service.notifier = {
      family(roomId, state) {
        io.to(staffRoom(roomId)).to(familyRoom(roomId, state.id)).emit('family', state);
      },
      room(roomId) {
        io.to(staffRoom(roomId)).emit('room-changed');
        for (const p of presence.get(roomId)?.values() ?? [])
          if (p.role === 'familia') io.to(p.socketId).emit('room-changed');
      },
    };

    io.use(async (socket, next) => {
      try {
        const { code, key } = socket.handshake.auth as { code?: string; key?: string };
        const { room, session } = await service.authenticate(String(code ?? ''), key);
        socket.data.roomId = room._id;
        socket.data.session = session;
        next();
      } catch (e) {
        next(new Error(toHttpError(e).message));
      }
    });

    io.on('connection', (socket) => {
      const roomId: string = socket.data.roomId;
      const session: Session = socket.data.session;
      if (session.role === 'familia') socket.join(familyRoom(roomId, session.familyId));
      else socket.join(staffRoom(roomId));
      if (!presence.has(roomId)) presence.set(roomId, new Map());
      presence.get(roomId)!.set(socket.id, {
        socketId: socket.id,
        role: session.role,
        familyId: session.role === 'familia' ? session.familyId : undefined,
        stationId: session.role === 'estacion' ? session.stationId : undefined,
        since: new Date().toISOString(),
      });
      io.to(staffRoom(roomId)).emit('presence', presenceOf(roomId));
      socket.on('disconnect', () => {
        presence.get(roomId)?.delete(socket.id);
        io.to(staffRoom(roomId)).emit('presence', presenceOf(roomId));
      });
    });
  } else {
    io.use((_socket, next) => next(new Error('El modo conectado requiere MongoDB.')));
  }

  return { app, http, io, service };
}
