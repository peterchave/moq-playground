# MOQT Playground

Browser-based test harness for exercising MOQT draft-ietf-moq-transport-16 control and data flows over WebTransport.

The app provides separate publisher and subscriber panels so you can connect to a relay, publish namespaces and tracks, subscribe, fetch, inspect object flow, and watch the connection visualization update as messages move across control and data streams.

## Features

- Publisher and subscriber sessions in one page
- Namespace publish and subscribe flows
- Track publish, subscribe, and fetch operations
- Object inspectors for subscriber and fetch data
- Connection and stream activity visualization
- Draft-16 focused transport and WebTransport implementations included locally in `src/transport` and `src/webtransport`

## Requirements

- Node.js 18 or newer
- pnpm
- A MOQT relay that supports WebTransport
- A browser with WebTransport support

## Development

Install dependencies:

```bash
pnpm install
```

Start the dev server:

```bash
pnpm dev
```

Run a typecheck:

```bash
pnpm check
```

Create a production build:

```bash
pnpm build
```

Preview the production build locally:

```bash
pnpm preview
```

## Project Notes

- The `@moqt/transport` and `@moqt/webtransport` imports are resolved through local aliases in `vite.config.ts` and `tsconfig.json`; they are implemented inside this repository.
- `dist/` is generated output and is intentionally ignored.
- No environment variables are required for the current app.

## Publishing Checklist

- Review `index.html` and the default relay placeholders before sharing screenshots or demos.
- Keep `node_modules/`, `dist/`, local env files, and OS/editor artifacts out of Git.
- Choose and add a license if you intend to open-source the project.
