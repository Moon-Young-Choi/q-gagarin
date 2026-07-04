# q-gagarin

Upbit API Node.js playground for REST and WebSocket examples.

## Requirements

- nvm
- Node.js 22
- npm

## Setup

```bash
nvm use
npm install
```

## Scripts

```bash
npm run ticker
npm run ticker:ws
npm run check
```

The default market is `KRW-BTC`. Override it with `UPBIT_MARKET`:

```bash
UPBIT_MARKET=KRW-ETH npm run ticker
UPBIT_MARKET=KRW-ETH npm run ticker:ws
```

These examples use Upbit public quotation APIs, so no API keys are required.
