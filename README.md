# Secure Chat - End-to-End Encrypted Messaging

A modern web-based messaging platform with **zero-knowledge architecture** - the server never sees your message content. All encryption happens locally in your browser using OpenPGP.js.

## Features

- **End-to-End Encryption**: Messages encrypted with PGP in your browser before transmission
- **Zero-Knowledge Server**: Server only routes encrypted data, cannot read messages
- **No Registration Required**: Temporary session-based user IDs, no personal data collected
- **Friend Request System**: Secure method to establish trusted communication channels
- **Real-Time Messaging**: Instant delivery via WebSocket connections
- **Modern Dark UI**: Clean, terminal-inspired interface optimized for all devices

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the Server

```bash
python server.py
```

The server will start on `ws://0.0.0.0:8765`

### 3. Open the Client

Simply open `index.html` in any modern web browser:

```bash
# On Windows
start index.html

# On macOS
open index.html

# On Linux
xdg-open index.html
```

Or serve it via a simple HTTP server:

```bash
python -m http.server 8080
```

Then navigate to `http://localhost:8080`

## How It Works

1. **Key Generation**: When you first connect, the app generates a PGP key pair in your browser
2. **Friend Requests**: Exchange your User ID with someone to send them a friend request
3. **Secure Messaging**: All messages are encrypted with the recipient's public key before being sent
4. **Decryption**: Only the recipient's private key can decrypt and read the message

## Security Architecture

- **Client-Side Encryption**: OpenPGP.js performs all cryptographic operations in the browser
- **Private Key Protection**: Your private key never leaves your device
- **Passphrase Support**: Optional passphrase protection for your private key
- **No Message Storage**: Server only routes messages, doesn't store content
- **TLS Recommended**: For production, use WSS (WebSocket Secure) connections

## Project Structure

```
secure-chat/
├── server.py          # Python WebSocket server
├── index.html         # Web client with encryption
├── requirements.txt   # Python dependencies
└── README.md          # This file
```

## Production Deployment

### Using WSS (Secure WebSockets)

For production, you should use TLS/SSL:

```python
import ssl
import asyncio
import websockets

ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ssl_context.load_cert_chain('cert.pem', 'key.pem')

async with websockets.serve(handle_client, "0.0.0.0", 8765, ssl=ssl_context):
    await asyncio.Future()
```

### Environment Variables

You can configure the server with environment variables:

```bash
export SECURE_CHAT_HOST=0.0.0.0
export SECURE_CHAT_PORT=8765
export SECURE_CHAT_SSL=true
```

## Browser Compatibility

- Chrome 80+
- Firefox 75+
- Safari 13.1+
- Edge 80+

## License

MIT License - Free for personal and commercial use.
