import asyncio
import websockets
import json
import secrets
import string
import os
from datetime import datetime
from typing import Optional
import redis.asyncio as redis
import asyncpg

# Redis client for shared state
redis_client: Optional[redis.Redis] = None

# PostgreSQL pool
db_pool: Optional[asyncpg.Pool] = None

# Local WebSocket connections: {user_id: websocket}
# This stays in-memory per instance
local_clients = {}

# Generate random 6-character user ID
def generate_user_id():
    return ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(6))

async def init_db():
    """Initialize PostgreSQL connection pool and create tables if not exist"""
    global db_pool
    db_url = os.environ.get('DATABASE_URL')
    if db_url:
        db_pool = await asyncpg.create_pool(db_url)
        async with db_pool.acquire() as conn:
            # Create tables
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id VARCHAR(6) PRIMARY KEY,
                    public_key TEXT NOT NULL,
                    display_name VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS friendships (
                    user_id VARCHAR(6),
                    friend_id VARCHAR(6),
                    created_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (user_id, friend_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (friend_id) REFERENCES users(user_id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    from_id VARCHAR(6),
                    to_id VARCHAR(6),
                    encrypted_message TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    delivered BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (from_id) REFERENCES users(user_id),
                    FOREIGN KEY (to_id) REFERENCES users(user_id)
                )
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_messages_to_id ON messages(to_id, delivered)
            ''')

async def init_redis():
    """Initialize Redis connection"""
    global redis_client
    redis_url = os.environ.get('REDIS_URL')
    if redis_url:
        redis_client = redis.from_url(redis_url, decode_responses=True)
    else:
        # Fallback for local development
        redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

async def register_user(websocket, data):
    """Register a new user or reconnect existing user"""
    existing_user_id = data.get('user_id')
    
    # If reconnecting
    if existing_user_id and await redis_client.exists(f'user:{existing_user_id}'):
        user_id = existing_user_id
        
        # Update Redis with new connection info
        await redis_client.hset(f'user:{user_id}', mapping={
            'public_key': data.get('public_key', ''),
            'display_name': data.get('display_name', f'User-{user_id}'),
            'status': 'online',
            'instance_id': os.environ.get('RENDER_INSTANCE_ID', 'local')
        })
        
        # Store websocket locally
        local_clients[user_id] = websocket
        
        return {
            'type': 'registered',
            'user_id': user_id,
            'reconnected': True,
            'message': 'User reconnected successfully'
        }
    
    # Generate new user
    user_id = generate_user_id()
    
    # Store in Redis
    await redis_client.hset(f'user:{user_id}', mapping={
        'public_key': data.get('public_key', ''),
        'display_name': data.get('display_name', f'User-{user_id}'),
        'status': 'online',
        'instance_id': os.environ.get('RENDER_INSTANCE_ID', 'local')
    })
    
    # Store in PostgreSQL
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, public_key, display_name)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id) DO UPDATE SET
                    public_key = EXCLUDED.public_key,
                    display_name = EXCLUDED.display_name
            ''', user_id, data.get('public_key', ''), data.get('display_name', f'User-{user_id}'))
    
    # Store websocket locally
    local_clients[user_id] = websocket
    
    return {
        'type': 'registered',
        'user_id': user_id,
        'reconnected': False,
        'message': 'User registered successfully'
    }

async def send_friend_request(user_id, data):
    """Send a friend request to another user"""
    target_id = data.get('target_id')
    
    # Check if target exists in Redis
    if not await redis_client.exists(f'user:{target_id}'):
        return {'type': 'error', 'message': 'User not found'}
    
    if target_id == user_id:
        return {'type': 'error', 'message': 'Cannot add yourself'}
    
    # Check if already friends in Redis
    if await redis_client.sismember(f'friends:{user_id}', target_id):
        return {'type': 'error', 'message': 'Already friends'}
    
    # Add to pending requests in Redis
    await redis_client.sadd(f'pending:{target_id}', user_id)
    
    # Notify target if online
    target_status = await redis_client.hget(f'user:{target_id}', 'status')
    if target_status == 'online':
        await notify_user(target_id, {
            'type': 'friend_request',
            'from_id': user_id,
            'from_name': await redis_client.hget(f'user:{user_id}', 'display_name'),
            'public_key': await redis_client.hget(f'user:{user_id}', 'public_key')
        })
    
    return {
        'type': 'friend_request_sent',
        'target_id': target_id,
        'message': 'Friend request sent'
    }

async def accept_friend_request(user_id, data):
    """Accept a pending friend request"""
    requester_id = data.get('requester_id')
    
    # Check if request exists
    if not await redis_client.sismember(f'pending:{user_id}', requester_id):
        return {'type': 'error', 'message': 'No pending request from this user'}
    
    # Remove from pending
    await redis_client.srem(f'pending:{user_id}', requester_id)
    
    # Add to both users' friend sets in Redis
    await redis_client.sadd(f'friends:{user_id}', requester_id)
    await redis_client.sadd(f'friends:{requester_id}', user_id)
    
    # Persist to PostgreSQL
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO friendships (user_id, friend_id)
                VALUES ($1, $2), ($2, $1)
                ON CONFLICT DO NOTHING
            ''', user_id, requester_id)
    
    # Notify both users
    requester_data = await redis_client.hgetall(f'user:{requester_id}')
    user_data = await redis_client.hgetall(f'user:{user_id}')
    
    await notify_user(user_id, {
        'type': 'friend_accepted',
        'user_id': requester_id,
        'display_name': requester_data.get('display_name', ''),
        'public_key': requester_data.get('public_key', '')
    })
    
    await notify_user(requester_id, {
        'type': 'friend_accepted',
        'user_id': user_id,
        'display_name': user_data.get('display_name', ''),
        'public_key': user_data.get('public_key', '')
    })
    
    return {'type': 'success', 'message': 'Friend request accepted'}

async def reject_friend_request(user_id, data):
    """Reject a pending friend request"""
    requester_id = data.get('requester_id')
    await redis_client.srem(f'pending:{user_id}', requester_id)
    
    await notify_user(requester_id, {
        'type': 'friend_rejected',
        'user_id': user_id
    })
    
    return {'type': 'success', 'message': 'Friend request rejected'}

async def send_message(user_id, data):
    """Route an encrypted message to the recipient"""
    target_id = data.get('target_id')
    encrypted_message = data.get('encrypted_message')
    
    # Check if target exists
    if not await redis_client.exists(f'user:{target_id}'):
        return {'type': 'error', 'message': 'Recipient not found'}
    
    # Check if friends
    if not await redis_client.sismember(f'friends:{user_id}', target_id):
        return {'type': 'error', 'message': 'Not friends with this user'}
    
    timestamp = datetime.now().isoformat()
    
    # Persist message to PostgreSQL
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO messages (from_id, to_id, encrypted_message, timestamp)
                VALUES ($1, $2, $3, $4)
            ''', user_id, target_id, encrypted_message, timestamp)
    
    message_data = {
        'type': 'message',
        'from_id': user_id,
        'from_name': await redis_client.hget(f'user:{user_id}', 'display_name'),
        'encrypted_message': encrypted_message,
        'timestamp': timestamp
    }
    
    # Try to deliver immediately if online
    target_status = await redis_client.hget(f'user:{target_id}', 'status')
    if target_status == 'online':
        await notify_user(target_id, message_data)
    
    return {'type': 'message_sent', 'message': 'Message delivered'}

async def get_friends_list(user_id):
    """Get list of friends for a user"""
    friend_ids = await redis_client.smembers(f'friends:{user_id}')
    friend_list = []
    
    for friend_id in friend_ids:
        friend_data = await redis_client.hgetall(f'user:{friend_id}')
        if friend_data:
            friend_list.append({
                'user_id': friend_id,
                'display_name': friend_data.get('display_name', ''),
                'status': friend_data.get('status', 'offline'),
                'public_key': friend_data.get('public_key', '')
            })
    
    return {
        'type': 'friends_list',
        'friends': friend_list
    }

async def get_pending_requests(user_id):
    """Get pending friend requests for a user"""
    requester_ids = await redis_client.smembers(f'pending:{user_id}')
    requests = []
    
    for requester_id in requester_ids:
        requester_data = await redis_client.hgetall(f'user:{requester_id}')
        if requester_data:
            requests.append({
                'user_id': requester_id,
                'display_name': requester_data.get('display_name', ''),
                'public_key': requester_data.get('public_key', '')
            })
    
    return {
        'type': 'pending_requests',
        'requests': requests
    }

async def notify_user(user_id, message):
    """Send a message to a user via their WebSocket connection"""
    # Check if user is connected to this instance
    if user_id in local_clients:
        try:
            await local_clients[user_id].send(json.dumps(message))
        except websockets.exceptions.ConnectionClosed:
            pass
    else:
        # Publish to Redis for other instances
        await redis_client.publish(f'user_channel:{user_id}', json.dumps(message))

async def handle_redis_messages():
    """Listen for messages from Redis Pub/Sub for users on other instances"""
    pubsub = redis_client.pubsub()
    
    # Subscribe to channels for all local users
    channels = [f'user_channel:{user_id}' for user_id in local_clients.keys()]
    if channels:
        await pubsub.subscribe(*channels)
    
    async for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                # Extract user_id from channel name
                channel = message['channel']
                user_id = channel.replace('user_channel:', '')
                
                if user_id in local_clients:
                    await local_clients[user_id].send(json.dumps(data))
            except Exception:
                pass

async def handle_client(websocket):
    """Main WebSocket handler"""
    user_id = None
    pubsub_task = None
    
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                action = data.get('action')
                
                if action == 'register':
                    response = await register_user(websocket, data)
                    user_id = response.get('user_id')
                    await websocket.send(json.dumps(response))
                    
                    # Start listening for Redis messages for this user
                    asyncio.create_task(handle_redis_messages())
                    
                    # Load and send queued messages from PostgreSQL
                    if db_pool and user_id:
                        async with db_pool.acquire() as conn:
                            rows = await conn.fetch('''
                                SELECT from_id, encrypted_message, timestamp
                                FROM messages
                                WHERE to_id = $1 AND delivered = FALSE
                                ORDER BY timestamp
                            ''', user_id)
                            
                            for row in rows:
                                await websocket.send(json.dumps({
                                    'type': 'message',
                                    'from_id': row['from_id'],
                                    'from_name': await redis_client.hget(f'user:{row["from_id"]}', 'display_name') or 'Unknown',
                                    'encrypted_message': row['encrypted_message'],
                                    'timestamp': row['timestamp'].isoformat()
                                }))
                            
                            # Mark as delivered
                            await conn.execute('''
                                UPDATE messages
                                SET delivered = TRUE
                                WHERE to_id = $1 AND delivered = FALSE
                            ''', user_id)
                
                elif action == 'friend_request':
                    if not user_id:
                        await websocket.send(json.dumps({'type': 'error', 'message': 'Not registered'}))
                        continue
                    response = await send_friend_request(user_id, data)
                    await websocket.send(json.dumps(response))
                
                elif action == 'accept_friend':
                    if not user_id:
                        await websocket.send(json.dumps({'type': 'error', 'message': 'Not registered'}))
                        continue
                    response = await accept_friend_request(user_id, data)
                    await websocket.send(json.dumps(response))
                
                elif action == 'reject_friend':
                    if not user_id:
                        await websocket.send(json.dumps({'type': 'error', 'message': 'Not registered'}))
                        continue
                    response = await reject_friend_request(user_id, data)
                    await websocket.send(json.dumps(response))
                
                elif action == 'send_message':
                    if not user_id:
                        await websocket.send(json.dumps({'type': 'error', 'message': 'Not registered'}))
                        continue
                    response = await send_message(user_id, data)
                    await websocket.send(json.dumps(response))
                
                elif action == 'get_friends':
                    if not user_id:
                        await websocket.send(json.dumps({'type': 'error', 'message': 'Not registered'}))
                        continue
                    response = await get_friends_list(user_id)
                    await websocket.send(json.dumps(response))
                
                elif action == 'get_pending_requests':
                    if not user_id:
                        await websocket.send(json.dumps({'type': 'error', 'message': 'Not registered'}))
                        continue
                    response = await get_pending_requests(user_id)
                    await websocket.send(json.dumps(response))
                
                elif action == 'ping':
                    await websocket.send(json.dumps({'type': 'pong'}))
                
                else:
                    await websocket.send(json.dumps({'type': 'error', 'message': 'Unknown action'}))
                    
            except json.JSONDecodeError:
                await websocket.send(json.dumps({'type': 'error', 'message': 'Invalid JSON'}))
                
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        # Clean up on disconnect
        if user_id:
            if user_id in local_clients:
                del local_clients[user_id]
            
            # Update status in Redis
            await redis_client.hset(f'user:{user_id}', 'status', 'offline')
            
            # Notify friends
            friend_ids = await redis_client.smembers(f'friends:{user_id}')
            for friend_id in friend_ids:
                await notify_user(friend_id, {
                    'type': 'status_update',
                    'user_id': user_id,
                    'status': 'offline'
                })

async def main():
    # Initialize connections
    await init_redis()
    await init_db()
    
    port = int(os.environ.get('PORT', 8765))
    
    print(f"Starting Secure Chat Server on port {port}")
    print(f"Redis: {'Connected' if redis_client else 'Not configured'}")
    print(f"PostgreSQL: {'Connected' if db_pool else 'Not configured'}")
    
    async with websockets.serve(handle_client, "0.0.0.0", port):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
