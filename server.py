import asyncio
import websockets
import json
import secrets
import string
import os
import http
from datetime import datetime
from typing import Optional
from collections import defaultdict

# Try to import Redis, but make it optional
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

# Try to import asyncpg, but make it optional
try:
    import asyncpg
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    asyncpg = None

# Redis client for shared state (optional)
redis_client = None

# PostgreSQL pool (optional)
db_pool = None

# Local WebSocket connections: {user_id: websocket}
local_clients = {}

# In-memory fallback storage (used when Redis is not available)
memory_users = {}  # {user_id: {public_key, display_name, status}}
memory_friends = defaultdict(set)  # {user_id: set(friend_ids)}
memory_pending = defaultdict(set)  # {user_id: set(requester_ids)}
memory_messages = defaultdict(list)  # {user_id: [message_data]}

def is_redis_connected():
    """Check if Redis is available and connected"""
    return REDIS_AVAILABLE and redis_client is not None

def is_postgres_connected():
    """Check if PostgreSQL is available and connected"""
    return POSTGRES_AVAILABLE and db_pool is not None

# Generate random 6-character user ID
def generate_user_id():
    return ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(6))

async def init_db():
    """Initialize PostgreSQL connection pool and create tables if not exist"""
    global db_pool
    if not POSTGRES_AVAILABLE:
        print("PostgreSQL not available (asyncpg not installed)")
        return
    
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("PostgreSQL: Not configured (no DATABASE_URL)")
        return
    
    try:
        db_pool = await asyncpg.create_pool(db_url)
        async with db_pool.acquire() as conn:
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
        print("PostgreSQL: Connected")
    except Exception as e:
        print(f"PostgreSQL: Connection failed - {e}")
        db_pool = None

async def init_redis():
    """Initialize Redis connection"""
    global redis_client
    if not REDIS_AVAILABLE:
        print("Redis not available (redis package not installed)")
        return
    
    redis_url = os.environ.get('REDIS_URL')
    try:
        if redis_url:
            redis_client = redis.from_url(redis_url, decode_responses=True)
        else:
            redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        await redis_client.ping()
        print("Redis: Connected")
    except Exception as e:
        print(f"Redis: Connection failed - {e}")
        print("Falling back to in-memory storage")
        redis_client = None

# Storage abstraction functions
async def user_exists(user_id):
    if is_redis_connected():
        return await redis_client.exists(f'user:{user_id}')
    return user_id in memory_users

async def get_user(user_id):
    if is_redis_connected():
        return await redis_client.hgetall(f'user:{user_id}')
    return memory_users.get(user_id, {})

async def set_user(user_id, data):
    if is_redis_connected():
        await redis_client.hset(f'user:{user_id}', mapping=data)
    else:
        memory_users[user_id] = data

async def is_friend(user_id, friend_id):
    if is_redis_connected():
        return await redis_client.sismember(f'friends:{user_id}', friend_id)
    return friend_id in memory_friends[user_id]

async def add_friend(user_id, friend_id):
    if is_redis_connected():
        await redis_client.sadd(f'friends:{user_id}', friend_id)
        await redis_client.sadd(f'friends:{friend_id}', user_id)
    else:
        memory_friends[user_id].add(friend_id)
        memory_friends[friend_id].add(user_id)

async def get_friends(user_id):
    if is_redis_connected():
        return await redis_client.smembers(f'friends:{user_id}')
    return memory_friends[user_id]

async def add_pending(to_user, from_user):
    if is_redis_connected():
        await redis_client.sadd(f'pending:{to_user}', from_user)
    else:
        memory_pending[to_user].add(from_user)

async def remove_pending(to_user, from_user):
    if is_redis_connected():
        await redis_client.srem(f'pending:{to_user}', from_user)
    else:
        memory_pending[to_user].discard(from_user)

async def is_pending(to_user, from_user):
    if is_redis_connected():
        return await redis_client.sismember(f'pending:{to_user}', from_user)
    return from_user in memory_pending[to_user]

async def get_pending(user_id):
    if is_redis_connected():
        return await redis_client.smembers(f'pending:{user_id}')
    return memory_pending[user_id]

async def queue_message(to_user, message_data):
    if is_redis_connected():
        await redis_client.lpush(f'messages:{to_user}', json.dumps(message_data))
    else:
        memory_messages[to_user].append(message_data)

async def get_queued_messages(user_id):
    if is_redis_connected():
        messages = []
        while True:
            msg = await redis_client.rpop(f'messages:{user_id}')
            if msg is None:
                break
            messages.append(json.loads(msg))
        return messages
    msgs = memory_messages[user_id][:]
    memory_messages[user_id] = []
    return msgs

async def register_user(websocket, data):
    """Register a new user or reconnect existing user"""
    existing_user_id = data.get('user_id')
    
    # If reconnecting
    if existing_user_id and await user_exists(existing_user_id):
        user_id = existing_user_id
        
        # Update user data
        await set_user(user_id, {
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
    
    # Store user data
    await set_user(user_id, {
        'public_key': data.get('public_key', ''),
        'display_name': data.get('display_name', f'User-{user_id}'),
        'status': 'online',
        'instance_id': os.environ.get('RENDER_INSTANCE_ID', 'local')
    })
    
    # Persist to PostgreSQL if available
    if is_postgres_connected():
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
    
    # Check if target exists
    if not await user_exists(target_id):
        return {'type': 'error', 'message': 'User not found'}
    
    if target_id == user_id:
        return {'type': 'error', 'message': 'Cannot add yourself'}
    
    # Check if already friends
    if await is_friend(user_id, target_id):
        return {'type': 'error', 'message': 'Already friends'}
    
    # Add to pending requests
    await add_pending(target_id, user_id)
    
    # Notify target if online
    target_data = await get_user(target_id)
    if target_data.get('status') == 'online':
        user_data = await get_user(user_id)
        await notify_user(target_id, {
            'type': 'friend_request',
            'from_id': user_id,
            'from_name': user_data.get('display_name', ''),
            'public_key': user_data.get('public_key', '')
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
    if not await is_pending(user_id, requester_id):
        return {'type': 'error', 'message': 'No pending request from this user'}
    
    # Remove from pending
    await remove_pending(user_id, requester_id)
    
    # Add to both users' friend sets
    await add_friend(user_id, requester_id)
    
    # Persist to PostgreSQL if available
    if is_postgres_connected():
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO friendships (user_id, friend_id)
                VALUES ($1, $2), ($2, $1)
                ON CONFLICT DO NOTHING
            ''', user_id, requester_id)
    
    # Notify both users
    requester_data = await get_user(requester_id)
    user_data = await get_user(user_id)
    
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
    await remove_pending(user_id, requester_id)
    
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
    if not await user_exists(target_id):
        return {'type': 'error', 'message': 'Recipient not found'}
    
    # Check if friends
    if not await is_friend(user_id, target_id):
        return {'type': 'error', 'message': 'Not friends with this user'}
    
    timestamp = datetime.now().isoformat()
    user_data = await get_user(user_id)
    
    message_data = {
        'type': 'message',
        'from_id': user_id,
        'from_name': user_data.get('display_name', ''),
        'encrypted_message': encrypted_message,
        'timestamp': timestamp
    }
    
    # Persist to PostgreSQL if available
    if is_postgres_connected():
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO messages (from_id, to_id, encrypted_message, timestamp)
                VALUES ($1, $2, $3, $4)
            ''', user_id, target_id, encrypted_message, timestamp)
    else:
        # Queue for offline delivery
        await queue_message(target_id, message_data)
    
    # Try to deliver immediately if online
    target_data = await get_user(target_id)
    if target_data.get('status') == 'online':
        await notify_user(target_id, message_data)
    
    return {'type': 'message_sent', 'message': 'Message delivered'}

async def get_friends_list(user_id):
    """Get list of friends for a user"""
    friend_ids = await get_friends(user_id)
    friend_list = []
    
    for friend_id in friend_ids:
        friend_data = await get_user(friend_id)
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
    requester_ids = await get_pending(user_id)
    requests = []
    
    for requester_id in requester_ids:
        requester_data = await get_user(requester_id)
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
    elif is_redis_connected():
        # Publish to Redis for other instances
        await redis_client.publish(f'user_channel:{user_id}', json.dumps(message))

async def handle_redis_messages():
    """Listen for messages from Redis Pub/Sub for users on other instances"""
    if not is_redis_connected():
        return
    
    pubsub = redis_client.pubsub()
    
    # Subscribe to channels for all local users
    channels = [f'user_channel:{user_id}' for user_id in local_clients.keys()]
    if channels:
        await pubsub.subscribe(*channels)
    
    async for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                channel = message['channel']
                user_id = channel.replace('user_channel:', '')
                
                if user_id in local_clients:
                    await local_clients[user_id].send(json.dumps(data))
            except Exception:
                pass

async def handle_client(websocket):
    """Main WebSocket handler"""
    user_id = None
    
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
                    if is_redis_connected():
                        asyncio.create_task(handle_redis_messages())
                    
                    # Load and send queued messages
                    if user_id:
                        if is_postgres_connected():
                            async with db_pool.acquire() as conn:
                                rows = await conn.fetch('''
                                    SELECT from_id, encrypted_message, timestamp
                                    FROM messages
                                    WHERE to_id = $1 AND delivered = FALSE
                                    ORDER BY timestamp
                                ''', user_id)
                                
                                for row in rows:
                                    sender_data = await get_user(row['from_id'])
                                    await websocket.send(json.dumps({
                                        'type': 'message',
                                        'from_id': row['from_id'],
                                        'from_name': sender_data.get('display_name', 'Unknown'),
                                        'encrypted_message': row['encrypted_message'],
                                        'timestamp': row['timestamp'].isoformat()
                                    }))
                                
                                await conn.execute('''
                                    UPDATE messages
                                    SET delivered = TRUE
                                    WHERE to_id = $1 AND delivered = FALSE
                                ''', user_id)
                        else:
                            # Send in-memory queued messages
                            queued = await get_queued_messages(user_id)
                            for msg in queued:
                                await websocket.send(json.dumps(msg))
                
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
            
            # Update status
            user_data = await get_user(user_id)
            if user_data:
                user_data['status'] = 'offline'
                await set_user(user_id, user_data)
            
            # Notify friends
            friend_ids = await get_friends(user_id)
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
    print(f"Storage mode: {'Redis' if is_redis_connected() else 'In-Memory'}")
    print(f"Persistence: {'PostgreSQL' if is_postgres_connected() else 'None (in-memory only)'}")
    
    # Health check handler for Back4app
    async def process_request(path, request_headers):
        if path == "/":
            return (http.HTTPStatus.OK, [("Content-Type", "text/plain")], b"OK")
        return None  # Let websockets handle WebSocket requests
    
    async with websockets.serve(handle_client, "0.0.0.0", port, process_request=process_request):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
