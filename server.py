import asyncio
import websockets
import json
import secrets
import string
import os
from datetime import datetime
from collections import defaultdict

# Store connected clients: {user_id: websocket}
clients = {}

# Store user metadata: {user_id: {public_key: str, display_name: str, status: str}}
users = {}

# Store friend requests: {user_id: [friend_user_ids]}
friends = defaultdict(set)

# Store pending friend requests: {user_id: [requester_ids]}
pending_requests = defaultdict(set)

# Store messages for offline users: {user_id: [messages]}
message_queue = defaultdict(list)

# Generate random 6-character user ID
def generate_user_id():
    return ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(6))

async def register_user(websocket, data):
    """Register a new user or reconnect existing user with their public key"""
    existing_user_id = data.get('user_id')  # Client may send existing user_id
    
    # If client sends existing user_id and we have that user, reconnect them
    if existing_user_id and existing_user_id in users:
        user_id = existing_user_id
        # Update their connection
        clients[user_id] = websocket
        users[user_id]['status'] = 'online'
        # Update public key and display name if changed
        users[user_id]['public_key'] = data.get('public_key', users[user_id]['public_key'])
        users[user_id]['display_name'] = data.get('display_name', users[user_id]['display_name'])
        
        return {
            'type': 'registered',
            'user_id': user_id,
            'reconnected': True,
            'message': 'User reconnected successfully'
        }
    
    # Generate new user_id for new users
    user_id = generate_user_id()
    
    users[user_id] = {
        'public_key': data.get('public_key', ''),
        'display_name': data.get('display_name', f'User-{user_id}'),
        'status': 'online'
    }
    clients[user_id] = websocket
    
    return {
        'type': 'registered',
        'user_id': user_id,
        'reconnected': False,
        'message': 'User registered successfully'
    }

async def send_friend_request(user_id, data):
    """Send a friend request to another user"""
    target_id = data.get('target_id')
    
    if target_id not in users:
        return {'type': 'error', 'message': 'User not found'}
    
    if target_id == user_id:
        return {'type': 'error', 'message': 'Cannot add yourself'}
    
    if target_id in friends[user_id]:
        return {'type': 'error', 'message': 'Already friends'}
    
    pending_requests[target_id].add(user_id)
    
    # Notify the target user if online
    if target_id in clients:
        await clients[target_id].send(json.dumps({
            'type': 'friend_request',
            'from_id': user_id,
            'from_name': users[user_id]['display_name'],
            'public_key': users[user_id]['public_key']
        }))
    
    return {
        'type': 'friend_request_sent',
        'target_id': target_id,
        'message': 'Friend request sent'
    }

async def accept_friend_request(user_id, data):
    """Accept a pending friend request"""
    requester_id = data.get('requester_id')
    
    if requester_id not in pending_requests[user_id]:
        return {'type': 'error', 'message': 'No pending request from this user'}
    
    pending_requests[user_id].discard(requester_id)
    friends[user_id].add(requester_id)
    friends[requester_id].add(user_id)
    
    # Notify both users
    response = {
        'type': 'friend_accepted',
        'user_id': requester_id,
        'display_name': users[requester_id]['display_name'],
        'public_key': users[requester_id]['public_key']
    }
    
    if user_id in clients:
        await clients[user_id].send(json.dumps(response))
    
    if requester_id in clients:
        await clients[requester_id].send(json.dumps({
            'type': 'friend_accepted',
            'user_id': user_id,
            'display_name': users[user_id]['display_name'],
            'public_key': users[user_id]['public_key']
        }))
    
    return {'type': 'success', 'message': 'Friend request accepted'}

async def reject_friend_request(user_id, data):
    """Reject a pending friend request"""
    requester_id = data.get('requester_id')
    pending_requests[user_id].discard(requester_id)
    
    if requester_id in clients:
        await clients[requester_id].send(json.dumps({
            'type': 'friend_rejected',
            'user_id': user_id
        }))
    
    return {'type': 'success', 'message': 'Friend request rejected'}

async def send_message(user_id, data):
    """Route an encrypted message to the recipient"""
    target_id = data.get('target_id')
    encrypted_message = data.get('encrypted_message')
    
    if target_id not in users:
        return {'type': 'error', 'message': 'Recipient not found'}
    
    if target_id not in friends[user_id]:
        return {'type': 'error', 'message': 'Not friends with this user'}
    
    message_data = {
        'type': 'message',
        'from_id': user_id,
        'from_name': users[user_id]['display_name'],
        'encrypted_message': encrypted_message,
        'timestamp': datetime.now().isoformat()
    }
    
    # If recipient is online, send immediately
    if target_id in clients:
        await clients[target_id].send(json.dumps(message_data))
    else:
        # Queue message for offline user
        message_queue[target_id].append(message_data)
    
    return {'type': 'message_sent', 'message': 'Message delivered'}

async def get_friends_list(user_id):
    """Get list of friends for a user"""
    friend_list = []
    for friend_id in friends[user_id]:
        friend_list.append({
            'user_id': friend_id,
            'display_name': users[friend_id]['display_name'],
            'status': users[friend_id]['status'],
            'public_key': users[friend_id]['public_key']
        })
    return {
        'type': 'friends_list',
        'friends': friend_list
    }

async def get_pending_requests(user_id):
    """Get pending friend requests for a user"""
    requests = []
    for requester_id in pending_requests[user_id]:
        requests.append({
            'user_id': requester_id,
            'display_name': users[requester_id]['display_name'],
            'public_key': users[requester_id]['public_key']
        })
    return {
        'type': 'pending_requests',
        'requests': requests
    }

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
                    
                    # Send any queued messages
                    if user_id and user_id in message_queue:
                        for queued_message in message_queue[user_id]:
                            await websocket.send(json.dumps(queued_message))
                        message_queue[user_id] = []
                
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
            if user_id in clients:
                del clients[user_id]
            if user_id in users:
                users[user_id]['status'] = 'offline'
            # Notify friends about status change
            for friend_id in friends[user_id]:
                if friend_id in clients:
                    asyncio.create_task(clients[friend_id].send(json.dumps({
                        'type': 'status_update',
                        'user_id': user_id,
                        'status': 'offline'
                    })))

async def main():
    # Use PORT from environment (Render sets this), default to 8765 for local
    port = int(os.environ.get('PORT', 8765))
    
    print(f"Starting Secure Chat Server on port {port}")
    async with websockets.serve(handle_client, "0.0.0.0", port):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
