from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg
from psycopg import errors
import jwt
from datetime import datetime, timedelta, timezone
import os

app = Flask(__name__)

secret = os.getenv("JWT_SECRET")
db_url = os.getenv("DATABASE_URL")

# first 4 functions are db helpers specifically
# can be moved to separate files as the project is expanded

# initialize the database table if it doesn't yet exist
def init_db():
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL
                )
            """)

# Queries db for a user
# returns None or a dict containing the tuple's contents
# Returns the user data
def get_user(username):
    # query database
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id, username, password_hash
                FROM users
                WHERE username = %s
            """, (username,))
            row = cur.fetchone()

    # didn't find user matching key, so return none
    if row is None:
        return None
    
    # return the user data
    return {
        "user_id": row[0],
        "username": row[1],
        "password_hash": row[2]
    }

# No explicit error check.
# Just errors on failed insert, and route function handles what to do after
# Return the inserted user id
def signup_user(username, pw_hash):
    # insert to database
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (username, password_hash)
                VALUES (%s, %s)
                RETURNING user_id
            """, (username, pw_hash))
            row = cur.fetchone()
        conn.commit()

    return row[0]

# use pyjwt for generating a token
def token_gen(username, uid):
    # create payload for token encoding
    payload = {
            "sub": str(uid),
            "username": username,
            "exp": datetime.now(timezone.utc) + timedelta(hours=24)
        }
    return jwt.encode(payload, secret, algorithm="HS256")




####################################
# API ENDPOINTS
####################################

# SIGNUP
# 200 on SUCCESS
# 409 on duplicate
# DOES NOT HANDLE INVALID INPUT (yet)
@app.post("/auth/signup")
def signup():
    
    # request to json
    data = request.get_json()

    # retrieve username and password from request
    username = data.get("username")
    password = data.get("password")

    # uses the workzeug password hash function
    pw_hash = generate_password_hash(password)

    # try/except block for signup
    # signup will error if duplicate
    try:
        user_id = signup_user(username, pw_hash)
    except errors.UniqueViolation:
        return jsonify({"error": "Username already exists"}), 409

    # return user id along with 200 success
    return jsonify({
        "message": "User created successfully",
        "user_id": user_id
    }), 200

# LOGIN
# 200 on SUCCESS
# 401 on unauthorized
# DOES NOT HANDLE INVALID INPUT (yet)
@app.post("/auth/login")
def login():

    # request to json
    data = request.get_json()

    # retrieve username and password from request
    username = data.get("username")
    password = data.get("password")

    # get user info
    user = get_user(username)

    if user is None:
        return jsonify({"error": "Invalid credentials"}), 401

    # uses the workzeug password check function
    if not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "Invalid credentials"}), 401

    # uses our token helper
    token = token_gen(user["username"], user["user_id"])

    # return token and user id along with 200 success
    return jsonify({
        "token": token,
        "user_id": user["user_id"]
    }), 200


if __name__ == "__main__":
    # initialize db then expose on port 8080
    init_db()
    app.run(host="0.0.0.0", port=8080, debug=True)